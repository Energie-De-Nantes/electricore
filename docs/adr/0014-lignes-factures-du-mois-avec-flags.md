# Lignes de factures du mois exposées avec flags d'état

## Contexte

L'ancien helper Odoo `lignes_a_facturer` filtrait côté XML-RPC sur `sale.order.x_invoicing_state == 'draft'` ET `account.move.state == 'draft'`. Hors période de facturation (toutes factures validées), il retournait systématiquement zéro ligne, rendant impossibles : le test du notebook de facturation, l'audit du mois précédent, ou toute consultation rétrospective. Symétriquement, `lignes_quantite_zero` souffrait du même problème.

## Décision

Remplacer ces deux helpers par une fonction unique `lignes_factures_du_mois(odoo, mois)` qui :

- Filtre côté Odoo uniquement par `sale.order.state == 'sale'` et `invoice_date` dans le mois cible (volume maîtrisé).
- N'applique aucun filtre sur `x_invoicing_state`, `account.move.state`, ni `quantity`.
- Ajoute deux colonnes booléennes mutuellement exclusives sur le sous-ensemble draft :
  - `a_facturer` = `(x_invoicing_state == 'draft') AND (account.move.state == 'draft') AND (quantity > 0)` — réplique le contrat de l'ex-`lignes_a_facturer`.
  - `a_supprimer` = même triplet avec `quantity == 0` — réplique l'ex-`lignes_quantite_zero`.

Le filtre métier devient explicite côté consommateur : `df.filter(pl.col("a_facturer"))` en prod, pas de filtre en test/audit. `rapprocher_facturation_mensuelle` ne filtre plus en interne et propage les deux flags au modèle `LignesFactureRapprochees`.

## Raison

Cette forme résout simultanément trois usages (facturation active, test hors période, audit a posteriori) avec une seule fonction et un seul endpoint. Les alternatives écartées :

- **Env var serveur (`ELECTRICORE_INCLUDE_ALL_INVOICING_STATES`)** : état caché invisible des consommateurs, source de surprises en debug.
- **Paramètre query API (`?inclure_validees=true`)** : surface publique, risque d'usage involontaire en prod.
- **Mock côté client (fixture)** : décorelle le test de la chaîne API réelle, défaite le but de la migration #13.

Le coût assumé : volume légèrement supérieur côté Odoo (toutes les lignes du mois, pas seulement les draft), borné par le filtre `invoice_date` qui limite à un mois calendaire.
