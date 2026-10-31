# 0058 — Déclaration de taxe dans la nouvelle architecture : un service, deux représentations, sans état

- Status: accepted
- Date: 2026-10-31

## Contexte

La déclaration CTA passe aujourd'hui par les endpoints *legacy* `/taxes/cta/*`
(`rapport.xlsx`, `detail.{xlsx,arrow}`), appelés par le bot. Ils lisent le vieil Odoo :
`pipeline_cta` fait une **jointure interne** sur `pdl_mapping` (`sale.order.x_pdl`), ce
qui restreint la déclaration aux PDL connus d'Odoo. C'est un trait du legacy, pas une
règle métier : la CTA est due sur tout le périmètre Enedis.

Le besoin (#651, recadré par grilling le 2026-10-31) est de déclarer la CTA **sur le parc
entier, sans Odoo**, pour plusieurs consommateurs du même calcul :

- le **bot** (`/taxes cta [trimestre]`, #746), qui livre un document xlsx ;
- le **Kiosque** (#708), qui affiche un aperçu puis propose de télécharger le même document ;
- plus tard, un **ERP**, qui pourrait créer la facture fournisseur CTA au bon montant.

Trois contraintes cadrent la réponse : peu d'endpoints, aucun calcul en double, et un
montant qu'on puisse retrouver après coup. Côté coût, le recalcul de tout le parc
(contexte mensuel, puis CTA) prend environ 0,3 s sur une base réelle (19 000 lignes,
29 mois).

## Décision

### 1. Un service, deux représentations

`declaration_cta_service.declaration_cta(trimestre)` produit **le cœur** : une seule table
au grain **RSC × mois**, avec `ref_situation_contractuelle`, `pdl`, `mois_annee`,
`trimestre`, `turpe_fixe_eur` (l'assiette), `taux_cta_pct` et `cta_eur`. Deux endpoints
l'exposent :

- `GET /taxes/cta/declaration.arrow` sert ce cœur tel quel. C'est lui que lisent l'aperçu
  du Kiosque et, demain, l'ERP.
- `GET /taxes/cta/declaration.xlsx` habille **le même** résultat en trois onglets : Résumé
  (totaux par trimestre et empreinte), Par taux (trimestre × taux) et Détail (le cœur).
  Les agrégats réutilisent les fonctions pures `agreger_resume` et `agreger_par_taux`
  (`core/builds/rapport_taxe.py`).

Résumé et Par taux n'existent que dans le xlsx. Aucun consommateur ne les recalcule : le
Kiosque télécharge les octets du xlsx de l'API, donc le même document que le bot.
L'assiette est le TURPE fixe recalculé par electricore, et la CTA passe par `ajouter_cta()`
(règle ADR-0027 : montant en € quand electricore possède l'assiette), jamais par
`pipeline_cta`. Zéro import `integrations` (ADR-0016), garanti par un garde-fou
d'architecture. Tag OpenAPI `taxes`, non `legacy`.

### 2. Sans état

electricore ne stocke ni ne fige aucune déclaration : chaque requête recalcule la vérité
du moment. C'est le **document posté** qui fige le montant, qu'il s'agisse de la
déclaration envoyée ou de la facture dans l'ERP. Un écart découvert plus tard (par exemple
un C15 rétroactif) est un signal de **régularisation**, pas une réécriture. C'est la
doctrine des méta-périodes (ADR-0027 et ADR-0038) étendue aux taxes.

### 3. Deux niveaux d'empreinte, sous contrat versionné

Pour repérer une dérive sans comparer des fichiers entiers :

- `source_hash` **par ligne** est le noyau `empreinte_contenu` (#625), appliqué aux colonnes
  de montant `ref_situation_contractuelle`, `pdl`, `mois_annee`, `turpe_fixe_eur`,
  `taux_cta_pct`, `cta_eur` (sans `trimestre`, qui se déduit de `mois_annee`). Il dit
  *quelles* lignes ont bougé.
- `empreinte_trimestre` est le sha256 tronqué des `source_hash` **triés** du trimestre. Elle
  est répétée sur chaque ligne de l'Arrow et affichée dans l'onglet Résumé. C'est la valeur
  qu'on note avec la déclaration : la même empreinte au retéléchargement signifie que rien
  n'a bougé. Elle est calculée **une seule fois, côté serveur**, plutôt que par chaque
  consommateur (trois implémentations à garder identiques).

La liste de colonnes et la construction de l'empreinte forment un **contrat** :
`VERSION_EMPREINTE` (exposée dans le Résumé) doit être incrémentée à toute modification,
puisque les empreintes de deux versions différentes ne se comparent pas. L'empreinte est
volontairement **plus sensible que la déclaration** : une CTA qui passe d'un contrat à un
autre, total inchangé, la fait changer. C'est un signal d'audit. Ce qui décide s'il faut
régulariser, c'est l'écart sur le total.

### 4. On ne déclare qu'un trimestre terminé

`trimestre` (au format `YYYY-TN`) est facultatif :

- **explicite et non terminé** à l'horizon (le 1ᵉʳ du mois courant) : 422 avec
  `X-Error-Kind: precondition`, que le client transforme en `PreconditionNonRemplie`. Un
  tiers de trimestre ne doit jamais partir comme une déclaration, et encore moins comme une
  facture ERP créée automatiquement ;
- **absent** : on sert uniquement les trimestres terminés.

### Alternatives écartées

- **Étendre le legacy** (rendre `pdl_mapping` facultatif dans `/taxes/cta/*`) : le legacy
  est figé et voué à disparaître (ADR-0027). Le nouveau chemin vit à côté sans le toucher.
- **Servir toute la `facturation_mensuelle`** (énergies, qualité…) comme ressource générique
  d'analyse : ça fait une surface publique plus large que le besoin, un nom qui se confond
  avec les *périodes* (énergie, abonnement, méta-période), et le TURPE variable n'entre pas
  dans l'assiette.
- **Recalculer agrégats ou empreinte chez chaque consommateur** : ce serait de la logique
  métier dans des clients minces (#708) et un risque de divergence.
- **Figer les déclarations côté electricore** : ce serait de l'état à gérer, alors que le
  document posté remplit déjà ce rôle.
- **Signaler un trimestre partiel** (colonne ou en-tête) au lieu de le refuser : chaque
  consommateur devrait le lire, et un ERP qui l'ignore facturerait faux sans rien dire.

## Conséquences

- Le bot (#746) et le Kiosque (#708) passent sur ces endpoints, et la CTA devient disponible
  en mode sans ERP. Les `/taxes/cta/*` legacy restent inchangés jusqu'à leur retrait.
- `electricore-client` 0.6.0 : `ElectricoreArrowClient.declaration_cta()` et
  `ElectricoreClient.declaration_cta_xlsx()`.
- « Terminé » est une notion **calendaire** (fixée par l'horizon), pas une garantie de
  fraîcheur des données : un trimestre terminé calculé sur une base pas à jour reste
  incomplet. En production, l'ingestion est quotidienne. Le reste (rétroactifs, retards)
  relève de l'empreinte et de la régularisation.
- Le même schéma (`/taxes/<taxe>/declaration.*`) sert de modèle pour les taxes suivantes.
  **L'accise ne le suit pas directement** : son assiette de déclaration est le *facturé*,
  qui appartient à l'ERP (ADR-0027). Elle demandera sa propre décision.
