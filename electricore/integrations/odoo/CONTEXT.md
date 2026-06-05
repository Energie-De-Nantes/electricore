# Contexte — integrations/odoo (adaptateur ERP)

Vocabulaire spécifique à l'adaptateur Odoo. Les concepts métier neutres (PDL, RSC en tant qu'identifiant Enedis, événements C15, etc.) vivent dans [`electricore/core/CONTEXT.md`](../../core/CONTEXT.md). Ce contexte ne définit que les notions propres à la représentation Odoo et aux orchestrations qui composent Enedis et Odoo.

## Pourquoi ce contexte vit ici

[ADR-0016](../../../docs/adr/0016-core-erp-agnostique.md) — `electricore/core/` est strictement ERP-agnostique. Tout ce qui touche à Odoo (lecture, écriture, rapprochements, champs `x_*`) appartient à `integrations/odoo/`.

---

## Modèles Odoo référencés

**`sale.order`** :
Commande de vente Odoo qui matérialise le contrat client. Porte les champs personnalisés ElectriCore (`x_pdl`, `x_ref_situation_contractuelle`, `x_invoicing_state`, `x_lisse`, `x_date_cfne`).

**`account.move`** :
Facture Odoo. Reliée au `sale.order` via `invoice_ids`.

**`account.move.line`** :
Ligne de facture Odoo. Reliée à la facture via `invoice_line_ids`, au produit via `product_id`.

**`product.product`** / **`product.category`** :
Produit et sa catégorie. La catégorie détermine le cadran (HP, HC, Base, Abonnement, …) dans les rapprochements.

---

## Champs personnalisés ElectriCore (préfixe `x_`)

**`x_pdl`** :
Champ `sale.order` qui porte le PDL Enedis associé au contrat.

**`x_ref_situation_contractuelle`** :
Champ `sale.order` qui porte la *RSC* (cf. core/CONTEXT.md) active pour la situation contractuelle courante. Renseigné par le *Rapprochement PDL ↔ RSC*.

**`x_invoicing_state`** :
Champ Odoo qui matérialise l'avancement d'un `sale.order` dans le cycle de facturation mensuel. Les vérifications pré-facturation (`/check odoo` côté bot) reposent en partie sur sa répartition.

**`x_lisse`** :
Drapeau indiquant que le contrat est en *contrat lissé* (cf. core/CONTEXT.md) — facturation mensuelle constante plutôt qu'au prorata réel.

**`x_date_cfne`** :
Date du CFNE (Changement de Fournisseur Nouvelle Entrée) côté Odoo, utilisée comme date de référence pour l'attribution de RSC.

---

## Opérations

**Rapprochement PDL ↔ RSC** :
Opération **amont** qui attribue la RSC active à un `sale.order` Odoo via un asof join temporel (PDL + `date_order` → RSC d'entrée C15 la plus récente avant la date). Nécessaire parce que le PDL n'identifie pas de façon unique un couple (contrat, usager) dans le temps — un même PDL peut porter plusieurs RSC successives —, et que la RSC n'est pas directement exposée aux facturistes par SGE. Le rapprochement est donc fait côté technique, puis écrit dans `x_ref_situation_contractuelle` sur le `sale.order`. Implémenté à ce jour par le notebook `notebooks/injection_rsc.py`.
_Éviter_ : matching, attribution RSC.

**Rapprochement facturation mensuelle** :
Opération **aval** qui relie les lignes de facture Odoo (déjà tagguées avec `x_ref_situation_contractuelle`) à la facturation Enedis du mois (clé : RSC), et calcule la `quantite_enedis` par catégorie de produit (HP, HC, Base, Abonnements). Distincte du *Rapprochement PDL ↔ RSC* : ici la RSC est déjà connue, on enrichit. Implémenté par `rapprocher_facturation_mensuelle()` dans `core/pipelines/facturation.py`.
_Éviter_ : réconciliation (anglicisme), jointure facturation, `updates_rsc` (ancien nom de variable).

**`lignes_facture_rapprochees`** :
DataFrame résultat du *Rapprochement facturation mensuelle*. Une ligne par ligne de facture Odoo du mois cible (tous états confondus), enrichie de la quantité calculée à partir des flux Enedis (`quantite_enedis`) et de deux flags : `a_facturer` (ligne en attente d'injection de quantité) et `a_supprimer` (ligne brouillon à quantité nulle, candidate à `unlink`). Sert de proposition de mise à jour : la décision d'écriture dans Odoo est prise par l'opérateur depuis un notebook (cf. [ADR-0012](../../../docs/adr/0012-api-read-only-odoo.md)).
