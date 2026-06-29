# Deux entrées du Contexte mensuel : parc complet et point unique restent distinctes

## Contexte

`core/builds/contexte_mensuel.py` expose deux entrées I/O qui résolvent les loaders
DuckDB par défaut puis délèguent à `charger()` (la composition pure, [ADR-0019](0019-roles-loaders-pipelines-builds-integrations.md)) :

- `contexte_du_mois(mois, horizon)` — reconstruit le *Contexte mensuel* du **parc entier**.
- `contexte_du_mois_filtre(pdl, rsc, mois, horizon)` — reconstruit celui d'un **seul point
  (PDL) ou contrat (RSC)**, en poussant un `WHERE` paramétré dans DuckDB via
  `_filtrer_point_ou_contrat` (issue #366).

Vues du code seul, les deux ne diffèrent que par cet enrobage de filtre — qui no-ope
d'ailleurs quand `pdl`/`rsc` sont `None`. La symétrie suggère un effondrement en une
entrée unique `contexte_du_mois(mois, horizon, pdl, rsc)` : suggestion formulée par la
revue d'architecture de juin 2026, et qui le sera à nouveau par toute revue future si la
raison du rejet n'est pas écrite.

## Décision

**Les deux entrées restent distinctes.** `contexte_du_mois` sert le parc,
`contexte_du_mois_filtre` le point/contrat unique. `charger()` / `_composer()` reste
l'unique seam de composition **profond** ; les deux entrées sont des **adaptateurs d'I/O
fins** par-dessus, conformes au boundary d'[ADR-0019](0019-roles-loaders-pipelines-builds-integrations.md).

## Raison

- **Appelants disjoints.** `contexte_du_mois` a trois appelants — `taxes_service`,
  `meta_periodes_service`, `facturation_service` — qui passent toujours `mois` et jamais
  `pdl`/`rsc`. `contexte_du_mois_filtre` en a **un seul** : `chronologie_service`
  (drill-down `/chronologie`, #366), qui passe `pdl`/`rsc`/`horizon` et **pas** `mois`.
  Les jeux de paramètres sont disjoints — ce ne sont pas des jumeaux accidentels mais les
  faces publiques de deux flux.

- **Le test de suppression pointe de côté, pas vers le bas.** Supprimer
  `contexte_du_mois_filtre` reverse son corps sur **un** appelant (`chronologie_service`),
  pas sur N. C'est une commodité mono-appelant *nommée*, pas une duplication essaimée.

- **Adaptateurs d'I/O fins par construction (ADR-0019).** La profondeur vit dans
  `charger()` / `_composer`, qui porte *toute* la composition — déjà singulier. Les entrées
  doivent être fines : elles ne font que résoudre les loaders par défaut au boundary.
  « Deux entrées superficielles » est la forme **voulue** du boundary, pas un défaut — les
  fusionner ne rend rien plus profond, ça **élargit** un adaptateur.

- **Le nom porte l'intention.** `contexte_du_mois_filtre` documente la *reconstruction d'un
  point unique* (#366) et sa garantie de **parité** (`charger(plein ∩ X) ≡ charger(plein) ∩ X`,
  testée — [ADR-0039](0039-chronologie-substrat-attributs-situation-hors-mart.md)). Une entrée
  fusionnée à quatre boutons optionnels (`mois`/`horizon`/`pdl`/`rsc`) utilisés par
  sous-ensembles disjoints serait une interface **plus large que les deux implémentations
  réunies** (module *shallow* — même argument qu'[ADR-0023](0023-periodisations-separees-abonnement-energie.md)).

### Alternatives écartées

- **Entrée unique `contexte_du_mois(mois, horizon, pdl, rsc)`** — sûre (le filtre no-ope sur
  `None`, la parité est testée) mais troque une *intention nommée* contre une fonction de
  moins, et élargit l'interface. Le gain de **localité est nul** : `charger()` est déjà le
  seam de composition unique ; les deux entrées n'en sont que les adaptateurs de boundary.

- **Une impl, `contexte_du_mois_filtre` en alias d'une ligne** — préserve les deux noms en
  effondrant le corps. Gain marginal (~2 lignes), aucun problème résolu, et offre en prime
  à `contexte_du_mois` des paramètres `pdl`/`rsc` que personne ne demande.

## Conséquences

- Toute proposition de fusion des deux entrées doit d'abord lever le constat « appelants et
  paramètres disjoints + boundary d'I/O fin (ADR-0019) », pas seulement la symétrie du code.
- Étendre le filtrage (p. ex. par segment) se fait dans `_filtrer_point_ou_contrat` — le
  seam **interne** — sans toucher les deux faces publiques.
- `charger()` demeure le point d'extension pour toute évolution de la *composition* : c'est
  là que vit la profondeur, et c'est le seul endroit qu'un changement de pipeline touche.
