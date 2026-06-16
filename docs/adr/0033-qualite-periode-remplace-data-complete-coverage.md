---
status: accepted
---

# Qualité de période remplace `data_complete` et `coverage`

## Contexte

Le pipeline énergie portait deux signaux de complétude qui ont pourri :

- Au grain *période d'énergie*, `data_complete` (booléen) ne signifiait que « les deux relevés bornants sont **présents** » — il ne distinguait **jamais** un relevé `réel` d'un `estimé` Enedis, alors que cette distinction est une mention légale sur la facture et conditionne les décisions de *contrat lissé*.
- Au grain *méta-période mensuelle*, `data_complete = coverage_abo==1.0 AND coverage_energie==1.0` reposait sur deux métriques bancales : `coverage_abo` était un placeholder figé à `1.0` (jamais calculé, et **conceptuellement faux** en facturation calendaire — un mois d'entrée/sortie tronqué est correct, pas incomplet) ; `coverage_energie` était une fraction jours-pondérée suggérant une granularité « facturer X % d'un mois » non-actionnable (l'énergie d'un mois est tout-ou-rien calculable) et ré-encodant au grain méta ce que le grain période disait déjà.

## Décision

Remplacer tout cet appareillage par la *qualité de période d'énergie* (issue #278, glossaire `core/CONTEXT.md`) : un état à trois valeurs `qualite ∈ {réelle, estimée, incalculable}` au grain période — rollup de la *nature d'index* des deux relevés bornants (`réel`/`corrigé` → réelle, `estimé` → estimée, relevé manquant → incalculable) — agrégé à la méta-période par **pire-gagne** (`incalculable > estimée > réelle`).

`data_complete`, `coverage_abo` et `coverage_energie` sont **retirés** de `PeriodeEnergie`, `PeriodeMeta`, des modèles d'abonnement (vestigiaux — jamais renseignés) et du contrat de l'API `/meta-periodes`. Le diagnostic « quel relevé manque » se fait par drill-down au grain période (`releve_manquant_debut`/`_fin`).

## Options considérées

- **Garder une fraction de couverture corrigée** (proportion de jours réels). Rejetée : une fraction n'est pas un *gate* de facturation actionnable en calendaire, et redit au grain méta ce que le grain période porte déjà.
- **Enum + ventilation comptée au grain méta** (`nb_periodes_{reelles,estimees,incalculables}`). Utile au tri facturiste, mais ajoute des colonnes pour une info atteignable par drill-down ; **différé** — ajoutable plus tard sans toucher à l'enum.

## Conséquences

- **Changement cassant** de `/meta-periodes` : les consommateurs (bot, clients) basculent de `data_complete`/`coverage_*` vers `qualite`.
- `qualite` est un **raffinement strict** : l'ancien `data_complete=True` se scinde en `{réelle, estimée}` ; l'ancien `False` devient `incalculable`. La facturation gagne la distinction réel/estimé qui lui manquait.
- L'axe « qualité » de l'abonnement disparaît définitivement (calculé depuis le C15, toujours connu ; la troncature calendaire n'est pas une incomplétude).
