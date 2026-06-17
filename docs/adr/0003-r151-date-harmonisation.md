# Harmonisation des dates R151 par ajustement +1 jour

Les flux Enedis utilisent deux conventions de date incompatibles : R64/R15/C15 stockent l'index *au début* du jour J, tandis que R151 stocke l'index *à la fin* du jour J. Pour produire des résultats cohérents, les dates R151 sont ajustées de +1 jour à la lecture (convention cible : « début de journée », majoritaire), avec un flag `date_ajustee = true` pour traçabilité. L'implémentation actuelle vit dans la couche SQL ([electricore/core/loaders/duckdb/sql.py](../../electricore/core/loaders/duckdb/sql.py)) — placement incidentel, pas architectural ; les pipelines en aval consomment des dates déjà harmonisées.

## Conséquences

- Une date `date_releve = '2024-01-16'` en DuckDB ne correspondra **pas** à la même date dans le fichier XML R151 source. La traçabilité passe par `date_ajustee`.
- Tout consommateur de la table brute `flux_r151` doit savoir qu'elle est en convention « fin de journée » ; passer par `releves_harmonises()` est la voie sûre.

## Amendement (#294, 16/06/2026) — le +1 jour vit en UN seul endroit : le mart `releves`

Le placement « incidentel » noté ci-dessus est résolu. L'harmonisation J → J+1 vit
désormais **uniquement** dans le mart dbt `releves` ([ADR-0029](0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md)),
le modèle canonique consommé par la chaîne énergie. Elle est **retirée du loader**
`SCHEMA_R151` ([sql.py](../../electricore/core/loaders/duckdb/sql.py)).

Conséquence **non silencieuse** (le changement de sortie est documenté, pas remplacé en
douce) :

- Le modèle `flux_r151` et l'endpoint brut **`/flux/r151` restent fidèles à la source** :
  date **nue**, convention « fin de journée », **sans** le +1 jour. Pousser le +1 jour dans
  `flux_r151` ferait servir des dates harmonisées sous une étiquette « brut » → trompeur.
- La responsabilité de connaître la convention fin-de-journée de R151 revient à l'utilisateur
  des flux bruts. Ces endpoints `/flux/*` sont **candidats à dépréciation** (ils
  disparaîtront) ; la voie sûre côté cœur est le mart `releves` (loader `releves()`), déjà
  harmonisé.
- La chaîne énergie lit le mart `releves` : elle **n'est pas affectée**.

Le flag `date_ajustee` mentionné plus haut n'a jamais été matérialisé ; la traçabilité de
la convention passe par la source (`source = 'flux_R151'`) et la documentation.
