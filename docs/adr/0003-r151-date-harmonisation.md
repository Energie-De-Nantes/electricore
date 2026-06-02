# Harmonisation des dates R151 par ajustement +1 jour

Les flux Enedis utilisent deux conventions de date incompatibles : R64/R15/C15 stockent l'index *au début* du jour J, tandis que R151 stocke l'index *à la fin* du jour J. Pour produire des résultats cohérents, les dates R151 sont ajustées de +1 jour à la lecture (convention cible : « début de journée », majoritaire), avec un flag `date_ajustee = true` pour traçabilité. L'implémentation actuelle vit dans la couche SQL ([electricore/core/loaders/duckdb/sql.py](../../electricore/core/loaders/duckdb/sql.py)) — placement incidentel, pas architectural ; les pipelines en aval consomment des dates déjà harmonisées.

## Conséquences

- Une date `date_releve = '2024-01-16'` en DuckDB ne correspondra **pas** à la même date dans le fichier XML R151 source. La traçabilité passe par `date_ajustee`.
- Tout consommateur de la table brute `flux_r151` doit savoir qu'elle est en convention « fin de journée » ; passer par `releves_harmonises()` est la voie sûre.
