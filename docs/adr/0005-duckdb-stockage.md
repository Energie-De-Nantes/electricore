# DuckDB comme couche de stockage

Les flux Enedis ingérés sont stockés dans une base DuckDB embarquée (`electricore/etl/flux_enedis_pipeline.duckdb`). Aucune alternative n'a été sérieusement évaluée : DuckDB combinait dès le départ stockage colonnaire, exécution OLAP rapide, format mono-fichier et zéro ops — l'évidence pour un projet solo.

## Limites à connaître avant un changement d'échelle

- **Un seul writer concurrent** : DuckDB verrouille la base en écriture. Plusieurs pipelines d'ingestion en parallèle nécessiteraient un autre moteur.
- **Mono-nœud** : pas de réplication ni de sharding natifs. Au-delà d'une machine, migrer vers Postgres ou ClickHouse devient nécessaire.

Ces limites n'étaient pas bloquantes au moment du choix et ne le sont pas aujourd'hui ; elles sont consignées ici pour éviter qu'un futur lecteur les découvre à chaud.
