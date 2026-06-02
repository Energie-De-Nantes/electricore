# DLT pour orchestrer l'ETL

L'ingestion SFTP → DuckDB est orchestrée par [DLT](https://dlthub.com/) (pipeline déclaratif). Cela remplace les scripts Python custom de l'ancien dépôt `electriflux` : DLT gère la gestion d'état incrémentale, la déduplication et l'évolution de schéma sans qu'il faille les coder à la main. Compromis assumé : moins de contrôle fin sur chaque étape, mais beaucoup moins de code de plomberie à maintenir.
