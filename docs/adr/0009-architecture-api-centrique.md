# Architecture API-centrique

À mesure que le projet s'est étendu (multiples opérateurs, déclenchements d'ETL distants, exports métier réguliers), tous les clients passent par l'API REST ([electricore/api/](../../electricore/api/)) : bot Telegram, scheduler ETL, futurs notebooks Marimo, intégrations externes. DuckDB, les pipelines Polars et `OdooReader/Writer` sont des composants internes consommés *via* l'API, pas directement.

## Conséquences

- **Notebooks : état transitoire.** Aujourd'hui [notebooks/](../../notebooks/) importe encore directement les pipelines et lit la base DuckDB en local. C'est une dette assumée — l'objectif est qu'ils migrent à terme vers le client HTTP. Drivers de la migration : (1) éliminer la synchronisation manuelle de la base DuckDB sur les postes de travail, (2) éviter la dérive entre logique des endpoints et requêtes ad-hoc des notebooks, (3) garder les données Enedis sensibles côté serveur.
- **L'API est un contrat.** Toute évolution d'endpoint impacte au moins le bot et le scheduler ; les changements cassants nécessitent une coordination.
- **Pas d'accès direct DuckDB depuis le bot.** Le bot ne connaît pas la structure du stockage ; toute requête passe par un endpoint.
