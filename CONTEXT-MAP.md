# Context Map — ElectriCore

ElectriCore est organisé en modules avec leur propre vocabulaire. Le glossaire métier de base vit dans `core` et est référencé par les autres modules ; chaque module ne définit que les termes qui lui sont propres.

## Contextes

- [**electricore/core/**](electricore/core/CONTEXT.md) — Vocabulaire métier (acteurs, PDL, FTA, cadrans, calendriers distributeur, tarification & taxes, événements C15, mesures & énergie, concepts pipeline, intégration Odoo).
- [**electricore/etl/**](electricore/etl/CONTEXT.md) — Vocabulaire d'ingestion (flux Enedis, Mode ETL, Job ETL, Scheduler, transformers).
- [**electricore/api/**](electricore/api/CONTEXT.md) — Vocabulaire du service REST (endpoints, services, `/health`).
- [**electricore/bot/**](electricore/bot/CONTEXT.md) — Vocabulaire du bot Telegram (commandes, allowlist, client API).

## Relations

- `core` est la source unique du vocabulaire métier ; les autres modules le référencent plutôt que de redéfinir les mêmes termes.
- `etl` ingère les flux Enedis dans DuckDB, consommés par `core`.
- `api` expose `core` (calculs) et orchestre `etl` (déclenchements + jobs).
- `bot` consomme `api` exclusivement (pas d'accès direct à `core` ni à `etl`).

Les ADRs structurants sont au niveau racine dans [docs/adr/](docs/adr/) ; aucun ADR par contexte pour l'instant.
