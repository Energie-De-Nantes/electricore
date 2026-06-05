# Context Map — ElectriCore

ElectriCore est organisé en modules avec leur propre vocabulaire. Le glossaire métier de base vit dans `core` et est référencé par les autres modules ; chaque module ne définit que les termes qui lui sont propres.

## Contextes

- [**electricore/core/**](electricore/core/CONTEXT.md) — Vocabulaire métier ERP-agnostique (acteurs, PDL, RSC en tant que concept Enedis, FTA, cadrans, calendriers distributeur, tarification & taxes, événements C15, mesures & énergie, concepts pipeline).
- [**electricore/integrations/**](electricore/integrations/) — Adaptateurs vers les ERP et systèmes externes. Premier occupant : [`odoo/`](electricore/integrations/odoo/CONTEXT.md) (modèles `sale.order` / `account.move`, champs `x_*`, rapprochements Odoo ↔ Enedis).
- [**electricore/etl/**](electricore/etl/CONTEXT.md) — Vocabulaire d'ingestion (flux Enedis, Mode ETL, Job ETL, Scheduler, transformers).
- [**electricore/api/**](electricore/api/CONTEXT.md) — Vocabulaire du service REST (endpoints, services, `/health`).
- [**electricore/bot/**](electricore/bot/CONTEXT.md) — Vocabulaire du bot Telegram (commandes, allowlist, client API).

## Relations

- `core` est la source unique du vocabulaire métier ERP-agnostique (cf. [ADR-0016](docs/adr/0016-core-erp-agnostique.md)) ; les autres modules le référencent plutôt que de redéfinir les mêmes termes.
- `integrations/<erp>` orchestre les rapprochements entre `core` (calculs purs) et l'ERP (Odoo aujourd'hui ; Haulogy, autres demain). N'est jamais importé depuis `core`.
- `etl` ingère les flux Enedis dans DuckDB, consommés par `core`.
- `api` expose `core` (calculs) et `integrations/<erp>` (orchestrations), et orchestre `etl` (déclenchements + jobs).
- `bot` consomme `api` exclusivement (pas d'accès direct à `core` ni à `etl` ni à `integrations`).

Les ADRs structurants sont au niveau racine dans [docs/adr/](docs/adr/) ; aucun ADR par contexte pour l'instant.
