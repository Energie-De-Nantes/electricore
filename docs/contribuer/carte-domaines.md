# Carte des domaines

Tu cherches un terme métier (PDL, RSC, FTA, cadran…) ou le vocabulaire propre à
un module ? ElectriCore est organisé en **contextes**, chacun avec son
glossaire colocalisé au code. Cette page est le miroir, côté site, de
[CONTEXT-MAP.md](https://github.com/Energie-De-Nantes/electricore/blob/main/CONTEXT-MAP.md) —
elle **lie** les glossaires, elle ne les déplace ni ne les copie : ils vivent
avec le code qu'ils décrivent, et c'est là qu'il faut les modifier.

## Les contextes

- [**`electricore/core/`**](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/core/CONTEXT.md) —
  vocabulaire métier ERP-agnostique : acteurs, PDL, RSC (concept Enedis), FTA, cadrans,
  calendriers distributeur, tarification & taxes, événements C15, affaires SGE, mesures &
  énergie, concepts pipeline. La **source unique** du vocabulaire métier.
- [**`electricore/integrations/odoo/`**](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/integrations/odoo/CONTEXT.md) —
  adaptateur vers l'ERP Odoo : modèles `sale.order` / `account.move`, champs `x_*`,
  rapprochements Odoo ↔ Enedis.
- [**`electricore/ingestion/`**](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/ingestion/CONTEXT.md) —
  vocabulaire d'ingestion : flux Enedis, Mode, Job, Scheduler, transformers.
- [**`electricore/api/`**](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/api/CONTEXT.md) —
  vocabulaire du service REST : endpoints, services, `/health`.
- [**`electricore/bot/`**](https://github.com/Energie-De-Nantes/electricore/blob/main/electricore/bot/CONTEXT.md) —
  vocabulaire du bot Telegram : commandes, allowlist, client API.

## Comment ils se parlent

- `core` est la source unique du vocabulaire métier ERP-agnostique
  ([ADR-0016](../adr/0016-core-erp-agnostique.md)) ; les autres contextes le référencent plutôt
  que de redéfinir les mêmes termes.
- `integrations/<erp>` orchestre les rapprochements entre `core` (calculs purs) et l'ERP (Odoo
  aujourd'hui ; d'autres demain). N'est jamais importé depuis `core`.
- `ingestion` ingère les flux Enedis dans DuckDB, consommés par `core`.
- `api` expose `core` et `integrations/<erp>`, et orchestre `ingestion` (déclenchements + jobs).
- `bot` consomme `api` exclusivement — jamais d'accès direct à `core`, `ingestion` ou
  `integrations`.

Les décisions d'architecture structurantes sont au niveau racine dans
[docs/adr/](https://github.com/Energie-De-Nantes/electricore/tree/main/docs/adr) — aucun ADR
par contexte pour l'instant.

[Retour à l'accueil](../index.md).
