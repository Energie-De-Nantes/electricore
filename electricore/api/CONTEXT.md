# Contexte — api (service REST)

Vocabulaire spécifique au service REST qui expose `core` et l'orchestration de l'`ingestion`. Hub central de l'architecture — voir [ADR-0009](../../docs/adr/0009-architecture-api-centrique.md).

## API

**API** :
Service REST FastAPI ([electricore/api/](.)) exposant les flux Enedis (`/flux/*`), les déclenchements d'ingestion (`/ingestion/*`), les calculs de taxes (`/taxes/*`), les exports de facturation et vérifications pré-facturation (`/facturation/*`, dont `/facturation/check/odoo.xlsx`), et la lecture des méta-périodes mensuelles (`/facturation/meta-periodes`, cf. *Endpoint méta-périodes*).

**Endpoint méta-périodes** :
`GET /facturation/meta-periodes` — endpoint de lecture par lequel un ERP **tire** les *méta-périodes mensuelles* d'electricore (Odoo construit ses `souscription.periode` à partir de ce flux). JSON enveloppé (`mois` / `contract_version` / `filters` / `pagination` / `data`), ERP-agnostique (zéro `integrations/odoo`, [ADR-0027](../../docs/adr/0027-endpoint-lecture-meta-periodes-odoo-tire.md)). Contrat figé : [docs/contrat-meta-periodes.md](../../docs/contrat-meta-periodes.md). Distinct des autres `/facturation/*` qui, eux, lisent Odoo. Charge utile **non valorisée aux prix fournisseur** : quantités physiques + montants réseau (TURPE, CTA) + *taux* accise.

**Endpoint sécurisé** :
Endpoint nécessitant la clé `X-API-Key` (header) ou `?api_key=` (query). Tous les endpoints sont sécurisés sauf `/`, `/health`, `/docs`, `/redoc`.

**`/health`** :
Endpoint public qui retourne l'état de l'API, de la base DuckDB (accessible, dernière écriture, comptes par table) et du bot. Utilisé pour les checks de déploiement et le monitoring externe.

**Service** :
Module de [services/](services/) qui contient la logique d'un endpoint (ex : `duckdb_service.py` pour les requêtes flux, `facturation_service.py` pour la réconciliation Odoo↔Enedis). Permet de séparer la couche HTTP de la logique d'accès aux données.
