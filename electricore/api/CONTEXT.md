# Contexte — api (service REST)

Vocabulaire spécifique au service REST qui expose `core` et l'orchestration de l'`etl`. Hub central de l'architecture — voir [ADR-0009](../../docs/adr/0009-architecture-api-centrique.md).

## API

**API** :
Service REST FastAPI ([electricore/api/](.)) exposant les flux Enedis (`/flux/*`), les déclenchements d'ingestion (`/ingestion/*`), les calculs de taxes (`/taxes/*`), les exports de facturation (`/facturation/*`) et les vérifications pré-facturation (`/check/*`).

**Endpoint sécurisé** :
Endpoint nécessitant la clé `X-API-Key` (header) ou `?api_key=` (query). Tous les endpoints sont sécurisés sauf `/`, `/health`, `/docs`, `/redoc`.

**`/health`** :
Endpoint public qui retourne l'état de l'API, de la base DuckDB (accessible, dernière écriture, comptes par table) et du bot. Utilisé pour les checks de déploiement et le monitoring externe.

**Service** :
Module de [services/](services/) qui contient la logique d'un endpoint (ex : `duckdb_service.py` pour les requêtes flux, `facturation_service.py` pour la réconciliation Odoo↔Enedis). Permet de séparer la couche HTTP de la logique d'accès aux données.
