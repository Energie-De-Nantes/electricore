# Configuration du projet — l'inventaire complet

Tous les points de configuration d'ElectriCore, en un seul endroit. Quand tu cherches
« où se règle X », c'est ici. (La centralisation plus propre est un chantier ouvert —
voir l'issue dédiée.)

## 1. Variables d'environnement (`.env` à la racine, gitignoré)

Chargées par `electricore/config/env.py::charger_env()` (API, notebooks) et par le
préambule de `pipeline_dbt.py` (ETL). **Priorité aux variables déjà présentes dans
l'environnement** (`setdefault`) : en Docker, `env_file: .env` + le bloc `environment:`
du compose priment.

### ETL (mouvement des flux)

| variable | rôle | défaut |
|---|---|---|
| `SFTP__URL` | URL SFTP Enedis (`sftp://user:pass@host/chemin` ou `file:///...` pour des zips locaux) | — (requis) |
| `AES__CURRENT__KEY` / `AES__CURRENT__IV` | clé AES-128-CBC active (hex) — déchiffrement des zips | — (requis) |
| `AES__PREVIOUS__KEY` / `AES__PREVIOUS__IV` | clé précédente, gardée ~4 semaines après rotation Enedis | optionnel |
| `AES__KEY` / `AES__IV` | format plat v1, compatibilité ascendante | optionnel |

Convention dlt : `SECTION__CLÉ` ⇔ `dlt.secrets['section']['clé']` — le fichier
`.dlt/secrets.toml` (`[sftp]`, `[aes.current]`, `[aes.previous]`) est l'alternative
équivalente aux env vars (procédure de rotation : CLAUDE.md).

### Bases de données

| variable | rôle | défaut |
|---|---|---|
| `DUCKDB_PATH` | la base DuckDB de production — résolue par **un seul module**, `chemin_base_duckdb()` (`electricore/config/env.py`, `.env` honoré, issue #146), consommé par les loaders core, l'API, le runner ETL et les tools (`reset_incremental_state`). En Docker : `/data/flux_enedis_pipeline.duckdb` (volume) | `electricore/etl/flux_enedis_pipeline.duckdb` (absolu, ancré sur le dépôt — indépendant du CWD) |
| `DBT_DUCKDB_PATH` | chemin vu par dbt (`etl/dbt/profiles.yml`) — **positionné automatiquement** par le runner et les tests ; ne se règle pas à la main | suit `DUCKDB_PATH` via le runner |

Schémas dans la base : `flux_raw` (brut JSON), `flux_enedis` (tables typées, lues par
les loaders — figé dans `etl/dbt/profiles.yml`).

### API

Centralisées dans `electricore/api/config.py` :

| variable | rôle | défaut |
|---|---|---|
| `API_KEY` / `API_KEYS` | clé(s) d'authentification (`X-API-Key`) | — |
| `ENABLE_API_KEY_HEADER` | activer l'auth par header | — |
| `API_TITLE` / `API_DESCRIPTION` / `API_VERSION` | métadonnées OpenAPI | version du paquet |
| `INSTANCE_SLUG` | identifiant d'instance (multi-déploiements) | `""` |

### Bot Telegram

| variable | rôle |
|---|---|
| `TELEGRAM_BOT_TOKEN` | token du bot |
| `TELEGRAM_ALLOWED_USERS` | allowlist d'IDs utilisateurs |
| `TELEGRAM_NOTIFY_CHAT_ID` | chat des alertes proactives (échec d'un job ETL, scheduler compris) ; vide = désactivé |
| `API_BASE_URL` | URL de l'API appelée par le bot (défaut `http://localhost:8001`) |

### Odoo (intégration)

`electricore/config/odoo.py::charger_config_odoo()` — sélecteur `ODOO_ENV`
(`test`/`prod`) puis `ODOO_{TEST|PROD}_{URL,DB,USERNAME,PASSWORD}`.

## 2. Fichiers de configuration versionnés

| fichier | rôle | quand y toucher |
|---|---|---|
| `electricore/etl/config/flux.yaml` | **mouvement** des flux : `file_pattern` (glob SFTP), `format` (xml/json), `file_regex` (fichiers extraits des zips) | nouveau flux Enedis, changement de nommage des zips |
| `electricore/etl/dbt/profiles.yml` | connexion dbt→DuckDB (path via env), **schéma cible `flux_enedis`** | jamais ou presque |
| `electricore/etl/dbt/dbt_project.yml` | matérialisations (staging=view, flux=table) | jamais ou presque |
| `electricore/etl/dbt/models/sources.yml` | déclaration des tables brutes `raw_*` | nouveau flux |
| `electricore/etl/dbt/models/flux/schema.yml` | data tests `not_null` (joués à chaque build) | durcir le filet, nouveau flux |
| `electricore/config/turpe_rules.csv` | règles tarifaires TURPE (fixe + variable) | publication CRE |
| `electricore/config/accise_rules.csv` | barème accise (TICFE) | loi de finances |
| `electricore/config/cta_rules.csv` | taux CTA | évolution réglementaire |

La **sélection/typage** des flux (ex-DSL de `flux.yaml`) n'est plus de la configuration :
ce sont les modèles SQL `etl/dbt/models/` — voir [ingestion.md](ingestion.md).

## 3. Déploiement (`deploy/docker/`)

| fichier | rôle |
|---|---|
| `docker-compose.yml` | services api/bot/etl-scheduler ; `env_file: .env` + `environment:` (`DUCKDB_PATH=/data/...`, `TZ=Europe/Paris`) ; volume `duckdb_data` |
| `crontab` (copié de `crontab.example`) | planning : `/etl/run all` à 02:00 via l'API, backup à 03:30 |
| `Caddyfile` (copié de `.example`) | reverse proxy TLS |
| `backup_duckdb.sh` | sauvegarde quotidienne de la base |
| `Dockerfile` | image unique, `uv sync --extra etl --extra dbt` |

## 4. Outillage développeur (config de dev, pas de runtime)

| fichier | rôle |
|---|---|
| `pyproject.toml` | dépendances + extras (`etl`, `dbt`, `viz`), ruff, mypy (périmètre strict), pytest (markers), coverage |
| `.pre-commit-config.yaml` | ruff check/format + détection de secrets (et pytest en pre-push) |
| `.github/workflows/ci.yml` | lint, typecheck, tests py3.12/3.13 avec extras `etl`+`dbt` |

## Régénérables (pas de la config, mais souvent cherchés ici)

- Fixtures XSD et golden : `tests/fixtures/flux/generer_fixtures_xsd.py` (exige les XSD
  Enedis en local) et `generer_golden.py` (oracle = chemin dbt).
- État incrémental dlt : `~/.dlt/` + tables `_dlt_*` — purge via `pipeline_dbt resync`.
