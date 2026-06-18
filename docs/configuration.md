# Configuration du projet — l'inventaire complet

Tous les points de configuration d'ElectriCore, en un seul endroit. Quand tu cherches
« où se règle X », c'est ici. Trois registres de savoir ([ADR-0024](adr/0024-trois-registres-de-savoir.md)) :
le **runtime** (cette section 1, module `runtime.py`), le **structurel** (code/modèles) et
le **réglementaire** (taux régulés, section 2).

## 1. Variables d'environnement — registre runtime (`.env` à la racine, gitignoré)

Lues par **un seul module canonique**, `electricore/config/runtime.py` (pydantic-settings,
#141, [ADR-0025](adr/0025-registre-runtime-pydantic-settings.md)). Un domaine indépendant
par groupe (`sftp`, `aes`, `duckdb`, `api`, `bot`, `odoo`), accédé par accessor mis en
cache (`runtime.sftp()`, …). **Précédence env-système > `.env`** (native pydantic-settings) :
en Docker, le bloc `environment:` du compose prime sur `env_file: .env`. Validation
fail-fast par point d'entrée via `runtime.valider(*domaines)` (l'API ne valide jamais le
bot ; le runner valide `sftp`+`aes`+`duckdb`).

### Ingestion (mouvement des flux)

| variable | rôle | défaut |
|---|---|---|
| `SFTP__URL` | URL SFTP Enedis (`sftp://user:pass@host/chemin` ou `file:///...` pour des zips locaux) | — (requis) |
| `AES__TROUSSEAU__<label>__KEY` / `__IV` | une clé AES (hex) du trousseau ; 16 octets (AES-128) ou 32 (AES-256) ; `<label>` parlant choisi par l'opérateur (`aes256_2026`, `aes128_2024`…) | — (≥ 1 requis) |

Le délimiteur `__` est l'`env_nested_delimiter` natif de pydantic-settings : `AES__TROUSSEAU__aes256_2026__KEY`
→ entrée `trousseau["aes256_2026"].key`. Le trousseau accepte un nombre arbitraire de clés ; la bonne
est **sélectionnée par essai** au déchiffrement (oracle PKCS7 + magic bytes ZIP), sans date ni protocole
(ADR-0037). Garder les anciennes clés préserve l'accès aux archives passées. Le support `.dlt/secrets.toml`
a été retiré (#141) — les clés vivent en variables d'environnement uniquement.

### Bases de données

| variable | rôle | défaut |
|---|---|---|
| `DUCKDB_PATH` | la base DuckDB de production — résolue par le domaine `duckdb` du registre (`runtime.duckdb().chemin`, issue #146), consommé par les loaders core, l'API, le runner d'ingestion et les tools (`reset_incremental_state`). En Docker : `/data/flux_enedis_pipeline.duckdb` (volume) | `electricore/ingestion/flux_enedis_pipeline.duckdb` (absolu, ancré sur le dépôt — indépendant du CWD) |
| `DBT_DUCKDB_PATH` | chemin vu par dbt (`ingestion/dbt/profiles.yml`) — **seul pont `os.environ`** (ADR-0025), positionné par le runner autour de l'invocation dbt (scopé `try/finally`) ; ne se règle pas à la main | suit `runtime.duckdb().chemin` via le runner |

Schémas dans la base : `flux_raw` (brut JSON), `flux_enedis` (tables typées, lues par
les loaders — figé dans `ingestion/dbt/profiles.yml`).

### API

Domaine `api` du registre (`runtime.api()`) ; `electricore/api/config.py` n'est plus
qu'une façade de compatibilité (`settings`) déléguant au registre.

| variable | rôle | défaut |
|---|---|---|
| `API_KEY` / `API_KEYS` | clé(s) d'authentification (`X-API-Key`) | — |
| `API_TITLE` / `API_DESCRIPTION` / `API_VERSION` | métadonnées OpenAPI | version du paquet |
| `INSTANCE_SLUG` | identifiant d'instance (multi-déploiements) | `""` |

### Bot Telegram

| variable | rôle |
|---|---|
| `TELEGRAM_BOT_TOKEN` | token du bot |
| `TELEGRAM_ALLOWED_USERS` | allowlist d'IDs utilisateurs |
| `TELEGRAM_NOTIFY_CHAT_ID` | chat des alertes proactives (échec d'un job d'ingestion, scheduler compris) ; vide = désactivé |

`API_BASE_URL` a été **supprimée** (#141/ADR-0025) : le bot tourne dans le processus de
l'API et la joint en interne (`localhost:8001`) — aucune variable requise.

### Odoo (intégration)

Domaine `odoo` du registre (`runtime.odoo(env=None)`) — sélecteur `ODOO_ENV`
(`test`/`prod`) puis `ODOO_{TEST|PROD}_{URL,DB,USERNAME,PASSWORD}` ; `charger_config_odoo()`
reste une façade déléguant au registre. `ODOO_ENV=prod` sans variables `ODOO_PROD_*` lève
`ConfigurationManquante` au boot (plus de retombée silencieuse sur la base test).

## 2. Fichiers de configuration versionnés

| fichier | rôle | quand y toucher |
|---|---|---|
| `electricore/ingestion/config/flux.yaml` | **mouvement** des flux : `file_pattern` (glob SFTP), `format` (xml/json), `file_regex` (fichiers extraits des zips) | nouveau flux Enedis, changement de nommage des zips |
| `electricore/ingestion/dbt/profiles.yml` | connexion dbt→DuckDB (path via env), **schéma cible `flux_enedis`** | jamais ou presque |
| `electricore/ingestion/dbt/dbt_project.yml` | matérialisations (staging=view, flux=table) | jamais ou presque |
| `electricore/ingestion/dbt/models/sources.yml` | déclaration des tables brutes `raw_*` | nouveau flux |
| `electricore/ingestion/dbt/models/flux/schema.yml` | data tests `not_null` (joués à chaque build) | durcir le filet, nouveau flux |
| `electricore/config/turpe_rules.csv` | règles tarifaires TURPE (fixe + variable) | publication CRE |
| `electricore/config/accise_rules.csv` | barème accise (TICFE) | loi de finances |
| `electricore/config/cta_rules.csv` | taux CTA | évolution réglementaire |

La **sélection/typage** des flux (ex-DSL de `flux.yaml`) n'est plus de la configuration :
ce sont les modèles SQL `ingestion/dbt/models/` — voir [ingestion.md](ingestion.md).

## 3. Déploiement (`deploy/docker/`)

| fichier | rôle |
|---|---|
| `docker-compose.yml` | services api/bot/ingestion-scheduler ; `env_file: .env` + `environment:` (`DUCKDB_PATH=/data/...`, `TZ=Europe/Paris`) ; volume `duckdb_data` |
| `crontab` (copié de `crontab.example`) | planning : `/ingestion/run all` à 02:00 via l'API, backup à 03:30 |
| `Caddyfile` (copié de `.example`) | reverse proxy TLS |
| `backup_duckdb.sh` | sauvegarde quotidienne de la base |
| `Dockerfile` | image unique, `uv sync --extra ingestion --extra dbt` |

## 4. Outillage développeur (config de dev, pas de runtime)

| fichier | rôle |
|---|---|
| `pyproject.toml` | dépendances + extras (`ingestion`, `dbt`, `viz`), ruff, mypy (périmètre strict), pytest (markers), coverage |
| `.pre-commit-config.yaml` | ruff check/format + détection de secrets (et pytest en pre-push) |
| `.github/workflows/ci.yml` | lint, typecheck, tests py3.12/3.13 avec extras `ingestion`+`dbt` |

## Régénérables (pas de la config, mais souvent cherchés ici)

- Fixtures XSD et golden : `tests/fixtures/flux/generer_fixtures_xsd.py` (exige les XSD
  Enedis en local) et `generer_golden.py` (oracle = chemin dbt).
- État incrémental dlt : `~/.dlt/` + tables `_dlt_*` — purge via `ingestion.py resync`.
