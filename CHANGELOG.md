# Changelog

Toutes les modifications notables de ce projet seront documentées dans ce fichier.

Le format est basé sur [Keep a Changelog](https://keepachangelog.com/fr/1.0.0/),
et ce projet adhère au [Semantic Versioning](https://semver.org/lang/fr/).

---

## [Unreleased]

### 🧮 Facturation — API épaisse (en cours)

Première brique de l'API épaisse v1.5 : extraction du rapprochement Odoo↔Enedis vers le domaine `core`, prérequis pour exposer les résultats structurés en Arrow IPC aux notebooks distants.

#### Ajouts

- **`rapprocher_facturation_mensuelle()`** ([`electricore/core/pipelines/facturation.py`](electricore/core/pipelines/facturation.py)) — fonction métier pure qui joint les lignes de facture Odoo (taggées `x_ref_situation_contractuelle`) avec la facturation Enedis mensuelle et calcule `quantite_enedis` selon la catégorie produit (HP/HC/Base/Abonnements).
- **Schéma Pandera `LignesFactureRapprochees`** ([`electricore/core/models/lignes_facture_rapprochees.py`](electricore/core/models/lignes_facture_rapprochees.py)) — validation stricte en sortie via `@pa.check_types(lazy=True)`.
- **ADR-0012** ([`docs/adr/0012-api-read-only-odoo.md`](docs/adr/0012-api-read-only-odoo.md)) — politique « API read-only sur Odoo ; écritures via notebook humain-dans-la-boucle » + règle nuancée pour OdooReader en notebook (autorisé pour enrichissement, interdit en amont d'une pipeline).
- **Glossaire** ([`electricore/core/CONTEXT.md`](electricore/core/CONTEXT.md)) — entrées « Rapprochement PDL ↔ RSC » (étape amont, notebook `injection_rsc.py`) et « Rapprochement facturation mensuelle » (étape aval, exposée par l'API) pour clarifier la distinction.

- **Endpoint `GET /facturation/arrow`** ([`electricore/api/main.py`](electricore/api/main.py)) — sérialise `lignes_facture_rapprochees` en flux Arrow IPC, lisible par `pl.read_ipc_stream`. Query param `mois=YYYY-MM-DD` ; sans paramètre, dernier mois disponible. Authentification API key, comme les autres endpoints data.
- **Endpoint `GET /taxes/accise/arrow`** ([`electricore/api/main.py`](electricore/api/main.py)) — détail Accise TICFE (table par PDL × mois × trimestre) sérialisé en Arrow IPC. Query param `trimestre=YYYY-TX` (sans : tout). Les agrégations « Par taux » et « Résumé » de l'XLSX restent à charge du notebook (group_by trivial).
- **Module `electricore.client`** ([`electricore/client/__init__.py`](electricore/client/__init__.py)) — classe `ElectricoreClient(url, api_key)` avec méthodes `.facturation(mois)` et `.accise(trimestre)`, retournant un `pl.DataFrame`. Extension point pour les futurs endpoints structurés et pour le HTTP transport DuckDBQuery prévu en v1.6.

#### Modifications

- **`api/services/facturation_service.generer_facturation_xlsx`** consomme désormais `rapprocher_facturation_mensuelle()` — comportement XLSX inchangé, logique métier simplifiée. Le chargement Odoo+Enedis est factorisé dans `calculer_lignes_facture_rapprochees()`, partagé entre les services XLSX et Arrow.

#### Tests

- Isolation des tests crypto ETL ([`tests/etl/test_crypto.py`](tests/etl/test_crypto.py)) — autouse fixture qui efface `AES__*` d'`os.environ` avant chaque test `load_key_chain`. Bug pré-existant exposé en important `electricore.api.main` au niveau test (lecture `.env` au chargement de l'API).

---

## [1.4.0] - 2026-06-02

### 🚀 Déployabilité — fondations VPS

Mise en place d'une stack Docker reproductible (`docker compose up -d`) avec ETL planifié et reverse-proxy TLS. La cible : un VPS unique qui exécute l'ETL à intervalle fixe, expose l'API + bot Telegram 24/7, et héberge optionnellement les fichiers Enedis collocés (pas de SFTP redondant). L'accès distant des notebooks via API HTTP est reporté à v1.5.

#### Ajouts

- **Image Docker** ([`deploy/docker/Dockerfile`](deploy/docker/Dockerfile)) — multi-stage avec `uv` au build, runtime minimal (`python:3.13-slim` + libxml2 + supercronic + duckdb CLI), utilisateur non-root, `tini` comme PID 1. Publiée sur `ghcr.io/energie-de-nantes/electricore` à chaque tag `vX.Y.Z`.
- **Stack docker-compose** ([`deploy/docker/docker-compose.yml`](deploy/docker/docker-compose.yml)) — services `api` (FastAPI + bot), `etl-scheduler` (supercronic), `caddy` (TLS automatique). Volumes nommés pour la base DuckDB et les sauvegardes.
- **ETL planifié** via supercronic — déclenche `POST /etl/run` toutes les nuits à 02:00. Les jobs scheduled apparaissent dans `/etl/jobs` aux côtés des runs manuels (bot, API), donc l'historique reste unifié.
- **Mode "fichiers collocés"** — pour le scénario où l'application tourne sur le même VPS qu'un dépôt Enedis. Aucune modification de code : `SFTP__URL=file:///var/enedis/` est nativement supporté par `dlt.sources.filesystem`. Les fichiers restent chiffrés AES sur disque, donc les clés AES restent obligatoires. Voir [`docs/deploiement.md`](docs/deploiement.md).
- **Sauvegardes DuckDB** ([`deploy/docker/backup_duckdb.sh`](deploy/docker/backup_duckdb.sh)) — `EXPORT DATABASE` + tar.gz quotidien, rétention 14 snapshots. Restauration documentée.
- **Endpoint `/health` enrichi** ([`electricore/api/main.py`](electricore/api/main.py)) — retourne désormais un payload structuré `{database: {accessible, last_write, tables}, bot: {running}, ...}` au lieu de juste `{status: "ok"}`. Toujours `200` (même base inaccessible), pour qu'ops puisse différencier un verrou ETL transitoire d'une panne réelle.
- **Retry-on-lock DuckDB** ([`electricore/api/services/duckdb_service.py`](electricore/api/services/duckdb_service.py)) — 3 tentatives × 1s pour absorber les fenêtres de checkpoint pendant que l'ETL écrit.
- **Documentation déploiement** ([`docs/deploiement.md`](docs/deploiement.md)) — guide complet : prérequis, installation initiale, modes SFTP vs collocé, rotation clés AES, sauvegarde/restauration, dépannage.

#### Modifications

- **Import package-style** ([`electricore/etl/pipeline_production.py:29`](electricore/etl/pipeline_production.py#L29)) — `from electricore.etl.sources.sftp_enedis import flux_enedis`. Permet d'invoquer le pipeline via `python -m electricore.etl.pipeline_production` depuis n'importe quel `cwd`. Service ETL côté API ajusté en conséquence.
- **Masquage URL généralisé** ([`electricore/etl/sources/sftp_enedis.py`](electricore/etl/sources/sftp_enedis.py)) — `mask_password_in_url()` gère désormais `file://` (no-op) en plus de `sftp://`.

#### Suppressions (breaking)

- **Unit systemd `electricore.service`** supprimée (ainsi que les fichiers `.example` annexes). Les utilisateurs sur déploiement bare-metal doivent rester sur v1.3.x ou migrer vers Docker. La doc Docker couvre tous les cas d'usage de l'ancienne unit.

#### Reportés à v1.5

- Transport HTTP pour `DuckDBQuery` builders — permettra aux notebooks marimo locaux de lire les données depuis l'API distante (sans nécessiter le fichier DuckDB local).
- Extra `[viz]` optionnel — marimo, altair, vegafusion, vl-convert-python, plotly retirés des dépendances par défaut. L'image production passerait de ~1.2 GB à ~400 MB.
- Image multi-arch (arm64) — actuellement amd64 uniquement.
- Observabilité : logging JSON structuré, intégration Sentry.

---

## [1.3.5] - 2026-06-01

### 🛠️ Hygiène d'ingénierie

Première itération de mise à niveau des conventions OSS standards (CI, lint, type-checking, workflow release). Pas de changement métier — l'ensemble du comportement runtime reste identique à v1.3.4.

- **CI GitHub Actions** ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) : exécute pytest sur chaque PR (Python 3.12 + 3.13), ruff lint + format, mypy. `uv sync --locked` détecte les dérives entre `pyproject.toml` et `uv.lock`.
- **Ruff lint + format** activés sur `electricore/` et `tests/` (ruleset E/F/I/UP/B, line-length 120, notebooks/scripts exclus). 536 violations auto-corrigées. 1 vrai bug surfacé et corrigé : `__all__` exportait `expr_data_complete` (symbole inexistant) dans `electricore/core/pipelines/facturation.py`.
- **Mypy strict** (scope étroit sur la surface d'API publique : `electricore.core.loaders.*`, `electricore.core.writers.*`, `electricore.core.pipelines.orchestration`). 85 erreurs → 0. Implicit-Optional éliminé, `QueryConfig.validator` correctement typé `type[pa.DataFrameModel] | None`, paramètre `how` du join typé `Literal`.
- **Pre-commit hooks** ([`.pre-commit-config.yaml`](.pre-commit-config.yaml)) : ruff lint + format à chaque commit local.
- **Workflow release** ([`.github/workflows/release.yml`](.github/workflows/release.yml)) : déclenché sur push de tag `vX.Y.Z`, publie sur PyPI via trusted publishing (OIDC, sans token API) et crée la release GitHub avec artefacts attachés + notes auto-générées.

### 🐛 Détecté latent — non corrigé dans cette release

- `json_to_dict_from_bytes` appelé mais non défini dans `electricore/etl/transformers/parsers.py` (chemin générique JSON jamais exercé en production ; tous les flux JSON utilisent `_json_r64_transformer_base`). Annoté `# noqa: F821` + TODO ; à implémenter avant d'ajouter un nouveau type de flux JSON.

---

## [1.0.0] - 2025-10-06

### 🎉 Version majeure - Architecture moderne Polars/DuckDB

Refonte complète du projet avec migration de pandas vers Polars, nouvelle architecture modulaire, et ajout de fonctionnalités majeures. **BREAKING CHANGES** : non rétrocompatible avec v0.x.

### ✨ Ajouts majeurs

#### Architecture & Performance
- **Migration Polars 100%** : Remplacement complet de pandas par Polars pour performances optimales
- **DuckDB integration** : Query builder fluide pour interroger la base de données DLT
- **LazyFrame pipelines** : Évaluation différée pour optimisation mémoire et calcul
- **Architecture modulaire** : Séparation claire ETL / Core / API

#### Module ETL (nouveau)
- **Pipeline DLT** : Extraction automatisée depuis SFTP Enedis
- **Transformers modulaires** : Crypto (AES-256-CBC), Archive (ZIP/TAR), Parsers (XML/CSV/JSON)
- **Configuration flux** : `flux.yaml` centralisé pour tous les flux Enedis
- **Modes exécution** : Test (2 fichiers, ~3s), Flux unique (~6s), Production complète
- **Support flux** : C15, F12, F15, R15, R151, R64, R401
- **Documentation** : Guide complet ETL + outils diagnostic

#### Module Core (refonte)
- **Query Builders**
  - `DuckDBQuery` : API fluide pour C15, R151, R15, F15, R64, relevés harmonisés
  - `OdooQuery` : Navigation relations avec `.follow()`, enrichissement `.enrich()`
  - `ParquetLoader` : Chargement direct fichiers Parquet
- **Pipelines Polars**
  - `pipeline_perimetre` : Détection périmètre actif avec 8 expressions composables
  - `pipeline_abonnements` : Calcul périodes abonnement avec bornes temporelles
  - `pipeline_energie` : Consommations par cadran (HPH/HCH/HPB/HCB/HP/HC/Base)
  - `pipeline_turpe` : TURPE fixe + variable + CMDPS
- **Modèles Pandera** : Validation stricte avec décorateurs `@pa.check_types`
- **Writers** : OdooWriter pour synchronisation ERP

#### Module API (nouveau)
- **FastAPI** : API REST sécurisée avec authentification API key
- **Endpoints** : `/flux/c15`, `/flux/r151`, `/flux/r15`, `/flux/f15`, `/health`
- **Services DuckDB** : Requêtage optimisé avec filtres et pagination
- **Documentation** : OpenAPI interactive sur `/docs`

#### TURPE - Implémentation complète
- **TURPE Fixe C5** (BT ≤ 36 kVA)
  - Formule : `(b × P) + cg + cc`
  - Calcul annuel, journalier, période avec prorata
- **TURPE Fixe C4** (BT > 36 kVA) - **NOUVEAU**
  - 4 puissances souscrites : P₁ (HPH), P₂ (HCH), P₃ (HPB), P₄ (HCB)
  - Formule progressive : `b₁×P₁ + b₂×(P₂-P₁) + b₃×(P₃-P₂) + b₄×(P₄-P₃) + cg + cc`
  - Validation contrainte réglementaire : P₁ ≤ P₂ ≤ P₃ ≤ P₄
  - Économies jusqu'à 20% via modulation saisonnière
  - Détection automatique C4/C5, rétrocompatibilité totale
- **TURPE Variable** : Calcul par cadran horaire (HPH/HCH/HPB/HCB ou HP/HC ou Base)
- **CMDPS** : Pénalités dépassement puissance
- **Configuration** : `turpe_rules.csv` avec tarifs officiels CRE
- **Nomenclature CRE** : `b_*` (puissance €/kVA/an), `c_*` (énergie c€/kWh)

#### Documentation
- **README** : Guide complet architecture + quickstart + exemples
- **CLAUDE.md** : Instructions projet pour IA avec patterns établis
- **Guides modules** : ETL, API, Query Builders, Conventions dates
- **Tutoriels TURPE** : Documentation technique C4 + usage standalone
- **Tests** : 167 tests unitaires + intégration avec fixtures métier

### 🔧 Améliorations

#### Code Quality
- **Type hints** : Annotations complètes pour meilleure maintenabilité
- **Functional programming** : Expressions pures `Fn(Series) -> Series`
- **Immutabilité** : Query builders fluides sans mutation d'état
- **Tests exhaustifs** : 167 passing avec Hypothesis property-based testing
- **Fixtures métier** : Cas réels (déménagement, changements contrat, etc.)

#### Performance
- **LazyFrame optimization** : Évaluation différée par Polars
- **DuckDB analytics** : Requêtes SQL optimisées sur données DLT
- **Vectorisation** : Élimination boucles Python au profit opérations Polars
- **Mémoire** : Réduction empreinte grâce lazy evaluation

#### Developer Experience
- **Notebooks Marimo** : Exploration interactive des pipelines
- **Outils diagnostic** : Scripts analyse flux, états incrémentaux
- **Configuration centralisée** : `database.yaml`, `flux.yaml`
- **Logs structurés** : Meilleure traçabilité ETL

### 🗑️ Suppressions (Breaking Changes)

#### Modules supprimés
- `electricore.core.énergies` → remplacé par `electricore.core.pipelines.energie`
- `electricore.core.périmètre` → remplacé par `electricore.core.pipelines.perimetre`
- `electricore.core.relevés` → intégré dans `electricore.core.pipelines.energie`
- `electricore.core.pipeline_*` (ancien format) → nouveaux pipelines dans `pipelines/`
- `electricore.core.orchestration` → remplacé par `pipelines/orchestration`
- `electricore.core.taxes` → remplacé par `pipelines/turpe`
- `electricore.core.services` → remplacé par `loaders/` et `writers/`
- `electricore.inputs.flux` → parsers intégrés dans `etl/transformers/parsers.py`

#### Dépendances supprimées
- `pandas` → migration complète vers Polars
- `toolz` → remplacement par opérations Polars natives
- Autres dépendances obsolètes nettoyées

### 📦 Changements techniques

#### Dependencies
- **Ajoutées**
  - `polars >=1.0.0` (remplacement pandas)
  - `pandera[polars] >=0.24.0` (validation)
  - `duckdb >=1.3.2` (analytics)
  - `dlt[workspace] >=1.16.0` (ETL)
  - `fastapi >=0.116.1` (API)
  - `uvicorn >=0.35.0` (serveur ASGI)
  - `pycryptodome >=3.23.0` (décryptage flux)
- **Supprimées**
  - `pandas` (migration Polars)
  - `toolz` (refactor architecture)

#### Configuration
- `pyproject.toml` : Configuration Poetry moderne avec groupes optionnels
- Python : `>=3.12,<3.15` (support Python 3.12+)
- Build : `poetry-core>=2.0.0`

#### Structure projet
```
electricore/
├── etl/              # Nouveau module ETL (DLT)
├── core/
│   ├── loaders/      # Query builders (nouveau)
│   ├── pipelines/    # Pipelines Polars (refonte)
│   ├── models/       # Modèles Pandera (refonte)
│   └── writers/      # Writers Odoo (nouveau)
├── api/              # Nouveau module API (FastAPI)
└── config/           # Configuration centralisée (nouveau)
```

### 🔄 Migration depuis v0.x

⚠️ **ATTENTION** : Cette version introduit des breaking changes majeurs.

#### Actions requises

1. **Mise à jour imports**
   ```python
   # Avant (v0.x)
   from electricore.core.énergies import calculer_energies
   from electricore.core.périmètre import detecter_perimetre

   # Après (v1.0.0)
   from electricore.core.pipelines.energie import pipeline_energie
   from electricore.core.pipelines.perimetre import pipeline_perimetre
   from electricore.core.loaders import c15, r151, charger_releves
   ```

2. **Migration pandas → Polars**
   ```python
   # Les DataFrames sont maintenant des Polars DataFrames/LazyFrames
   import polars as pl

   # Avant : pandas df
   df = pd.read_csv("data.csv")

   # Après : Polars LazyFrame (recommandé)
   lf = pl.scan_csv("data.csv")
   ```

3. **Configuration**
   - Créer `config/database.yaml` pour DuckDB
   - Créer `etl/config/flux.yaml` pour ETL
   - Migrer anciens paramètres vers nouvelle structure

4. **Tests**
   - Adapter fixtures pour Polars
   - Utiliser nouveaux modèles Pandera
   - Tester avec nouveaux query builders

#### Guide complet
Consultez [CLAUDE.md](CLAUDE.md) pour architecture détaillée et patterns établis.

### 🐛 Corrections

- Fix validation Pandera pour colonnes optionnelles C4
- Fix gestion dates timezone Europe/Paris
- Fix convention dates R151 (+1 jour pour harmonisation)
- Fix calcul TURPE avec règles temporelles multiples
- Fix tests paramétrés avec colonnes C4 NULL

### 🔒 Sécurité

- API authentification par clé API
- Décryptage sécurisé flux cryptés (AES-256-CBC)
- Validation inputs stricte avec Pandera
- Configuration sensible via variables environnement

### 📊 Métriques

- **Tests** : 167 passing, 29 skipped
- **Couverture** : ~49%
- **Fichiers modifiés** : 141 fichiers
- **Lignes** : +24,734 / -9,297
- **Commits** : 287 commits depuis v0.2.7

### 🙏 Contributeurs

- Virgile - Architecture, développement, migration Polars
- Claude Code (Anthropic) - Assistance développement et refactoring

---

## [0.2.7] - 2024-08-13

Dernière version avant migration majeure v1.0.0.

### Ajouts
- Amélioration visualisation pipeline
- Mise à jour toolz vers 1.0.0

### Corrections
- Correction FutureWarning pandas pour fillna

---

## [0.1.0] - 2024-01-01

Version initiale du projet ElectriCore.

### Ajouts
- Pipeline de base pour traitement flux Enedis
- Calculs énergétiques avec pandas
- Intégration Odoo basique

---

[1.0.0]: https://github.com/Energie-De-Nantes/electricore/compare/v0.2.7...v1.0.0
[0.2.7]: https://github.com/Energie-De-Nantes/electricore/compare/v0.1.0...v0.2.7
[0.1.0]: https://github.com/Energie-De-Nantes/electricore/releases/tag/v0.1.0
