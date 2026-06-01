# Changelog

Toutes les modifications notables de ce projet seront documentées dans ce fichier.

Le format est basé sur [Keep a Changelog](https://keepachangelog.com/fr/1.0.0/),
et ce projet adhère au [Semantic Versioning](https://semver.org/lang/fr/).

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
