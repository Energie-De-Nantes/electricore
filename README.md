# ⚡ ElectriCore - Moteur de traitement données énergétiques

**ElectriCore** est un outil libre pour reprendre le contrôle des données du réseau électrique français. Architecture moderne **Polars + DuckDB** pour transformer les flux bruts Enedis en données exploitables par LibreWatt, Odoo et autres outils de suivi énergétique.

## 🎯 Objectifs

Un outil de calcul énergétique **performant** et **maintenable** pour :
- ✅ **Transformer** les flux XML/CSV Enedis en données structurées
- ✅ **Calculer** les indicateurs essentiels (périmètre, abonnements, consommations, TURPE)
- ✅ **Exposer** les données via API REST sécurisée
- ✅ **Intégrer** avec Odoo et autres systèmes tiers

---

## 🏗️ Architecture - Modules

```
electricore/
├── ingestion/        # 📥 INGESTION - Flux Enedis → DuckDB (ELT dlt + dbt)
│
├── core/             # 🧮 CORE - Calculs énergétiques ERP-agnostiques (Polars)
│   ├── models/       # Modèles Pandera (validation des données)
│   ├── pipelines/    # Pipelines de calcul (historique, abonnements, énergie, turpe, accise…)
│   ├── builds/       # Livrables assemblés (contexte mensuel, rapports — ADR-0019)
│   └── loaders/      # Query builders (DuckDB, Parquet)
│
├── integrations/     # 🔌 INTEGRATIONS - Adaptateurs ERP (cf. ADR-0016)
│   └── odoo/         # OdooReader, OdooQuery, OdooWriter, helpers, schémas Pandera Odoo
│
├── api/              # 🌐 API - Accès aux données (FastAPI)
│
└── bot/              # 🤖 BOT Telegram - UI opérationnelle, client de l'API
```

`core/` ne dépend ni d'Odoo ni d'aucun ERP — règle exécutable via le test [`tests/architecture/test_core_purity.py`](tests/architecture/test_core_purity.py) (cf. [ADR-0016](docs/adr/0016-core-erp-agnostique.md)). Les orchestrations qui composent Enedis et Odoo (rapprochement facturation, accise/CTA) vivent dans `integrations/odoo/`.

### Diagramme de flux

```mermaid
graph TB
    SFTP_Enedis[/SFTP Enedis\] --> Ingestion_Enedis[Ingestion<br/>#40;dlt + dbt#41;]
    SFTP_Axpo[/SFTP Axpo<br/>Courbes\] -.-> Ingestion_Axpo[Ingestion<br/>#40;dlt + dbt#41;]
    Ingestion_Enedis --> DuckDB[(DuckDB)]
    Ingestion_Axpo -.-> DuckDB
    Odoo[(Odoo ERP)] --> OdooReader[OdooReader]
    OdooWriter[OdooWriter] --> Odoo

    DuckDB --> API[API REST<br/>#40;FastAPI#41;]
    DuckDB -->|Query Builder| Core[Core Pipelines<br/>#40;Polars#41;]
    OdooReader -->|Query Builder| Core
    OdooReader --> API
    Core --> API
    Core --> OdooWriter

    API -->|JSON| Client[\Clients API/]

    style API fill:#4CAF50,stroke:#2E7D32,stroke-width:3px,color:#fff
    style DuckDB fill:#1976D2,stroke:#0D47A1,color:#fff
    style Odoo fill:#FF9800,stroke:#E65100,color:#fff
    style Core fill:#9C27B0,stroke:#4A148C,color:#fff
    style Ingestion_Axpo stroke-dasharray: 5 5
```

---

## 📥 Module ingestion — Flux Enedis → DuckDB

Ingestion modulaire basée sur **dlt** (landing brut) et **dbt** (linéarisation SQL) pour les flux Enedis.

### Flux supportés

| Flux   | Description                  | Tables générées               |
|--------|------------------------------|-------------------------------|
| **C15** | Changements contractuels    | `flux_c15`                    |
| **F12** | Facturation distributeur    | `flux_f12`                    |
| **F15** | Facturation détaillée       | `flux_f15_detail`             |
| **R15** | Relevés avec événements     | `flux_r15`, `flux_r15_acc`    |
| **R151**| Relevés périodiques         | `flux_r151`                   |
| **R64** | Relevés JSON timeseries     | `flux_r64`                    |

### Architecture modulaire

```python
# Ingestion avec transformers chaînables
encrypted_files | decrypt_transformer | unzip_transformer | parse_transformer
```

**Transformers disponibles** :
- `crypto.py` - Déchiffrement AES
- `archive.py` - Extraction ZIP
- `parsers.py` - Parsing XML/CSV

### Utilisation

```bash
# Test rapide (2 fichiers)
uv run python -m electricore.ingestion test

# R151 complet (~6 secondes)
uv run python -m electricore.ingestion r151

# Tous les flux (production)
uv run python -m electricore.ingestion all
```

**Résultat** : Base DuckDB `electricore/ingestion/flux_enedis_pipeline.duckdb` avec toutes les tables flux.

📖 **Documentation complète** : [electricore/ingestion/README.md](electricore/ingestion/README.md)

---

## 🧮 Module Core - Calculs Énergétiques Polars

Pipelines de calculs énergétiques basés sur **Polars pur** (LazyFrames + expressions fonctionnelles).

### Pipelines disponibles

#### 1. **Historique** - Événements contractuels enrichis (ex-périmètre, [ADR-0013](docs/adr/0013-renommage-perimetre-historique.md))
```python
from electricore.core.pipelines.historique import pipeline_historique
from electricore.core.loaders import c15

# Depuis DuckDB avec Query Builder
historique_lf = (
    c15()
    .filter({"Date_Evenement": ">= '2024-01-01'"})
    .lazy()
)

historique_enrichi = pipeline_historique(historique_lf)
# Événements C15 enrichis : impacte_abonnement, impacte_energie, resume_modification, ...
```

#### 2. **Abonnements** - Périodes d'abonnement
```python
from electricore.core.pipelines.abonnements import pipeline_abonnements

# Périodes homogènes de la part fixe, dérivées de l'historique enrichi
abonnements_lf = pipeline_abonnements(historique_enrichi)
# Colonnes: pdl, mois_annee, nb_jours, puissance_souscrite_kva, formule_tarifaire_acheminement, ...
```

#### 3. **Énergies** - Consommations par cadran
```python
from electricore.core.pipelines.energie import pipeline_energie
from electricore.core.loaders import releves_harmonises

releves_lf = releves_harmonises().lazy()

energie_lf = pipeline_energie(historique_enrichi, releves_lf)
# Colonnes: pdl, debut, fin, energie_base_kwh, energie_hp_kwh, energie_hc_kwh, ...
```

#### 4. **TURPE** - Tarif d'acheminement réseau
```python
from electricore.core.pipelines.turpe import ajouter_turpe_fixe, ajouter_turpe_variable

# TURPE fixe (périodes d'abonnement)
abonnements_turpe = ajouter_turpe_fixe(abonnements_lf)
# Colonnes: ..., turpe_fixe_eur

# TURPE variable (périodes d'énergie)
energie_turpe = ajouter_turpe_variable(energie_lf)
# Colonnes: ..., turpe_variable_eur
```

#### 5. **Contexte mensuel** - Pipeline complet ([ADR-0019](docs/adr/0019-roles-loaders-pipelines-builds-integrations.md))
```python
from electricore.core.builds.contexte_mensuel import contexte_du_mois

# Compose : historique → abonnements + énergie → facturation mensuelle
ctx = contexte_du_mois("2026-05-01")   # None → dernier mois disponible

ctx.facturation_mensuelle   # DataFrame agrégé du mois (validé Pandera)
ctx.abonnements             # LazyFrame des périodes d'abonnement
ctx.energie                 # LazyFrame des périodes d'énergie
ctx.historique_enrichi      # LazyFrame de l'historique enrichi
```

### 🔧 Interfaces de Requêtage

#### DuckDB Query Builder - Architecture Fonctionnelle Modulaire

**Architecture en 6 modules** pour performance et maintenabilité :
- `config.py` - Configuration et connexions DuckDB
- `expressions.py` - Expressions Polars pures réutilisables
- `transforms.py` - Transformations composables avec `compose()`
- `sql.py` - Génération SQL fonctionnelle (dataclasses frozen)
- `query.py` - Query builder immutable (`DuckDBQuery`)
- `__init__.py` - API publique + helper `_CTEQuery` pour requêtes CTE

```python
from electricore.core.loaders import c15, r151, releves, releves_harmonises

# Historique périmètre (flux C15)
historique = (
    c15()
    .filter({"Date_Evenement": ">= '2024-01-01'"})
    .limit(100)
    .collect()
)

# Relevés périodiques (flux R151)
relevés = (
    r151()
    .filter({"pdl": ["PDL123", "PDL456"]})
    .limit(1000)
    .lazy()  # Retourne LazyFrame pour optimisations
)

# Relevés unifiés (R151 + R15) avec CTE
tous_releves = releves().collect()

# Relevés harmonisés (R151 + R64) avec CTE
releves_cross_flux = (
    releves_harmonises()
    .filter({"flux_origine": "R64"})
    .collect()
)
```

**Fonctions disponibles** : `c15()`, `r151()`, `r15()`, `f15()`, `r64()`, `releves()`, `releves_harmonises()`

**Caractéristiques** :
- ✅ Immutabilité garantie (frozen dataclasses)
- ✅ Composition fonctionnelle pure
- ✅ Lazy evaluation optimisée
- ✅ Support CTE (Common Table Expressions)
- ✅ Validation Pandera intégrée

📖 **Documentation** : docstrings des modules [electricore/core/loaders/duckdb/](electricore/core/loaders/duckdb/)

#### Odoo Query Builder - Intégration ERP

```python
from electricore.integrations.odoo import OdooReader
import polars as pl

config = {
    'url': 'https://odoo.example.com',
    'db': 'production',
    'username': 'api_user',
    'password': 'secret'
}

with OdooReader(config) as odoo:
    # Query builder avec navigation relationnelle
    factures_df = (
        odoo.query('sale.order', domain=[('x_pdl', '!=', False)])
        .follow('invoice_ids', fields=['name', 'invoice_date', 'amount_total'])
        .filter(pl.col('amount_total') > 100)
        .collect()
    )

    # Enrichissement avec données liées
    commandes_enrichies = (
        odoo.query('sale.order', fields=['name', 'date_order'])
        .enrich('partner_id', fields=['name', 'email'])
        .collect()
    )
```

**Méthodes disponibles** : `.query()`, `.follow()`, `.enrich()`, `.filter()`, `.select()`, `.rename()`, `.collect()`

📖 **Documentation complète** : [docs/odoo-query-builder.md](docs/odoo-query-builder.md)

---

## 🌐 Module API - Accès aux Données

API REST sécurisée basée sur **FastAPI** pour accéder aux données flux depuis DuckDB.

### Endpoints

#### Publics (sans authentification)
- `GET /` - Informations API et tables disponibles
- `GET /health` - Statut API et base de données
- `GET /docs` - Documentation Swagger interactive

#### Sécurisés (authentification requise)
- `GET /flux/{table_name}` - Données d'une table flux (+ `/info` : métadonnées, fraîcheur)
- `POST /ingestion/run`, `GET /ingestion/jobs` - Déclenchement et suivi des jobs d'ingestion
- `GET /taxes/...` - Accise, CTA (exports Arrow/XLSX)
- `GET /facturation/...` - Documents mensuels, check pré-facturation Odoo (XLSX)
- `GET /admin/api-keys` - Configuration clés API

### Utilisation

```bash
# Démarrer l'API
uv run uvicorn electricore.api.main:app --reload

# Requête avec authentification
curl -H "X-API-Key: votre_cle" "http://localhost:8000/flux/r151?limit=10"

# Filtrer par PDL
curl -H "X-API-Key: votre_cle" "http://localhost:8000/flux/c15?prm=12345678901234"

# Métadonnées d'une table
curl -H "X-API-Key: votre_cle" "http://localhost:8000/flux/r151/info"
```

📖 **Documentation complète** : [electricore/api/README.md](electricore/api/README.md)

---

## 🚀 Installation & Usage

### Prérequis

- Python 3.12+
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

### Installation

```bash
# Cloner le projet
git clone https://github.com/Energie-De-Nantes/electricore.git
cd electricore

# Installation runtime (core + API + bot)
uv sync

# + ingestion SFTP Enedis, dlt + dbt (pour serveur de collecte)
uv sync --extra ingestion --extra dbt

# + libs notebooks (marimo, altair, plotly, vegafusion, vl-convert)
uv sync --extra viz

# Tout (dev local complet)
uv sync --extra ingestion --extra dbt --extra viz
```

### Configuration initiale

Créer un fichier `.env` à la racine du projet et renseigner les variables selon les modules utilisés :

```bash
# === API — obligatoire pour démarrer l'API ===
# Générer : python -c "import secrets; print(secrets.token_urlsafe(32))"
API_KEY=votre_cle_api_secrete
# Ou plusieurs clés : API_KEYS=cle1,cle2,cle3

# === BASE DE DONNÉES ===
# Chemin vers le fichier DuckDB produit par l'ingestion
# Par défaut : electricore/ingestion/flux_enedis_pipeline.duckdb (relatif au cwd)
# En production, utiliser un chemin absolu :
# DUCKDB_PATH=/srv/<slug>/data/flux_enedis_pipeline.duckdb

# === INGESTION ENEDIS — obligatoire pour l'ingestion ===
SFTP__URL=sftp://utilisateur:mot_de_passe@hote:22/chemin
# Trousseau de clés AES (ADR-0037) : un <label> parlant par clé, sélection par essai.
# Garder les anciennes clés dans le trousseau préserve l'accès aux archives passées.
# __IV optionnel (ADR-0040) : absent ⇒ schéma IV-préfixé (AES-256) ; présent ⇒ IV-fixe (AES-128).
AES__TROUSSEAU__aes256_2026__KEY=cle_hex_fournie_par_enedis   # AES-256 : pas de __IV
# AES__TROUSSEAU__aes128_2024__KEY=ancienne_cle_hex           # AES-128 : clé + IV
# AES__TROUSSEAU__aes128_2024__IV=ancien_iv_hex

# === ODOO — pour les calculs CTA/Accise et la réconciliation ===
ODOO_ENV=test  # ou prod
ODOO_TEST_URL=https://votre-instance.odoo.com
ODOO_TEST_DB=nom_de_la_base
ODOO_TEST_USERNAME=utilisateur@example.com
ODOO_TEST_PASSWORD=mot_de_passe
# ODOO_PROD_URL / ODOO_PROD_DB / ODOO_PROD_USERNAME / ODOO_PROD_PASSWORD

# === BOT TELEGRAM — optionnel ===
TELEGRAM_BOT_TOKEN=token_obtenu_via_botfather  # convention : @<slug>_electricore_bot
TELEGRAM_ALLOWED_USERS=123456789   # IDs séparés par virgule
TELEGRAM_NOTIFY_CHAT_ID=           # chat des alertes (échec d'ingestion) ; vide = désactivé
```

### Commandes essentielles

```bash
# Tests
uv run --group test pytest -q

# Ingestion complète (nécessite --extra ingestion)
uv run python -m electricore.ingestion all

# API FastAPI
uv run uvicorn electricore.api.main:app --reload

# Notebooks interactifs (Marimo)
uv run marimo edit notebooks/
```

#### Lanceur opérateur `electricore-notebooks` (pont transitoire — retirer à l'arrivée de `souscriptions_odoo`)

Pour qu'un opérateur non-dev fasse tourner les notebooks Odoo opérationnels
(`facturation`, `injection_rsc`) sans git ni code : installer l'extra `notebooks`
puis lancer la commande. Les deux notebooks sont servis en mode **run**
(lecture seule), chacun avec son mode simulation par défaut et son bouton
« Injecter dans Odoo » gardés.

```bash
uv sync --extra notebooks
uv run electricore-notebooks   # valide l'env, sert les apps sur localhost, ouvre le navigateur
```

Variables requises (sinon message clair + sortie non-nulle) : creds Odoo
(`ODOO_<ENV>_*`) + `ELECTRICORE_API_URL` + `ELECTRICORE_API_KEY` (voir `.env.example`).

### Déploiement VPS (Docker)

Pour exécuter ElectriCore en production sur un VPS — ingestion planifiée, API + bot 24/7, TLS automatique — un script d'installation provisionne tout depuis un VPS Ubuntu/Debian frais :

```bash
ssh root@<vps>
curl -fsSL https://raw.githubusercontent.com/Energie-De-Nantes/electricore/main/deploy/install.sh -o install.sh
EDITOR=nano sudo -E bash install.sh \
    --slug <slug> --domain <slug>.electricore.fr --email ops@example.com
```

Le script crée un user système dédié, installe Docker, configure UFW, télécharge la stack, ouvre `.env` dans l'éditeur, vérifie le DNS, démarre les conteneurs et lance une ingestion test (~5-10 min total).

Guide complet : [`docs/deploiement.md`](docs/deploiement.md) (Quickstart, multi-instance, mode SFTP distant vs collocés, rotation clés AES, sauvegarde, mise à jour, migration depuis l'ancien layout, dépannage).

---

## 🧪 Tests & Validation

Suite de tests avec **671 tests passants** (35 skips légitimes, juin 2026 ✅) :

### Infrastructure de test

- ✅ **Configuration pytest** : 8 markers (unit, integration, slow, smoke, duckdb, odoo, hypothesis, skip_ci)
- ✅ **Fixtures partagées** : Connexions DuckDB temporaires, données minimales, helpers d'assertion
- ✅ **Tests paramétrés** : `@pytest.mark.parametrize` pour réduire la duplication
- ✅ **Tests snapshot** : Syrupy pour détection automatique de régression
- ✅ **Golden d'ingestion** : fixtures générées depuis les XSD Enedis, parité dbt garantie en CI
- ✅ **Script anonymisation** : Extraction sécurisée de cas métier réels

### Types de tests

- **Tests unitaires** - Expressions Polars pures (historique, abonnements, énergie, TURPE, taxes), bot, sérialiseurs API
- **Tests d'intégration** - Endpoints API (flux, taxes, facturation, ingestion), snapshots de pipelines
- **Tests d'ingestion** - Golden dbt par flux, crypto AES, xml→dict, modes du runner
- **Tests d'architecture** - Pureté ERP de `core/` (ADR-0016), imports par rôle (ADR-0019), topologies bot/builds
- **Fixtures métier** - Cas réels (MCT, MES/RES, changements)

### Commandes

```bash
# Tous les tests
uv run --group test pytest -q

# Tests rapides uniquement
uv run --group test pytest -m unit

# Exécution parallèle
uv run --group test pytest -n auto

# Avec coverage
uv run --group test pytest --cov=electricore --cov-report=html
```

**Couverture** : ~77% (juin 2026, seuil CI à 45%)

📖 Documentation complète : [tests/README.md](tests/README.md)

---

## 🗺️ Roadmap

### Complété ✅
- Migration Polars complète (périmètre, abonnements, énergies, turpe, accise/CTA)
- Query Builder DuckDB avec architecture fonctionnelle modulaire (6 modules)
- Connecteur Odoo avec Query Builder ; `core/` ERP-agnostique ([ADR-0016](docs/adr/0016-core-erp-agnostique.md))
- API FastAPI sécurisée avec authentification (hub central, [ADR-0009](docs/adr/0009-architecture-api-centrique.md))
- Ingestion ELT dlt + dbt en production ([ADR-0020](docs/adr/0020-linearisation-en-dbt.md), [ADR-0021](docs/adr/0021-bascule-production-dbt.md))
- Bot Telegram par domaines métier ([ADR-0022](docs/adr/0022-surface-bot-domaines-hybrides.md))
- Déploiement VPS Docker + script d'installation ([ADR-0011](docs/adr/0011-deploiement-vps-docker.md))
- CI/CD GitHub Actions (tests + release d'images ghcr.io)

### En cours 🔄
- Property-based testing des pipelines (issues #194–#198)
- Millésime et péremption des taux régulés (issues #185–#187)
- Centralisation de la configuration runtime ([ADR-0025](docs/adr/0025-registre-runtime-pydantic-settings.md), issue #141)

### À venir 📅
- Régularisation des contrats lissés (épique #191)
- API SOAP Enedis (alternative SFTP)
- Nouveaux connecteurs (Axpo, autres sources)
- Suivi des souscriptions aux services de données

---

## 📚 Documentation Complémentaire

- [Ingestion README](electricore/ingestion/README.md) - Ingestion des flux Enedis
- [API README](electricore/api/README.md) - API REST et authentification
- [Bot README](electricore/bot/README.md) - Bot Telegram : surface de commandes, alertes, no-ERP
- [Query Builder DuckDB](electricore/core/loaders/duckdb/) - Modules `query.py`, `sql.py`, `expressions.py`
- [Query Builder Odoo](docs/odoo-query-builder.md) - Intégration Odoo
- [Conventions Dates](docs/conventions-dates-enedis.md) - Formats temporels Enedis
- [CONTEXT-MAP.md](CONTEXT-MAP.md) — Carte des contextes multi-modules (vocabulaire métier dans `electricore/core/CONTEXT.md`, plus contextes ingestion/api/bot)
- [docs/adr/](docs/adr/) — Décisions architecturales (monorepo, Polars, DuckDB, harmonisation R151…)

---

## 🤝 Contribution

Les contributions sont les bienvenues ! Avant toute modification :

1. Lancer les tests : `uv run --group test pytest -q`
2. Vérifier la cohérence avec les patterns Polars existants
3. Documenter les nouvelles fonctionnalités
4. Suivre les conventions de code du projet

---

## 📄 Licence

AGPL-3.0 - Voir [LICENSE](LICENSE)

---

## 🙏 Remerciements

- **Polars** - Framework data processing moderne
- **DuckDB** - Base analytique embarquée
- **dlt** - Chargement de données déclaratif
- **FastAPI** - Framework API performant
- **Pandera** - Validation schémas données