# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**ElectriCore** is a French energy data processing engine - an open-source tool to reclaim control over electricity grid data. It transforms raw Enedis (French electricity distributor) flux into structured, exploitable data for LibreWatt, Odoo ERP, and other energy management tools.

**Stack**: Polars + DuckDB for high-performance data processing. Pure Polars (no pandas dependency).

**Three main modules**:
- **Ingestion**: Automated ELT pipeline (dlt + dbt) - SFTP → DuckDB
- **Core**: Energy calculation pipelines (Polars LazyFrames)
- **API**: Secure REST API (FastAPI) with authentication

## Key Commands

```bash
# Install (uv)
uv sync                          # core + API + bot (runtime production)
uv sync --extra ingestion --extra dbt  # + pipeline d'ingestion SFTP (dlt + dbt)
uv sync --extra viz              # + libs notebooks (marimo, altair, plotly…)
uv sync --extra ingestion --extra dbt --extra viz  # tout (dev local complet)

# Run tests
uv run --group test pytest -q

# Run with coverage
uv run --group test pytest --cov=electricore tests/

# Build/package
uv build

# Run ingestion pipeline (requires [ingestion] + [dbt] extras)
uv run python -m electricore.ingestion all

# Start API server
uv run uvicorn electricore.api.main:app --reload
```

## Architecture

### Module Structure

```
electricore/
├── ingestion/                 # 📥 INGESTION - Flux Enedis → DuckDB (ELT dlt + dbt)
│   ├── sources/               # DLT sources (SFTP Enedis)
│   ├── transformers/          # Modular transformers (crypto, archive, parsers)
│   ├── config/                # Flux configuration
│   ├── dbt/                   # Modèles dbt (staging + flux_*, linéarisation SQL)
│   ├── runner.py          # Ingestion (landing brut → dbt build)
│   └── __main__.py        # CLI : python -m electricore.ingestion
│
├── core/                      # 🧮 CORE - Energy Calculations (ERP-agnostique, ADR-0016)
│   ├── pipelines/             # Polars pipelines
│   │   ├── historique.py      # Historique périmètre enrichi (événements C15, ADR-0013)
│   │   ├── periodes.py        # Formules de période partagées (ADR-0023)
│   │   ├── abonnements.py     # Subscription periods
│   │   ├── energie.py         # Energy consumption by time slot
│   │   ├── turpe.py           # TURPE network tariff (fixed + variable)
│   │   ├── accise.py          # Accise (TICFE) tax calculation
│   │   ├── cta.py             # CTA (contribution tarifaire d'acheminement)
│   │   ├── taux.py            # Taux régulés en vigueur
│   │   └── facturation.py     # Monthly billing aggregation (Pandera-validated)
│   ├── builds/                # Livrables assemblés au boundary I/O (ADR-0019)
│   │   ├── contexte_mensuel.py    # contexte_du_mois() / charger() / rapprocher()
│   │   ├── rapport_facturation.py # RapportFacturation (ERP-agnostique)
│   │   └── rapport_taxe.py        # RapportTaxe
│   ├── models/                # Pandera validation schemas (Enedis only)
│   ├── loaders/               # Data loading & query builders
│   │   └── duckdb/            # DuckDBQuery builder (c15, r151, releves, etc.)
│   └── writers/               # Data export (ERP-agnostique, vide pour l'instant)
│
├── integrations/              # 🔌 INTEGRATIONS - Adaptateurs ERP (ADR-0016)
│   └── odoo/                  # OdooReader, OdooQuery, OdooWriter, helpers, transforms
│
├── api/                       # 🌐 API - REST Access Layer (hub central, voir ADR-0009)
│   ├── services/              # Query services (DuckDB, jobs d'ingestion, facturation, taxes)
│   ├── security.py            # API key authentication
│   └── main.py                # FastAPI application
│
├── bot/                       # 🤖 Bot Telegram - UI opérationnelle (voir ADR-0010, ADR-0022)
│   ├── app.py                 # Assemblage : application, menu natif, surveillance
│   ├── handlers/              # Un module par domaine (/ingestion, /flux, /perimetre, /taxes, /facturation)
│   ├── auth.py                # @require_allowed (allowlist), @require_odoo (no-ERP)
│   ├── surveillance.py        # Alertes proactives (échec de job d'ingestion)
│   └── client.py              # Client HTTP async vers l'API
│
└── config/                    # ⚙️ Tariff rules (TURPE, Accise CSV files)
```

Production deployment: VPS + Docker Compose stack ([deploy/docker/](deploy/docker/), guide [docs/deploiement.md](docs/deploiement.md)), voir [ADR-0011](docs/adr/0011-deploiement-vps-docker.md).

### Supported Flux Types

| Flux    | Description               | Tables                     |
|---------|---------------------------|----------------------------|
| **C15** | Contract changes          | `flux_c15`                 |
| **F12** | Distributor invoicing     | `flux_f12`                 |
| **F15** | Detailed invoices         | `flux_f15_detail`          |
| **R15** | Meter readings + events   | `flux_r15`, `flux_r15_acc` |
| **R151**| Periodic readings         | `flux_r151`                |
| **R64** | JSON timeseries readings  | `flux_r64`                 |

### Data Flow

```
SFTP Enedis → Ingestion (dlt + dbt) → DuckDB → Query Builders → Core Pipelines → Results
                                  ↓
                                 API (FastAPI) → Clients
                                  ↓
                              Odoo (XML-RPC)
```

## Established Patterns

### Functional Programming
- **Pure expressions**: `Fn(Series) -> Series` - Composable transformations
- **LazyFrame pipelines**: `Fn(LazyFrame) -> LazyFrame` - Optimized execution
- **Chainable operators**: Method chaining for readable data flows

### Query Builders (Immutable, Fluent API)
- **DuckDBQuery**: `c15()`, `r151()`, `r15()`, `f15()`, `r64()`, `releves()`, `releves_harmonises()`
- **OdooQuery**: `.query()`, `.follow()`, `.enrich()` with auto-detection

### Ingestion Modularity
- **Transformer pipeline**: `encrypted | decrypt | unzip | parse`
- Each transformer is isolated, testable, and reusable

### Validation
- **Pandera schemas**: `@pa.check_types(lazy=True)` decorators on facturation pipeline functions
- Pandera is a hard dependency (not optional)

## Important Notes

### Domain & Conventions
- **Language**: Business domain in French (périmètre, relevés, énergies, abonnements, TURPE)
- **Timezone**: All dates use `Europe/Paris` timezone
- **Date convention**: R151 flux uses +1 day adjustment to harmonize with R64/R15/C15 (see [docs/conventions-dates-enedis.md](docs/conventions-dates-enedis.md))
- **Column naming**: Follows `grandeur_cadran_unité` format
- **Domain glossary**: see [CONTEXT-MAP.md](CONTEXT-MAP.md) — multi-context layout, business vocabulary in [`electricore/core/CONTEXT.md`](electricore/core/CONTEXT.md) (PDL, FTA, TURPE, cadrans, événements C15, périmètre, abonnement, Odoo); module-specific terms in `ingestion/api/bot/CONTEXT.md`.
- **Architecture decisions**: see [docs/adr/](docs/adr/) — load-bearing past decisions (monorepo, Polars-only, R151 date adjustment, French language, DuckDB, DLT, query builders, AES rotation, API-centric architecture, Telegram bot, VPS Docker deployment, core ERP-agnostique, rôles loaders/pipelines/builds/integrations, linéarisation dbt + bascule production, surface bot par domaines, périodisations séparées, trois registres de savoir, facturation calendaire)

#### Column Naming Conventions

All energy meter data columns follow the **`grandeur_cadran_unité`** format for clarity and consistency:

**Index columns** (cumulative meter readings):
- `index_base_kwh` - Base tariff index
- `index_hp_kwh`, `index_hc_kwh` - Peak/Off-peak hours (HPHC tariff)
- `index_hph_kwh`, `index_hpb_kwh`, `index_hch_kwh`, `index_hcb_kwh` - 4 time slots (Tempo/EJP tariffs)
- `avant_index_*_kwh`, `apres_index_*_kwh` - Before/after readings for contract changes (C15 flux)

**Energy columns** (calculated consumption):
- `energie_base_kwh`, `energie_hp_kwh`, `energie_hc_kwh` - Main energy consumption by time slot
- `energie_hph_kwh`, `energie_hpb_kwh`, `energie_hch_kwh`, `energie_hcb_kwh` - Detailed 4-slot consumption

**Power columns** (subscribed power):
- `puissance_souscrite_kva` - Single subscribed power (BT ≤ 36 kVA, C5 tariff)
- `puissance_souscrite_hph_kva`, `puissance_souscrite_hch_kva`, `puissance_souscrite_hpb_kva`, `puissance_souscrite_hcb_kva` - Multi-slot subscribed power (BT > 36 kVA, C4 tariff)

**TURPE columns** (network tariff costs):
- `turpe_fixe_eur` - Fixed TURPE cost (€/day based on subscribed power)
- `turpe_variable_eur` - Variable TURPE cost (€/kWh based on consumption)

**Time slots (cadrans temporels)**:
- `base` - Base tariff (single price)
- `hp`/`hc` - Peak/Off-peak (Heures Pleines/Heures Creuses)
- `hph`/`hpb`/`hch`/`hcb` - High/Low season Peak/Off-peak (Haute/Basse saison, Heures Pleines/Creuses)

### Import Paths
```python
# Loaders (read operations) — core/ ERP-agnostique (ADR-0016)
from electricore.core.loaders import (
    c15, r151, r15, releves,           # DuckDB query builders
)

# Adaptateur Odoo — vit dans integrations/, pas dans core/
from electricore.integrations.odoo import (
    OdooReader, OdooQuery, OdooWriter,  # connecteurs
    query, commandes_lignes, lignes_factures_du_mois,  # helpers d'accès
)

# Pipelines individuels
from electricore.core.pipelines.historique import pipeline_historique
from electricore.core.pipelines.abonnements import pipeline_abonnements
from electricore.core.pipelines.energie import pipeline_energie
from electricore.core.pipelines.turpe import ajouter_turpe_fixe, ajouter_turpe_variable
from electricore.core.pipelines.accise import pipeline_accise
from electricore.core.pipelines.cta import pipeline_cta
from electricore.core.pipelines.facturation import pipeline_facturation

# Contexte mensuel complet (build, ADR-0019) — remplace l'ancienne orchestration
from electricore.core.builds.contexte_mensuel import contexte_du_mois, charger, rapprocher
```

### Naming Conventions
- Query builders: `DuckDBQuery` (dans `core/loaders/duckdb/`) et `OdooQuery` (dans `integrations/odoo/`)
- Sous-packages : `core/loaders/duckdb/`, `integrations/odoo/` (pas de suffixe `_loader`)

## Key Modules & Quick Examples

### 1. Ingestion Pipeline

```bash
# Test mode (2 files, ~3s)
uv run python -m electricore.ingestion test

# Single flux (R151 complete, ~6s)
uv run python -m electricore.ingestion r151

# Full production (all flux)
uv run python -m electricore.ingestion all
```

Result: DuckDB database at `electricore/ingestion/flux_enedis_pipeline.duckdb`

#### Trousseau de clés AES Enedis (ADR-0037)

Enedis rote ses clés AES périodiquement et en change la longueur (bascule
AES-128 → AES-256 du 8-9 juin 2026). Le registre runtime
(`electricore/config/runtime.py`, domaine `aes`, #141/ADR-0025/ADR-0037) expose un
**trousseau de taille arbitraire** — **en variables d'environnement uniquement**
(`.env` ou env système ; le support `.dlt/secrets.toml` a été retiré) :

```bash
AES__TROUSSEAU__aes256_2026__KEY=cle_hex_64   # AES-256 (32 octets), SANS __IV → schéma IV-préfixé (ADR-0040)
AES__TROUSSEAU__aes128_2024__KEY=cle_hex_32   # AES-128 historique (16 octets)
AES__TROUSSEAU__aes128_2024__IV=iv_hex_32     # IV présent → schéma IV-fixe
```

L'`__IV` est **optionnel** (ADR-0040) : présent ⇒ schéma **IV-fixe** (AES-128, IV en
config) ; absent ⇒ schéma **IV-préfixé** (AES-256, l'IV est en tête de chaque fichier).

Le `<label>` est un nom parlant choisi par l'opérateur ; il remonte dans les logs de
déchiffrement. La bonne clé est **sélectionnée par essai** (oracle PKCS7 + magic bytes
ZIP), sans aucune date ni notion de protocole — AES-128 et AES-256 sont le même schéma,
la longueur de clé est auto-sélectionnée.

Procédure de rotation (secrets-as-code, ADR-0044) — en prod, le trousseau vit dans le
`secrets.env` **chiffré** du provider, plus dans un `.env` édité sur la box :
1. Obtenir la nouvelle clé Enedis
2. Sur la machine admin, édition chiffrée in-place : `sops providers/<slug>/secrets.env`,
   ajouter un nouveau label (`AES__TROUSSEAU__<nouveau>__KEY`, plus `__IV` seulement si Enedis
   en fournit un — schéma IV-fixe ; sans IV ⇒ IV-préfixé, ADR-0040)
3. Commit → push → `reconfigure` sur la box (pull + déchiffre + restart) — chaque fichier
   déchiffre avec la clé qui marche, par essai
4. Retirer une vieille clé seulement quand plus aucune archive chiffrée avec elle n'est (re)téléchargeable

> **`sops updatekeys` ≠ rotation de secret** (ADR-0044) : `updatekeys` (ex. ajout d'une box
> via `deploy/add-provider.sh`) ne rote que l'**enveloppe** (destinataires age). Une clé qui a
> pu **fuiter** doit être changée **à la source** (nouvelle clé Enedis), pas juste re-wrappée.

(Dev local non-conteneur : `uv run` lit toujours `.env`/env système directement —
pydantic-settings — non concerné par SOPS.)

L'échec de déchiffrement n'est plus silencieux : un flux qui a des fichiers mais **0**
déchiffrement réussi fait passer le job à `failed` → la surveillance bot alerte
(escalade per-flux, ADR-0037). Supersède [ADR-0008](docs/adr/0008-rotation-cles-aes.md) ;
porté par [issue #221](https://github.com/Energie-De-Nantes/electricore/issues/221).

### 2. Core Pipelines

```python
from electricore.core.pipelines.historique import pipeline_historique
from electricore.core.loaders import c15

# Load from DuckDB with query builder
historique_lf = c15().filter({"Date_Evenement": ">= '2024-01-01'"}).lazy()

# Run pipeline
historique_enrichi = pipeline_historique(historique_lf).collect()
# → événements C15 enrichis : impacte_abonnement, impacte_energie, resume_modification…

# Contexte mensuel complet (historique → abonnements + énergie → facturation)
from electricore.core.builds.contexte_mensuel import contexte_du_mois
ctx = contexte_du_mois("2026-05-01")  # None → dernier mois disponible
ctx.facturation_mensuelle             # DataFrame agrégé du mois
```

### 3. DuckDB Query Builder

```python
from electricore.core.loaders import c15, r151, releves

# Contract changes (C15)
historique = c15().filter({"pdl": ["PDL123"]}).limit(100).collect()

# Periodic readings (R151)
releves_lf = r151().filter({"date_releve": ">= '2024-01-01'"}).lazy()

# Unified readings (R151 + R15)
tous_releves = releves().collect()
```

### 4. Odoo Query Builder

```python
from electricore.integrations.odoo import OdooReader
import polars as pl

with OdooReader(config) as odoo:
    # Navigate relations
    factures = (
        odoo.query('sale.order', domain=[('x_pdl', '!=', False)])
        .follow('invoice_ids', fields=['name', 'amount_total'])
        .filter(pl.col('amount_total') > 100)
        .collect()
    )

    # Enrich with related data
    orders = (
        odoo.query('sale.order')
        .enrich('partner_id', fields=['name', 'email'])
        .collect()
    )
```

### 5. API Server

```bash
# Start server
uv run uvicorn electricore.api.main:app --reload

# Test endpoints
curl http://localhost:8000/health
curl -H "X-API-Key: your_key" "http://localhost:8000/flux/c15?limit=10"

# Interactive docs
open http://localhost:8000/docs
```

## Testing

```bash
# Run all tests
uv run --group test pytest -q

# Run specific test file
uv run --group test pytest tests/unit/test_turpe.py -v

# Run with coverage
uv run --group test pytest --cov=electricore tests/
```

**Current status**: 671 tests passing, 35 skipped (légitimes) ✅ (juin 2026)

**Test coverage**:
- ✅ Périmètre/historique, abonnements, énergie, TURPE — tests unitaires complets
- ✅ Taxes — accise, CTA, taux en vigueur, schémas
- ✅ Facturation — expressions, rapport, rapprochement, contexte mensuel
- ✅ Bot — handlers par domaine, auth, client, livraison, surveillance, no-ERP
- ✅ API — endpoints flux/taxes/facturation/ingestion (intégration), sérialiseurs
- ✅ Ingestion — golden dbt générés depuis les XSD Enedis, crypto, xml→dict, modes du runner
- ✅ Architecture — pureté core (ADR-0016), imports par rôle (ADR-0019), topologies bot/builds
- ⚠️ Orchestration core — couverte indirectement (snapshots à régénérer)

## Documentation

- **Transmission**: [docs/transmission.md](docs/transmission.md) - Guide pour nouvel arrivant (Rust/Java → Python)
- **Main README**: [README.md](README.md) - Complete project overview
- **Ingestion Module**: [electricore/ingestion/README.md](electricore/ingestion/README.md)
- **Configuration (inventaire complet)**: [docs/configuration.md](docs/configuration.md) - Env vars, fichiers versionnés, deploy, outillage
- **Ingestion (architecture ELT dlt+dbt)**: [docs/ingestion.md](docs/ingestion.md) - Schéma + recettes (ajouter un champ, nouvelle version XSD Enedis, nouveau flux, pièges DuckDB)
- **API Module**: [electricore/api/README.md](electricore/api/README.md)
- **Bot Module**: [electricore/bot/README.md](electricore/bot/README.md) - Surface de commandes, alertes, no-ERP (ADR-0022)
- **DuckDB Query Builder**: [electricore/core/loaders/duckdb/](electricore/core/loaders/duckdb/) — docstrings des modules `query.py`, `sql.py`, `expressions.py`
- **Odoo Query Builder**: [docs/odoo-query-builder.md](docs/odoo-query-builder.md)
- **Date Conventions**: [docs/conventions-dates-enedis.md](docs/conventions-dates-enedis.md)

## Agent skills

### Issue tracker

Issues live as GitHub issues on `Energie-De-Nantes/electricore` (via the `gh` CLI). See [docs/agents/issue-tracker.md](docs/agents/issue-tracker.md).

### Triage labels

Canonical label names (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`) used as-is on GitHub. See [docs/agents/triage-labels.md](docs/agents/triage-labels.md).

### Domain docs

Multi-context layout: [CONTEXT-MAP.md](CONTEXT-MAP.md) at root pointing to per-module `CONTEXT.md` (core/ingestion/api/bot); ADRs are repo-wide in [docs/adr/](docs/adr/). See [docs/agents/domain.md](docs/agents/domain.md).

## GitHub auth & PR workflow

Inside the Claude Code sandbox the global `gh` keyring login is unreachable, so plain `gh`
commands — including the issue-tracker workflow above — fail with an authentication error.
This repo carries a **repo-scoped** fine-grained token at `.gh-token` (gitignored). Read it
at runtime, anchored to the repo root so it works from any subfolder:

```bash
GH_TOKEN=$(cat "$(git rev-parse --show-toplevel)/.gh-token") gh pr create --fill
GH_TOKEN=$(cat "$(git rev-parse --show-toplevel)/.gh-token") gh issue create ...
```

The token is scoped to **only this repo** (Contents / Pull requests / Issues; no
Administration, no CI secrets) — never copy it into `~/.config/gh`. Regenerate it with the
token-setup helper (run from inside this repo) if it expires.

**Branch & PR model** — push freely to feature branches; the default branch is protected by
a ruleset (PR required; force-push & deletion blocked):

- Never push to or merge the default branch directly.
- For any change: branch → commit → push → `gh pr create --fill` (with the token above), then stop.
- Merging the PR is a human step on the website.
