# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**ElectriCore** is a French energy data processing engine - an open-source tool to reclaim control over electricity grid data. It transforms raw Enedis (French electricity distributor) flux into structured, exploitable data for LibreWatt, Odoo ERP, and other energy management tools.

**Stack**: Polars + DuckDB for high-performance data processing. Pure Polars (no pandas dependency).

**Three main modules**:
- **ETL**: Automated extraction/transformation pipeline (DLT) - SFTP → DuckDB
- **Core**: Energy calculation pipelines (Polars LazyFrames)
- **API**: Secure REST API (FastAPI) with authentication

## Key Commands

```bash
# Install (uv)
uv sync              # core + API + marimo + viz
uv sync --extra etl  # + SFTP ETL pipeline

# Run tests
uv run --group test pytest -q

# Run with coverage
uv run --group test pytest --cov=electricore tests/

# Build/package
uv build

# Run ETL pipeline (requires [etl] extra)
uv run python electricore/etl/pipeline_production.py all

# Start API server
uv run uvicorn electricore.api.main:app --reload
```

## Architecture

### Module Structure

```
electricore/
├── etl/                       # 📥 ETL - Data Extraction & Transformation
│   ├── sources/               # DLT sources (SFTP Enedis)
│   ├── transformers/          # Modular transformers (crypto, archive, parsers)
│   ├── config/                # Flux configuration
│   └── pipeline_production.py # Production pipeline with modes
│
├── core/                      # 🧮 CORE - Energy Calculations
│   ├── pipelines/             # Polars pipelines
│   │   ├── perimetre.py       # PDL perimeter (contract events)
│   │   ├── abonnements.py     # Subscription periods
│   │   ├── energie.py         # Energy consumption by time slot
│   │   ├── turpe.py           # TURPE network tariff (fixed + variable)
│   │   ├── accise.py          # Accise (TICFE) tax calculation
│   │   ├── facturation.py     # Monthly billing aggregation (Pandera-validated)
│   │   └── orchestration.py   # Full pipeline orchestration
│   ├── models/                # Pandera validation schemas
│   ├── loaders/               # Data loading & query builders
│   │   ├── duckdb/            # DuckDBQuery builder (c15, r151, releves, etc.)
│   │   ├── odoo/              # OdooReader + OdooQuery
│   │   └── parquet.py         # Parquet loader
│   └── writers/               # Data export
│       └── odoo.py            # OdooWriter
│
├── api/                       # 🌐 API - REST Access Layer
│   ├── services/              # Query services (DuckDB)
│   ├── security.py            # API key authentication
│   └── main.py                # FastAPI application
│
└── config/                    # ⚙️ Tariff rules (TURPE, Accise CSV files)
```

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
SFTP Enedis → ETL (DLT) → DuckDB → Query Builders → Core Pipelines → Results
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

### ETL Modularity
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
- **Column naming**: Follows `grandeur_cadran_unité` format (see [docs/conventions-nommage.md](docs/conventions-nommage.md))

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
# Loaders (read operations)
from electricore.core.loaders import (
    c15, r151, r15, releves,           # DuckDB query builders
    OdooReader, OdooQuery,             # Odoo read + query
    charger_releves, charger_historique # Polars loaders
)

# Writers (write operations)
from electricore.core.writers import OdooWriter

# Pipelines individuels
from electricore.core.pipelines.perimetre import pipeline_perimetre
from electricore.core.pipelines.abonnements import pipeline_abonnements
from electricore.core.pipelines.energie import pipeline_energie
from electricore.core.pipelines.turpe import ajouter_turpe_fixe, ajouter_turpe_variable
from electricore.core.pipelines.accise import pipeline_accise
from electricore.core.pipelines.facturation import pipeline_facturation

# Pipeline complet (orchestration)
from electricore.core.pipelines.orchestration import facturation, calculer_historique_enrichi
```

### Naming Conventions
- Query builders: `DuckDBQuery` and `OdooQuery` (harmonized naming)
- Files: `duckdb/`, `odoo/` (sub-packages, no `_loader` suffix)

## Key Modules & Quick Examples

### 1. ETL Pipeline

```bash
# Test mode (2 files, ~3s)
uv run python electricore/etl/pipeline_production.py test

# Single flux (R151 complete, ~6s)
uv run python electricore/etl/pipeline_production.py r151

# Full production (all flux)
uv run python electricore/etl/pipeline_production.py all
```

Result: DuckDB database at `electricore/etl/flux_enedis_pipeline.duckdb`

#### Rotation des clés AES Enedis

Enedis effectue des rotations de clés AES périodiquement. Le format `secrets.toml` supporte
plusieurs clés simultanément pour couvrir la période de transition :

```toml
# Format recommandé (v2) — supporte la rotation
[aes.current]
key = "nouvelle_clé_hex"
iv  = "nouvel_iv_hex"

[aes.previous]           # optionnel, garder ~4 semaines après rotation
key = "ancienne_clé_hex"
iv  = "ancien_iv_hex"
```

Procédure de rotation :
1. Obtenir la nouvelle clé Enedis
2. Déplacer `[aes]` → `[aes.previous]`, créer `[aes.current]` avec la nouvelle clé
3. Relancer le pipeline — les anciens fichiers déchiffrent avec `previous`, les nouveaux avec `current`
4. Après ~4 semaines : supprimer `[aes.previous]`

Le format `[aes]` plat (v1) reste supporté pour la compatibilité ascendante.

### 2. Core Pipelines

```python
from electricore.core.pipelines.perimetre import pipeline_perimetre
from electricore.core.loaders import c15

# Load from DuckDB with query builder
historique_lf = c15().filter({"Date_Evenement": ">= '2024-01-01'"}).lazy()

# Run pipeline
perimetre_df = pipeline_perimetre(historique_lf).collect()
# → pdl, Date_Evenement, impacte_abonnement, impacte_energie, resume_modification
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
from electricore.core.loaders import OdooReader
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

**Current status**: 183 tests passing, 12 skipped (légitimes) ✅

**Test coverage**:
- ✅ Périmètre, abonnements, énergie, TURPE — tests unitaires complets
- ✅ Facturation — expressions testées
- ⚠️ Accise, orchestration — non couverts (à faire)
- ⚠️ API, ETL — non couverts

## Documentation

- **Transmission**: [docs/transmission.md](docs/transmission.md) - Guide pour nouvel arrivant (Rust/Java → Python)
- **Main README**: [README.md](README.md) - Complete project overview
- **ETL Module**: [electricore/etl/README.md](electricore/etl/README.md)
- **API Module**: [electricore/api/README.md](electricore/api/README.md)
- **DuckDB Integration**: [electricore/core/loaders/DUCKDB_INTEGRATION_GUIDE.md](electricore/core/loaders/DUCKDB_INTEGRATION_GUIDE.md)
- **Odoo Query Builder**: [docs/odoo-query-builder.md](docs/odoo-query-builder.md)
- **Date Conventions**: [docs/conventions-dates-enedis.md](docs/conventions-dates-enedis.md)
