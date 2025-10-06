# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**ElectriCore** is a French energy data processing engine - an open-source tool to reclaim control over electricity grid data. It transforms raw Enedis (French electricity distributor) flux into structured, exploitable data for LibreWatt, Odoo ERP, and other energy management tools.

**Modern stack**: Polars + DuckDB architecture for high-performance data processing with 100% Polars migration complete.

**Three main modules**:
- **ETL**: Automated extraction/transformation pipeline (DLT) - SFTP → DuckDB
- **Core**: Energy calculation pipelines (Polars LazyFrames)
- **API**: Secure REST API (FastAPI) with authentication

## Key Commands

```bash
# Run tests (138 passing)
poetry run pytest -q

# Install dependencies
poetry install

# Build/package
poetry build

# Run ETL pipeline
poetry run python electricore/etl/pipeline_production.py all

# Start API server
poetry run uvicorn electricore.api.main:app --reload
```

## Architecture

### Module Structure

```
electricore/
├── etl/                       # 📥 ETL - Data Extraction & Transformation
│   ├── sources/               # DLT sources (SFTP Enedis)
│   ├── transformers/          # Modular transformers (crypto, archive, parsers)
│   ├── config/                # Flux configuration (flux.yaml)
│   └── pipeline_production.py # Production pipeline with modes
│
├── core/                      # 🧮 CORE - Energy Calculations
│   ├── pipelines/             # Polars pipelines (périmètre, abonnements, energie, turpe)
│   ├── pipelines_polars/      # Migration-in-progress Polars implementations
│   ├── models/                # Pandera validation schemas
│   ├── loaders/               # Data loading & query builders
│   │   ├── duckdb.py          # DuckDBQuery builder (c15, r151, releves, etc.)
│   │   ├── polars.py          # Polars data loaders
│   │   └── odoo.py            # OdooReader + OdooQuery
│   └── writers/               # Data export
│       └── odoo.py            # OdooWriter
│
├── api/                       # 🌐 API - REST Access Layer
│   ├── services/              # Query services (DuckDB)
│   ├── auth.py                # API key authentication
│   └── main.py                # FastAPI application
│
└── inputs/flux/               # XML/CSV parsers (R15, R151, C15)
```

### Supported Flux Types

| Flux   | Description                | Tables                   |
|--------|----------------------------|--------------------------|
| **C15** | Contract changes          | `flux_c15`               |
| **F12** | Distributor invoicing     | `flux_f12`               |
| **F15** | Detailed invoices         | `flux_f15_detail`        |
| **R15** | Meter readings + events   | `flux_r15`, `flux_r15_acc` |
| **R151**| Periodic readings         | `flux_r151`              |
| **R64** | JSON timeseries readings  | `flux_r64`               |

### Data Flow

```
SFTP Enedis → ETL (DLT) → DuckDB → Query Builders → Core Pipelines → Results
                                  ↓
                                 API (FastAPI) → Clients
                                  ↓
                              Odoo (XML-RPC)
```

## Polars Migration Status

✅ **100% COMPLETE** - Full Polars migration achieved!

- ✅ **Pipeline périmètre**: 8 composable expressions + LazyFrame pipeline
- ✅ **Pipeline abonnements**: Period calculation with temporal bounds
- ✅ **Pipeline énergies**: Consumption by time slots (HP/HC/Base)
- ✅ **Pipeline TURPE**: Fixed and variable regulatory taxes

All pipelines now use pure Polars (no pandas dependency) with LazyFrame optimization.

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
- **Pandera schemas**: `@pa.check_types` decorators on pipeline functions
- **Tests**: Include pandas comparison for migration validation

## Important Notes

### Domain & Conventions
- **Language**: Business domain in French (périmètre, relevés, énergies, abonnements, TURPE)
- **Timezone**: All dates use `Europe/Paris` timezone
- **Date convention**: R151 flux uses +1 day adjustment to harmonize with R64/R15/C15 (see [docs/conventions-dates-enedis.md](docs/conventions-dates-enedis.md))

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

# Pipelines
from electricore.core.pipelines.perimetre import pipeline_perimetre
from electricore.core.pipelines.abonnements import pipeline_abonnements
from electricore.core.pipelines.energie import pipeline_energie
```

### Naming Conventions
- Query builders: `DuckDBQuery` and `OdooQuery` (harmonized naming)
- Files: `duckdb.py`, `polars.py`, `odoo.py` (no `_loader` suffix)

## Key Modules & Quick Examples

### 1. ETL Pipeline

```bash
# Test mode (2 files, ~3s)
poetry run python electricore/etl/pipeline_production.py test

# Single flux (R151 complete, ~6s)
poetry run python electricore/etl/pipeline_production.py r151

# Full production (all flux)
poetry run python electricore/etl/pipeline_production.py all
```

Result: DuckDB database at `electricore/etl/flux_enedis_pipeline.duckdb`

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
relevés = r151().filter({"date_releve": ">= '2024-01-01'"}).lazy()

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
poetry run uvicorn electricore.api.main:app --reload

# Test endpoints
curl http://localhost:8000/health
curl -H "X-API-Key: your_key" "http://localhost:8000/flux/c15?limit=10"

# Interactive docs
open http://localhost:8000/docs
```

## Testing

```bash
# Run all tests
poetry run pytest -q

# Run specific test file
poetry run pytest tests/core/pipelines/test_perimetre.py -v

# Run with coverage
poetry run pytest --cov=electricore tests/
```

**Current status**: 138 tests passing ✅

**Test coverage**: All Polars pipelines have comprehensive tests including:
- Pure function expressions
- LazyFrame transformations
- Pandera schema validation
- Edge cases and data quality

## Documentation

- **Main README**: [README.md](README.md) - Complete project overview
- **ETL Module**: [electricore/etl/README.md](electricore/etl/README.md)
- **API Module**: [electricore/api/README.md](electricore/api/README.md)
- **DuckDB Integration**: [electricore/core/loaders/DUCKDB_INTEGRATION_GUIDE.md](electricore/core/loaders/DUCKDB_INTEGRATION_GUIDE.md)
- **Odoo Query Builder**: [docs/odoo-query-builder.md](docs/odoo-query-builder.md)
- **Date Conventions**: [docs/conventions-dates-enedis.md](docs/conventions-dates-enedis.md)