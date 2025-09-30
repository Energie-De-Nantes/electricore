# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ElectriCore is a French energy data processing engine that transforms raw Enedis (French electricity distributor) data into business-ready formats. Core business logic for LibreWatt and Odoo energy management modules.

## Key Commands

```bash
# Run tests
poetry run pytest -q

# Install dependencies  
poetry install

# Build/package
poetry build
```

## Architecture

### Current Structure
- **`electricore/core/pipelines_polars/`** - Modern Polars-based pipelines
- **`electricore/core/loaders/`** - Data loading (DuckDB, Polars, Odoo read)
- **`electricore/core/writers/`** - Data export (Odoo write)
- **`electricore/etl/`** - DLT-based ETL pipeline (SFTP → DuckDB)
- **`electricore/api/`** - FastAPI REST endpoints
- **`electricore/inputs/flux/`** - Enedis XML parsers (R15, R151, C15)

### Polars Migration Status
- ✅ **Pipeline périmètre**: Complete with 8 composable expressions + LazyFrame pipeline
- 🔄 **Pipeline relevés**: Next priority  
- 🔄 **Pipeline énergies**: Planned
- 🔄 **Pipeline taxes**: Planned

### Established Patterns
- **Expressions pures**: `Fn(Series) -> Series` - Composable transformations
- **LazyFrame pipelines**: `Fn(LazyFrame) -> LazyFrame` - Optimized execution
- **Validation**: Tests include pandas comparison for migration validation
- **Query Builders**: `DuckDBQuery` and `OdooQuery` - Immutable, fluent API for data queries

## Important Notes

- Business domain in French (périmètre, relevés, énergies, etc.)
- All dates use Europe/Paris timezone
- Pandera schemas for DataFrame validation with `@pa.check_types`
- Migration approach: Polars LazyFrames + functional expressions