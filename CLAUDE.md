# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ElectriCore is a French energy data processing engine that transforms raw electrical grid data into business-ready formats. It serves as the core business logic for energy management tools like LibreWatt and Odoo modules, processing Enedis (French electricity distributor) data.

## Key Commands

```bash
# Run tests
pytest tests/
# or using poetry
poetry run pytest -q

# Install dependencies
poetry install

# Build/package
poetry build
```

## Architecture

The codebase follows a clear modular architecture with French domain terminology:

- **`electricore/core/`** - Business logic modules:
  - `périmètre/` - Contract perimeter management (historical tracking)
  - `relevés/` - Meter readings processing
  - `énergies/` - Energy calculations and aggregations
  - `taxes/` - Tax calculations (TURPE - French grid tariffs)
  - `abonnements/` - Subscription management
  - `services.py` - Main orchestration functions (`facturation()`)

- **`electricore/inputs/`** - Data connectors:
  - `flux/` - Enedis XML flux parsers (R15, R151, C15 formats)

- **`electricore/outputs/`** - Export interfaces (to be implemented)

## Data Flow

The main processing pipeline follows this sequence:
1. **Flux parsing** (C15 → HistoriquePérimètre, R151 → RelevéIndex)
2. **Base preparation** (combining perimeter history with date ranges)
3. **Reading integration** (adding meter readings to base)
4. **Energy calculation** (computing consumption/production)
5. **Tax calculation** (applying TURPE rules)

Key function: `facturation(deb, fin, historique, relevés)` in `services.py`

## Data Validation

Uses Pandera for DataFrame schema validation. All core data models are defined with type annotations and validation rules. Function signatures use `@pa.check_types` decorator for runtime validation.

## Important Notes

- All dates use Europe/Paris timezone (`pd.DatetimeTZDtype`)
- Business domain is in French (périmètre, relevés, énergies, etc.)
- Handles complex cases like "Modifications Contractuelles Impactantes" (MCT)
- Main data processing uses pandas DataFrames with strict typing
- Test command available via `[test]` section in pyproject.toml