"""
Chargeur DuckDB pour pipelines ElectriCore - Architecture fonctionnelle.

Ce module fournit une API fluide et fonctionnelle pour charger les données
depuis DuckDB vers Polars avec validation Pandera.

Architecture :
- config.py : Configuration et connexions (immutable)
- query.py : Query builder immutable avec lazy evaluation
- sql.py : Génération SQL fonctionnelle avec dataclasses
- transforms.py : Transformations composables LazyFrame
- expressions.py : Expressions Polars pures et réutilisables
- registry.py : Registre des configurations de flux
- helpers.py : Fonctions factory et utilitaires

API publique :
- c15(), r151(), r15(), f15(), r64() : Query builders par flux individuel
- releves() : modèle de relevés canonique dbt (C15 + R64 + R151, ADR-0029)
- DuckDBQuery : Builder immutable avec méthodes chainables
- Utilitaires : get_available_tables(), execute_custom_query()
"""

# Imports internes
from .config import DuckDBConfig, DuckDBLockError, duckdb_readonly_conn
from .helpers import (
    # API fluide
    affaires,
    c15,
    chronologie,
    execute_custom_query,
    f15,
    flux,
    # Utilitaires
    get_available_tables,
    r15,
    r64,
    r151,
    releves,
    spine,
)
from .query import DuckDBQuery
from .registry import ENTREES_C15, SORTIES_C15, FluxInconnu

# =============================================================================
# EXPORTS PUBLICS
# =============================================================================

__all__ = [
    # Configuration
    "DuckDBConfig",
    "DuckDBLockError",
    "duckdb_readonly_conn",
    # Query builder
    "DuckDBQuery",
    # API fluide (recommandée)
    "flux",
    "c15",
    "r151",
    "r15",
    "f15",
    "r64",
    "releves",
    "spine",
    "chronologie",
    "affaires",
    "FluxInconnu",
    # Groupings C15 canoniques (cf. CONTEXT.md)
    "ENTREES_C15",
    "SORTIES_C15",
    # Utilitaires
    "get_available_tables",
    "execute_custom_query",
]
