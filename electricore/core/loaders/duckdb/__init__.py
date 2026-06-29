"""
Chargeur DuckDB pour pipelines ElectriCore - Architecture fonctionnelle.

Ce module fournit une API fluide et fonctionnelle pour charger les données
depuis DuckDB vers Polars avec validation Pandera.

Architecture :
- config.py : Configuration et connexions (immutable)
- query.py : Query builder immutable avec lazy evaluation
- sql.py : Génération SQL fonctionnelle (build_base_query = SELECT *)
- descriptor.py : FluxDescriptor — descripteur unifié d'un flux/mart (#389)
- registry.py : Registre unique des descripteurs de flux
- helpers.py : Fonctions factory

API publique :
- c15(), r151(), r15(), f15(), r64() : Query builders par flux individuel
- releves() : modèle de relevés canonique dbt (C15 + R64 + R151, ADR-0029)
- DuckDBQuery : Builder immutable avec méthodes chainables
"""

# Imports internes
from .config import DuckDBConfig, DuckDBLockError, duckdb_readonly_conn
from .helpers import (
    # API fluide
    affaires,
    c12,
    c15,
    chronologie_releves,
    f15,
    flux,
    r15,
    r64,
    r67,
    r151,
    releves,
    spine_contrat,
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
    "c12",
    "c15",
    "r151",
    "r15",
    "f15",
    "r64",
    "r67",
    "releves",
    "spine_contrat",
    "chronologie_releves",
    "affaires",
    "FluxInconnu",
    # Groupings C15 canoniques (cf. CONTEXT.md)
    "ENTREES_C15",
    "SORTIES_C15",
]
