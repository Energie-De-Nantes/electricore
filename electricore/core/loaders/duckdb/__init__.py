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
- c15(), r151(), r15(), f15(), r64() : Query builders par flux
- releves(), releves_harmonises() : Vues unifiées
- DuckDBQuery : Builder immutable avec méthodes chainables
- Utilitaires : get_available_tables(), execute_custom_query()
- API legacy : load_historique_perimetre(), load_releves()
"""

# Imports internes
from .config import DuckDBConfig, duckdb_connection
from .query import DuckDBQuery
from .helpers import (
    # API fluide
    c15,
    r151,
    r15,
    f15,
    r64,
    releves,
    releves_harmonises,
    # API legacy
    load_historique_perimetre,
    load_releves,
    # Utilitaires
    get_available_tables,
    execute_custom_query,
)


# =============================================================================
# EXPORTS PUBLICS
# =============================================================================

__all__ = [
    # Configuration
    "DuckDBConfig",
    "duckdb_connection",
    # Query builder
    "DuckDBQuery",
    # API fluide (recommandée)
    "c15",
    "r151",
    "r15",
    "f15",
    "r64",
    "releves",
    "releves_harmonises",
    # API legacy (compatibilité)
    "load_historique_perimetre",
    "load_releves",
    # Utilitaires
    "get_available_tables",
    "execute_custom_query",
]
