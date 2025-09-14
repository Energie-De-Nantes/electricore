"""
Chargeurs de données pour ElectriCore.

Modules de chargement et validation des données depuis différentes sources.
"""

from .polars_loader import charger_releves, charger_historique
from .duckdb_loader import (
    # API fluide (nouvelles fonctions recommandées)
    c15,
    r151,
    r15,
    releves,
    QueryBuilder,
    # API legacy (compatibilité)
    load_historique_perimetre,
    load_releves,
    # Utilitaires
    get_available_tables,
    execute_custom_query,
    DuckDBConfig
)

__all__ = [
    # Loaders Parquet existants
    "charger_releves",
    "charger_historique",
    # API fluide DuckDB (recommandée)
    "c15",
    "r151",
    "r15",
    "releves",
    "QueryBuilder",
    # API legacy DuckDB (compatibilité)
    "load_historique_perimetre",
    "load_releves",
    # Utilitaires DuckDB
    "get_available_tables",
    "execute_custom_query",
    "DuckDBConfig"
]