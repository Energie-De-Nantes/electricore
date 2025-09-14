"""
Chargeurs de données pour ElectriCore.

Modules de chargement et validation des données depuis différentes sources.
"""

from .polars_loader import charger_releves, charger_historique
from .duckdb_loader import (
    load_historique_perimetre,
    load_releves,
    get_available_tables,
    execute_custom_query,
    DuckDBConfig
)

__all__ = [
    # Loaders Parquet existants
    "charger_releves",
    "charger_historique",
    # Nouveaux loaders DuckDB
    "load_historique_perimetre",
    "load_releves",
    "get_available_tables",
    "execute_custom_query",
    "DuckDBConfig"
]