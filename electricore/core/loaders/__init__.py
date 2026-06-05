"""Chargeurs de données pour ElectriCore (DuckDB, Parquet).

Module ERP-agnostique conformément à [ADR-0016](../../../docs/adr/0016-core-erp-agnostique.md) :
toute intégration ERP (Odoo, …) vit dans `electricore.integrations.<erp>`.
"""

from .duckdb import (
    DuckDBConfig,
    DuckDBQuery,
    # API fluide (nouvelles fonctions recommandées)
    c15,
    execute_custom_query,
    f15,
    # Utilitaires
    get_available_tables,
    # API legacy (compatibilité)
    load_historique,
    load_releves,
    r15,
    r64,
    r151,
    releves,
    releves_harmonises,
)
from .parquet import charger_historique, charger_releves

__all__ = [
    # Loaders Parquet existants
    "charger_releves",
    "charger_historique",
    # API fluide DuckDB (recommandée)
    "c15",
    "r151",
    "r15",
    "f15",
    "r64",
    "releves",
    "releves_harmonises",
    "DuckDBQuery",
    # API legacy DuckDB (compatibilité)
    "load_historique",
    "load_releves",
    # Utilitaires DuckDB
    "get_available_tables",
    "execute_custom_query",
    "DuckDBConfig",
]
