"""Chargeurs de données pour ElectriCore (DuckDB).

Module ERP-agnostique conformément à [ADR-0016](../../../docs/adr/0016-core-erp-agnostique.md) :
toute intégration ERP (Odoo, …) vit dans `electricore.integrations.<erp>`.
"""

from .duckdb import (
    DuckDBConfig,
    DuckDBQuery,
    # API fluide
    affaires,
    c15,
    chronologie_releves,
    execute_custom_query,
    f15,
    # Utilitaires
    get_available_tables,
    r15,
    r64,
    r151,
    releves,
    spine_contrat,
)

__all__ = [
    # API fluide DuckDB (recommandée)
    "c15",
    "r151",
    "r15",
    "f15",
    "r64",
    "releves",
    "spine_contrat",
    "chronologie_releves",
    "affaires",
    "DuckDBQuery",
    # Utilitaires DuckDB
    "get_available_tables",
    "execute_custom_query",
    "DuckDBConfig",
]
