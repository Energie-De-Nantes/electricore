"""Chargeurs de données pour ElectriCore (DuckDB).

Module ERP-agnostique conformément à [ADR-0016](../../../docs/adr/0016-core-erp-agnostique.md) :
toute intégration ERP (Odoo, …) vit dans `electricore.integrations.<erp>`.
"""

from .duckdb import (
    DuckDBConfig,
    DuckDBQuery,
    # API fluide
    affaires,
    c12,
    c15,
    chronologie_releves,
    f15,
    r15,
    r64,
    r67,
    r151,
    releves,
    spine_contrat,
)

__all__ = [
    # API fluide DuckDB (recommandée)
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
    "DuckDBQuery",
    "DuckDBConfig",
]
