"""
Chargeurs de données pour ElectriCore.

Modules de chargement et validation des données depuis différentes sources.
"""

from .parquet import charger_releves, charger_historique
from .duckdb import (
    # API fluide (nouvelles fonctions recommandées)
    c15,
    r151,
    r15,
    f15,
    r64,
    releves,
    releves_harmonises,
    DuckDBQuery,
    # API legacy (compatibilité)
    load_historique_perimetre,
    load_releves,
    # Utilitaires
    get_available_tables,
    execute_custom_query,
    DuckDBConfig
)
from .odoo import (
    OdooReader,
    OdooQuery,
    OdooConfig,
    # API fonctionnelle - Helpers simples
    query,
    factures,
    lignes_factures,
    commandes,
    partenaires,
    # API fonctionnelle - Helpers avec navigation
    commandes_factures,
    commandes_lignes,
    # Expressions Polars utilitaires
    expr_calculer_trimestre_facturation,
)

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
    "load_historique_perimetre",
    "load_releves",
    # Utilitaires DuckDB
    "get_available_tables",
    "execute_custom_query",
    "DuckDBConfig",
    # Connecteur Odoo
    "OdooReader",
    "OdooQuery",
    "OdooConfig",
    # API fonctionnelle Odoo - Helpers simples
    "query",
    "factures",
    "lignes_factures",
    "commandes",
    "partenaires",
    # API fonctionnelle Odoo - Helpers avec navigation
    "commandes_factures",
    "commandes_lignes",
    # Expressions Polars utilitaires Odoo
    "expr_calculer_trimestre_facturation",
]