"""
Chargeurs de données pour ElectriCore.

Modules de chargement et validation des données depuis différentes sources.
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
    load_historique_perimetre,
    load_releves,
    r15,
    r64,
    r151,
    releves,
    releves_harmonises,
)
from .odoo import (
    OdooConfig,
    OdooQuery,
    OdooReader,
    commandes,
    # API fonctionnelle - Helpers avec navigation
    commandes_factures,
    commandes_lignes,
    consommations_mensuelles,
    # Expressions Polars utilitaires
    expr_calculer_trimestre_facturation,
    factures,
    lignes_a_facturer,
    lignes_factures,
    partenaires,
    # API fonctionnelle - Helpers simples
    query,
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
    "consommations_mensuelles",
    "lignes_a_facturer",
    # Expressions Polars utilitaires Odoo
    "expr_calculer_trimestre_facturation",
]