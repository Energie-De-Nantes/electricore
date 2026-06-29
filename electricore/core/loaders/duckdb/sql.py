"""
Génération SQL fonctionnelle pour requêtes DuckDB.

Ce module fournit `build_base_query` : `SELECT * FROM {table}` + WHERE optionnel.
La machinerie de projection colonne par colonne (`Column`, `build_select_clause`) a
été retirée (#506) — depuis la bascule ADR-0042, tous les flux utilisent `SELECT *`,
la forme résiduelle vivant dans les modèles dbt.

Le fuseau horaire est posé une fois à la connexion (`config.duckdb_readonly_conn`, #393).
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .descriptor import FluxDescriptor

# Heure légale française : le fuseau de TOUS les flux Enedis (cf.
# docs/conventions-dates-enedis.md), invariant de domaine uniforme (ADR-0042). Constante
# nommée plutôt que littéral épars : le fuseau de session de la connexion read-only la pose
# une fois à la lecture (`duckdb_readonly_conn`, #393), rendant les instants déterministes —
# l'ancien ancrage/filtre par colonne (FormeTemporelle, #390/#391) a été retiré (#398).
HEURE_LEGALE = "Europe/Paris"


def build_base_query(descriptor: "FluxDescriptor") -> str:
    """Construit `SELECT * FROM {table}` + WHERE optionnel (fonction pure).

    Args:
        descriptor: Descripteur de flux (table + WHERE optionnel).

    Returns:
        Requête SQL complète.
    """
    parts = ["SELECT *", f"FROM {descriptor.table}"]
    if descriptor.where_clause:
        parts.append(f"WHERE {descriptor.where_clause}")
    return "\n".join(parts)
