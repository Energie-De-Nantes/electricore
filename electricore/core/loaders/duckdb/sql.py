"""
Génération SQL fonctionnelle pour requêtes DuckDB.

Ce module fournit les primitives pour construire des requêtes SQL de manière
fonctionnelle et type-safe avec dataclasses immutables : la colonne (`Column`) et le
bâtisseur de requête (`build_base_query`, qui émet un `SELECT *` quand le descripteur
n'énumère pas ses colonnes — cas par défaut depuis ADR-0042).

Les *descripteurs* de flux (qui apparient table + transform + validateur) vivent dans
`registry.py` ; ce module reste de la pure génération SQL. Le fuseau horaire est posé une
fois à la connexion (`config.duckdb_readonly_conn`, #393), plus colonne par colonne.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .descriptor import FluxDescriptor

# Heure légale française : le fuseau de TOUS les flux Enedis (cf.
# docs/conventions-dates-enedis.md), invariant de domaine uniforme (ADR-0042). Constante
# nommée plutôt que littéral épars : le fuseau de session de la connexion read-only la pose
# une fois à la lecture (`duckdb_readonly_conn`, #393), rendant les instants déterministes —
# l'ancien ancrage/filtre par colonne (FormeTemporelle, #390/#391) a été retiré (#398).
HEURE_LEGALE = "Europe/Paris"


# =============================================================================
# DATACLASSES IMMUTABLES POUR DÉFINITION SQL
# =============================================================================


@dataclass(frozen=True, slots=True)
class Column:
    """
    Définition immutable d'une colonne SQL.

    Attributes:
        name: Nom de la colonne dans le résultat
        sql_expr: Expression SQL pour cette colonne
        alias: Alias optionnel (si différent du name)
    """

    name: str
    sql_expr: str
    alias: str | None = None

    def to_sql(self) -> str:
        """Convertit en fragment SQL SELECT."""
        # Si l'expression SQL est différente du nom, ajouter l'alias
        if self.sql_expr != self.name:
            alias_name = self.alias if self.alias else self.name
            return f"{self.sql_expr} as {alias_name}"
        # Sinon, utiliser directement le nom (colonne simple)
        return self.sql_expr


# =============================================================================
# FONCTIONS PURES DE GÉNÉRATION SQL
# =============================================================================


def build_select_clause(columns: tuple[Column, ...]) -> str:
    """
    Construit la clause SELECT depuis une liste de colonnes.

    Fonction pure : Fn(tuple[Column]) -> str

    Args:
        columns: Tuple de définitions de colonnes

    Returns:
        Clause SELECT formatée
    """
    return ",\n    ".join(col.to_sql() for col in columns)


def build_base_query(descriptor: "FluxDescriptor") -> str:
    """
    Construit la requête SQL complète depuis un descripteur de flux.

    Fonction pure : Fn(FluxDescriptor) -> str

    Args:
        descriptor: Descripteur de flux (table + colonnes + WHERE/commentaires)

    Returns:
        Requête SQL complète formatée
    """
    # Construire les parties de la requête
    parts = []

    # Commentaires optionnels
    if descriptor.comments:
        parts.append(descriptor.comments)

    # Clause SELECT — un descripteur SANS colonnes énumérées est un `SELECT *` pur
    # (ADR-0042) : la forme résiduelle vit dans le modèle dbt `flux_*`, le loader ne
    # re-projette ni ne re-type plus. Sinon, projection colonne par colonne (legacy).
    if descriptor.columns:
        select = build_select_clause(descriptor.columns)
        parts.append(f"SELECT\n    {select}")
    else:
        parts.append("SELECT *")

    # Clause FROM
    parts.append(f"FROM {descriptor.table}")

    # Clause WHERE optionnelle
    if descriptor.where_clause:
        parts.append(f"WHERE {descriptor.where_clause}")

    return "\n".join(parts)
