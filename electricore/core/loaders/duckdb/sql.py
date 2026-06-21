"""
Génération SQL fonctionnelle pour requêtes DuckDB.

Ce module fournit les primitives pour construire des requêtes SQL de manière
fonctionnelle et type-safe avec dataclasses immutables : la colonne (`Column`),
les helpers de construction (`col_*`) et le bâtisseur de requête
(`build_base_query`).

Les *descripteurs* de flux (qui apparient ces colonnes à un transform et un
validateur) vivent dans `registry.py` ; ce module reste de la pure génération SQL.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .descriptor import FluxDescriptor

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

    # Clause SELECT
    select = build_select_clause(descriptor.columns)
    parts.append(f"SELECT\n    {select}")

    # Clause FROM
    parts.append(f"FROM {descriptor.table}")

    # Clause WHERE optionnelle
    if descriptor.where_clause:
        parts.append(f"WHERE {descriptor.where_clause}")

    return "\n".join(parts)


# =============================================================================
# HELPERS POUR CONSTRUIRE DES COLONNES COMMUNES
# =============================================================================


def col_simple(name: str) -> Column:
    """Colonne simple sans transformation."""
    return Column(name=name, sql_expr=name)


def col_cast_timestamp(name: str) -> Column:
    """Colonne castée en TIMESTAMP."""
    return Column(name=name, sql_expr=f"CAST({name} AS TIMESTAMP)")


def col_paris(name: str) -> Column:
    """Colonne temporelle naïve (DATE/TIMESTAMP sans fuseau) ancrée en Europe/Paris.

    Les sources Enedis sans offset sont des heures-mur Paris : l'ancrage explicite
    (timezone(...)) rend l'instant correct quel que soit le fuseau de session DuckDB
    (poste local, CI, VPS). Les colonnes déjà TIMESTAMPTZ n'en ont pas besoin.
    """
    return Column(name=name, sql_expr=f"timezone('Europe/Paris', CAST({name} AS TIMESTAMP))")


def col_cast_null_varchar(alias: str) -> Column:
    """Colonne NULL castée en VARCHAR avec alias."""
    return Column(name=alias, sql_expr="CAST(NULL AS VARCHAR)", alias=alias)


def col_literal(value: str, alias: str) -> Column:
    """Colonne littérale avec alias."""
    return Column(name=alias, sql_expr=f"'{value}'", alias=alias)


def col_literal_bool(value: bool, alias: str) -> Column:
    """Colonne booléenne littérale."""
    sql_value = "TRUE" if value else "FALSE"
    return Column(name=alias, sql_expr=sql_value, alias=alias)
