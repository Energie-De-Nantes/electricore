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
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .descriptor import FluxDescriptor

# Heure légale française : le fuseau de TOUS les flux Enedis (cf.
# docs/conventions-dates-enedis.md), invariant de domaine uniforme (ADR-0042).
# Constante nommée plutôt que littéral épars — la partagent : le fuseau de session de
# la connexion read-only (`duckdb_readonly_conn`, #393), l'ancrage de lecture (forme
# `NAIF_PARIS`) et le filtre déterministe (#391). Le mécanisme par colonne est
# transitoire : ADR-0042 le retire (#398) au profit du seul fuseau de session.
HEURE_LEGALE = "Europe/Paris"


class FormeTemporelle(StrEnum):
    """Forme temporelle d'une colonne, qui pilote l'ancrage de fuseau à la lecture.

    Tous les flux Enedis sont en heure légale française, sous deux formes de donnée
    (cf. docs/conventions-dates-enedis.md) :

    - `OFFSET` : instant à offset explicite (xsd:dateTime, ex. `+02:00`) stocké en
      TIMESTAMPTZ — l'instant est absolu, porté par la donnée. À la lecture, seul
      l'affichage est ramené en Europe/Paris ; au filtre, le littéral est interprété
      en Europe/Paris (mirror du cast, #391).
    - `NAIF_PARIS` : horodate naïve (TIMESTAMP sans fuseau, ou DATE) = heure-mur
      Paris. Ancrée en SQL (`timezone(...)`) → instant correct quel que soit le
      fuseau de session, puis affichée Europe/Paris.
    - `JOUR` : jour nu (xs:date) sans instant — relève de la convention *jour*, pas
      d'ancrage de fuseau (la colonne reste une Date).
    """

    OFFSET = "offset"
    NAIF_PARIS = "naif_paris"
    JOUR = "jour"


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
        forme: Forme temporelle (pour les colonnes date/timestamp). Pilote
            l'ancrage de fuseau à la lecture (et le filtre déterministe, #391).
            `None` pour une colonne non temporelle.
    """

    name: str
    sql_expr: str
    alias: str | None = None
    forme: FormeTemporelle | None = None

    def to_sql(self) -> str:
        """Convertit en fragment SQL SELECT (la forme tz dérive l'ancrage SQL)."""
        # Forme naïve = heure-mur Paris : ancrage explicite (timezone(...)) → instant
        # correct quel que soit le fuseau de session DuckDB (poste local, CI, VPS).
        if self.forme is FormeTemporelle.NAIF_PARIS:
            return f"timezone('{HEURE_LEGALE}', CAST({self.name} AS TIMESTAMP)) as {self.name}"
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
    """Colonne simple sans transformation (non temporelle)."""
    return Column(name=name, sql_expr=name)


def col_offset(name: str) -> Column:
    """Colonne temporelle à offset explicite (TIMESTAMPTZ) — instant absolu.

    L'offset est porté par la donnée : aucun ancrage SQL. La lecture ramène
    seulement l'affichage en Europe/Paris ; le filtre interprète son littéral en
    Europe/Paris (forme `OFFSET`, mirror du cast, #391).
    """
    return Column(name=name, sql_expr=name, forme=FormeTemporelle.OFFSET)


def col_paris(name: str) -> Column:
    """Colonne temporelle naïve (DATE/TIMESTAMP sans fuseau) ancrée en Europe/Paris.

    Les sources Enedis sans offset sont des heures-mur Paris : l'ancrage explicite
    (timezone(...)), dérivé de la forme `NAIF_PARIS`, rend l'instant correct quel que
    soit le fuseau de session DuckDB (poste local, CI, VPS).
    """
    return Column(name=name, sql_expr=name, forme=FormeTemporelle.NAIF_PARIS)


def col_jour(name: str) -> Column:
    """Colonne jour nu (xs:date) — pas d'instant, pas d'ancrage de fuseau."""
    return Column(name=name, sql_expr=name, forme=FormeTemporelle.JOUR)


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
