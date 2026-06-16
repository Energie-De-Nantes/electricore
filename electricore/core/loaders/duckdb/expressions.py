"""
Expressions Polars réutilisables pour transformations DuckDB.

Ce module contient des expressions fonctionnelles pures suivant la philosophie
Polars. Chaque expression est une fonction pure : Fn(params) -> pl.Expr.

Les expressions sont composables et peuvent être utilisées dans with_columns()
pour construire des pipelines de transformation complexes.
"""

import polars as pl

from electricore.core.models.cadrans import CADRANS, col_index

# =============================================================================
# CONSTANTES IMMUTABLES
# =============================================================================

# Colonnes d'index énergétiques (tuple = immutable) - Format: index_cadran_kwh
INDEX_COLS = tuple(col_index(c) for c in CADRANS)

# Colonnes de dates pour chaque type de flux
DATE_COLS_HISTORIQUE = ("date_evenement", "avant_date_releve", "apres_date_releve")
DATE_COLS_RELEVES = ("date_releve",)
DATE_COLS_FACTURES = ("date_facture", "date_debut", "date_fin")
DATE_COLS_R64 = ("date_releve", "modification_date")


# =============================================================================
# EXPRESSIONS POUR CONVERSION TIMEZONE
# =============================================================================


def expr_with_timezone(date_col: str, tz: str = "Europe/Paris") -> pl.Expr:
    """
    Expression pour conversion d'une colonne date vers un timezone spécifique.

    Fonction pure : Fn(str, str) -> pl.Expr

    Args:
        date_col: Nom de la colonne date à convertir
        tz: Timezone cible (défaut: Europe/Paris)

    Returns:
        Expression Polars pour la conversion timezone

    Example:
        >>> df.with_columns(expr_with_timezone("date_evenement"))
    """
    return pl.col(date_col).dt.convert_time_zone(tz)


def expr_dates_with_timezone(*date_cols: str, tz: str = "Europe/Paris") -> list[pl.Expr]:
    """
    Expressions multiples pour conversion de plusieurs colonnes dates.

    Fonction pure : Fn(*str, str) -> List[pl.Expr]

    Args:
        *date_cols: Noms des colonnes dates à convertir
        tz: Timezone cible (défaut: Europe/Paris)

    Returns:
        Liste d'expressions Polars pour les conversions

    Example:
        >>> exprs = expr_dates_with_timezone("date_debut", "date_fin")
        >>> df.with_columns(exprs)
    """
    return [expr_with_timezone(col, tz) for col in date_cols]
