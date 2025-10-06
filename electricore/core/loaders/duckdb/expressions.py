"""
Expressions Polars réutilisables pour transformations DuckDB.

Ce module contient des expressions fonctionnelles pures suivant la philosophie
Polars. Chaque expression est une fonction pure : Fn(params) -> pl.Expr.

Les expressions sont composables et peuvent être utilisées dans with_columns()
pour construire des pipelines de transformation complexes.
"""

import polars as pl
from typing import List


# =============================================================================
# CONSTANTES IMMUTABLES
# =============================================================================

# Colonnes d'index énergétiques (tuple = immutable)
INDEX_COLS = ("base", "hp", "hc", "hph", "hpb", "hcb", "hch")

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


def expr_dates_with_timezone(
    *date_cols: str,
    tz: str = "Europe/Paris"
) -> List[pl.Expr]:
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


# =============================================================================
# EXPRESSIONS POUR CONVERSION UNITÉS (Wh -> kWh)
# =============================================================================

def expr_wh_to_kwh(index_col: str) -> pl.Expr:
    """
    Expression pour conversion Wh -> kWh avec troncature (floor).

    Fonction pure : Fn(str) -> pl.Expr

    La conversion applique une troncature (floor) pour ne compter que
    les kWh complets, conformément aux règles métier Enedis.

    Convention Enedis :
    - Si unite = "Wh" : division par 1000 puis floor()
    - Si unite = "kWh" : valeur inchangée
    - NULL reste NULL

    Args:
        index_col: Nom de la colonne d'index à convertir

    Returns:
        Expression Polars avec conversion conditionnelle

    Example:
        >>> df.with_columns(expr_wh_to_kwh("hp"))
    """
    return (
        pl.when(pl.col("unite") == "Wh")
        .then(
            pl.when(pl.col(index_col).is_not_null())
            .then((pl.col(index_col) / 1000).floor())
            .otherwise(pl.col(index_col))
        )
        .otherwise(pl.col(index_col))
        .alias(index_col)
    )


def expr_wh_to_kwh_multi(*index_cols: str) -> List[pl.Expr]:
    """
    Expressions multiples pour conversion Wh -> kWh de plusieurs colonnes.

    Fonction pure : Fn(*str) -> List[pl.Expr]

    Args:
        *index_cols: Noms des colonnes d'index à convertir

    Returns:
        Liste d'expressions Polars pour les conversions

    Example:
        >>> exprs = expr_wh_to_kwh_multi("hp", "hc", "base")
        >>> df.with_columns(exprs)
    """
    return [expr_wh_to_kwh(col) for col in index_cols]


def expr_normalize_unit(unit_col: str = "unite") -> pl.Expr:
    """
    Expression pour normaliser l'unité après conversion Wh -> kWh.

    Fonction pure : Fn(str) -> pl.Expr

    Args:
        unit_col: Nom de la colonne unité (défaut: "unite")

    Returns:
        Expression Polars avec normalisation

    Example:
        >>> df.with_columns(expr_normalize_unit("unite"))
    """
    return (
        pl.when(pl.col(unit_col) == "Wh")
        .then(pl.lit("kWh"))
        .otherwise(pl.col(unit_col))
        .alias(unit_col)
    )


# =============================================================================
# EXPRESSIONS POUR MÉTADONNÉES DÉRIVÉES
# =============================================================================

def expr_has_metadata(flux_origin_col: str = "flux_origine") -> pl.Expr:
    """
    Expression pour détecter la présence de métadonnées selon le flux.

    Fonction pure : Fn(str) -> pl.Expr

    Logique :
    - R64 : métadonnées présentes si type_releve non NULL
    - Autres flux : métadonnées présentes si id_calendrier_distributeur non NULL

    Args:
        flux_origin_col: Nom de la colonne flux d'origine

    Returns:
        Expression Polars booléenne

    Example:
        >>> df.with_columns(expr_has_metadata())
    """
    return (
        pl.when(pl.col(flux_origin_col) == "R64")
        .then(pl.col("type_releve").is_not_null())
        .otherwise(pl.col("id_calendrier_distributeur").is_not_null())
        .alias("has_metadata")
    )


def expr_cadrans_count(*index_cols: str) -> pl.Expr:
    """
    Expression pour compter le nombre de cadrans avec valeurs non-NULL.

    Fonction pure : Fn(*str) -> pl.Expr

    Args:
        *index_cols: Noms des colonnes de cadrans à compter

    Returns:
        Expression Polars avec comptage horizontal

    Example:
        >>> df.with_columns(expr_cadrans_count("hp", "hc", "base"))
    """
    return pl.sum_horizontal([
        pl.col(col).is_not_null().cast(pl.Int32) for col in index_cols
    ]).alias("cadrans_count")