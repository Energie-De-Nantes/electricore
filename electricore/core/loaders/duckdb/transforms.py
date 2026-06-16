"""
Transformations composables pour LazyFrame DuckDB.

Ce module fournit des transformations fonctionnelles pures suivant le pattern :
Fn(LazyFrame) -> LazyFrame

Les transformations peuvent être composées avec compose() pour créer
des pipelines de transformation complexes.
"""

from collections.abc import Callable
from typing import Any

import polars as pl

from .expressions import (
    DATE_COLS_FACTURES,
    DATE_COLS_HISTORIQUE,
    DATE_COLS_R64,
    DATE_COLS_RELEVES,
    expr_dates_with_timezone,
)

# =============================================================================
# HIGHER-ORDER FUNCTIONS (Currying)
# =============================================================================


def transform_dates(date_cols: tuple[str, ...], tz: str = "Europe/Paris") -> Callable[[pl.LazyFrame], pl.LazyFrame]:
    """
    Retourne une fonction de transformation des dates vers un timezone.

    Higher-order function : Fn(tuple[str], str) -> Fn(LazyFrame) -> LazyFrame

    Args:
        date_cols: Tuple des colonnes dates à convertir
        tz: Timezone cible (défaut: Europe/Paris)

    Returns:
        Fonction de transformation LazyFrame

    Example:
        >>> transform_fn = transform_dates(("date_debut", "date_fin"))
        >>> lf_transformed = transform_fn(lf)
    """

    def _transform(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns(expr_dates_with_timezone(*date_cols, tz=tz))

    return _transform


def transform_add_defaults(**defaults: Any) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
    """
    Retourne une fonction d'ajout de colonnes avec valeurs par défaut.

    Higher-order function : Fn(**kwargs) -> Fn(LazyFrame) -> LazyFrame

    Args:
        **defaults: Paires colonne=valeur pour les valeurs par défaut

    Returns:
        Fonction de transformation LazyFrame

    Example:
        >>> transform_fn = transform_add_defaults(unite="kWh", precision="kWh")
        >>> lf_transformed = transform_fn(lf)
    """

    def _transform(lf: pl.LazyFrame) -> pl.LazyFrame:
        return lf.with_columns([pl.lit(value).alias(col) for col, value in defaults.items()])

    return _transform


# =============================================================================
# COMPOSITION FONCTIONNELLE
# =============================================================================


def compose(*transforms: Callable[[pl.LazyFrame], pl.LazyFrame]) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
    """
    Compose plusieurs transformations en une seule fonction.

    Fonction d'ordre supérieur : Fn(*Fn) -> Fn

    Les transformations sont appliquées de gauche à droite (ordre naturel).

    Args:
        *transforms: Fonctions de transformation à composer

    Returns:
        Fonction composée appliquant toutes les transformations

    Example:
        >>> transform_pipeline = compose(
        ...     transform_dates(("date_releve",)),
        ...     transform_add_defaults(source="flux_R151")
        ... )
        >>> lf_result = transform_pipeline(lf)
    """

    def _composed(lf: pl.LazyFrame) -> pl.LazyFrame:
        result = lf
        for transform in transforms:
            result = transform(result)
        return result

    return _composed


# =============================================================================
# PIPELINES PRÉDÉFINIS (Compositions pures)
# =============================================================================

# Pipeline pour historique (flux C15)
transform_historique = compose(
    transform_dates(DATE_COLS_HISTORIQUE), transform_add_defaults(unite="kWh", precision="kWh")
)


# Pipeline pour relevés individuels (flux R151, R15 — endpoints /flux/* et registre).
# La conversion Wh→kWh vit désormais au boundary de linéarisation dbt (ADR-0034) :
# flux_r151 émet des index en kWh entiers. Le loader ne fait plus que l'harmonisation
# des dates ; convertir ici doublerait la division.
transform_releves = transform_dates(DATE_COLS_RELEVES)


# Pipeline pour factures (flux F15)
transform_factures = transform_dates(DATE_COLS_FACTURES)


# Pipeline pour relevés R64 — conversion Wh→kWh portée par flux_r64 en dbt (ADR-0034).
transform_r64 = transform_dates(DATE_COLS_R64)
