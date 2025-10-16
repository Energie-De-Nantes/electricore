"""
Gestion des dépendances optionnelles pour ElectriCore.

Ce module fournit des imports conditionnels et des décorateurs
pour gérer gracieusement l'absence de dépendances optionnelles.
"""

import functools
from typing import TypeVar, Callable, Any

# Détection de Pandera
try:
    import pandera.polars as pa
    from pandera.typing.polars import DataFrame, LazyFrame
    HAS_PANDERA = True
except ImportError:
    HAS_PANDERA = False
    # Créer des types de remplacement
    pa = None  # type: ignore
    DataFrame = Any  # type: ignore
    LazyFrame = Any  # type: ignore


F = TypeVar('F', bound=Callable[..., Any])


def optional_validation(func: F) -> F:
    """
    Décorateur qui désactive la validation Pandera si non installée.

    Si Pandera n'est pas disponible, la fonction s'exécute normalement
    sans validation. Sinon, le décorateur @pa.check_types est appliqué.

    Usage:
        @optional_validation
        def pipeline_perimetre(df: LazyFrame[PerimetreModel]) -> LazyFrame[PerimetreModel]:
            ...
    """
    if HAS_PANDERA:
        return pa.check_types(func)  # type: ignore
    else:
        # Pas de validation, fonction passthrough
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)
        return wrapper  # type: ignore


__all__ = [
    'HAS_PANDERA',
    'pa',
    'DataFrame',
    'LazyFrame',
    'optional_validation',
]
