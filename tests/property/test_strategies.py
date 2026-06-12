"""
Tests du socle property-based : dérivation de stratégies depuis les schémas Pandera.

Le helper `strategie_depuis_schema` est la source de vérité unique : il dérive
colonnes, dtypes, nullabilité et bornes simples depuis un DataFrameModel Pandera,
sans redéfinition manuelle (issue #194, garde-fou anti-2025).
"""

import polars as pl
import pytest
from hypothesis import given

from electricore.core.models.periode_abonnement import PeriodeAbonnement
from tests.property.strategies import strategie_depuis_schema


@pytest.mark.hypothesis
@given(df=strategie_depuis_schema(PeriodeAbonnement, max_size=10))
def test_frames_generes_valident_le_schema_pandera(df: pl.DataFrame):
    """Les frames générés depuis PeriodeAbonnement passent la validation Pandera.

    C'est la balle traçante du socle : dtypes, nullabilité, bornes (ge=0)
    et regex (mois_annee) dérivés du schéma produisent des données valides.
    """
    PeriodeAbonnement.validate(df, lazy=True)
