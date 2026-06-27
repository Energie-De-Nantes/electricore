"""Schémas Pandera des deux bords de l'estimation de provision (ADR-0048).

Frontière d'entrée R67 « énergie-par-période » + frontière de sortie `EstimationProvision`.
"""

from datetime import date

import pandera.errors as pa_errors
import polars as pl
import pytest

from electricore.core.models.provision import (
    PROFONDEURS,
    EnergieParPeriodeR67,
    EstimationProvision,
)


def _frame_r67_base(**overrides) -> pl.DataFrame:
    base = {
        "pdl": ["PDL1", "PDL1"],
        "debut": [date(2025, 1, 1), date(2025, 2, 1)],
        "fin": [date(2025, 2, 1), date(2025, 3, 1)],
        "code_nature": ["R", "R"],
        "energie_base_kwh": [100, 120],
        "energie_hp_kwh": [None, None],
        "energie_hc_kwh": [None, None],
        "energie_hph_kwh": [None, None],
        "energie_hpb_kwh": [None, None],
        "energie_hch_kwh": [None, None],
        "energie_hcb_kwh": [None, None],
    }
    base.update(overrides)
    return pl.DataFrame(base)


def test_frontiere_r67_accepte_base_only_avec_natures_rec():
    """Tracer : base-only, natures R/E/C, négatifs préservés — la frontière accepte."""
    frame = _frame_r67_base(
        code_nature=["R", "C"],
        energie_base_kwh=[100, -30],  # régul négative préservée
    )
    EnergieParPeriodeR67.validate(frame)  # ne lève pas


def test_frontiere_r67_tolere_colonnes_annexes():
    """R67 porte d'autres colonnes (code_grille, periode_*) → strict=False les tolère."""
    frame = _frame_r67_base().with_columns(
        pl.lit("D").alias("code_grille"),
        pl.lit("CYCL").alias("id_motif_releve"),
    )
    EnergieParPeriodeR67.validate(frame)


def test_frontiere_r67_rejette_nature_inconnue():
    frame = _frame_r67_base(code_nature=["R", "X"])
    with pytest.raises(pa_errors.SchemaError):
        EnergieParPeriodeR67.validate(frame)


def _frame_estimation(**overrides) -> pl.DataFrame:
    base = {
        "pdl": ["PDL1"],
        "energie_base_kwh": [1200.0],
        "energie_hp_kwh": [None],
        "energie_hc_kwh": [None],
        "energie_hph_kwh": [None],
        "energie_hpb_kwh": [None],
        "energie_hch_kwh": [None],
        "energie_hcb_kwh": [None],
        "energie_base_mensuel_kwh": [100.0],
        "energie_hp_mensuel_kwh": [None],
        "energie_hc_mensuel_kwh": [None],
        "energie_hph_mensuel_kwh": [None],
        "energie_hpb_mensuel_kwh": [None],
        "energie_hch_mensuel_kwh": [None],
        "energie_hcb_mensuel_kwh": [None],
        "couverture_debut": [date(2024, 6, 1)],
        "couverture_fin": [date(2025, 6, 1)],
        "couverture_mois": [12.0],
        "couverture_suffisante": [True],
        "profondeur_cadran": ["base"],
        "qualite": ["réelle"],
        "presence_regularisation": [False],
        "signal_alertable": [False],
    }
    base.update(overrides)
    return pl.DataFrame(base)


def test_sortie_estimation_provision_valide():
    EstimationProvision.validate(_frame_estimation())


def test_sortie_rejette_profondeur_inconnue():
    frame = _frame_estimation(profondeur_cadran=["octuple"])
    with pytest.raises(pa_errors.SchemaError):
        EstimationProvision.validate(frame)


def test_profondeurs_canoniques():
    assert PROFONDEURS == ("base", "hp_hc", "4_cadrans")
