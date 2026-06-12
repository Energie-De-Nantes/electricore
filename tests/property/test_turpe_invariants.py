"""
Property tests des invariants TURPE (issue #194).

Entrées générées depuis les schémas Pandera (socle `strategies.py`),
vraies règles versionnées (CSV) comme fixture. Invariants :
- puissances/énergies positives → contributions ≥ 0 ;
- tarifs nuls → contribution nulle ;
- préservation des lignes (aucune période perdue ni dupliquée par la jointure) ;
- validité temporelle (aucune règle appliquée hors de sa fenêtre).
"""

import datetime as dt
from zoneinfo import ZoneInfo

import hypothesis.strategies as st
import polars as pl
import pytest
from hypothesis import given

from electricore.core.models.periode_abonnement import PeriodeAbonnement
from electricore.core.models.periode_energie import PeriodeEnergie
from electricore.core.pipelines.turpe import (
    ajouter_turpe_fixe,
    ajouter_turpe_variable,
    load_turpe_rules,
)
from tests.property.strategies import strategie_depuis_schema

TZ_PARIS = ZoneInfo("Europe/Paris")

# Vraies règles versionnées — fixture de module (hypothesis interdit les fixtures function-scoped)
REGLES = load_turpe_rules().collect()

FTAS = REGLES["Formule_Tarifaire_Acheminement"].unique().sort().to_list()

_COLONNES_TARIFS = [
    "cg", "cc", "b",
    "b_hph", "b_hch", "b_hpb", "b_hcb",
    "c_hph", "c_hch", "c_hpb", "c_hcb",
    "c_hp", "c_hc", "c_base",
    "cmdps",
]  # fmt: skip


def _regles_synthetiques(start: dt.datetime, end: dt.datetime | None, **tarifs: float) -> pl.DataFrame:
    """Une règle TURPE contrôlée (FTA 'BTTEST'), même schéma que load_turpe_rules."""
    data = {"Formule_Tarifaire_Acheminement": ["BTTEST"], "start": [start], "end": [end]}
    data.update({col: [tarifs.get(col)] for col in _COLONNES_TARIFS})
    return pl.DataFrame(
        data,
        schema_overrides={
            "start": pl.Datetime("us", "Europe/Paris"),
            "end": pl.Datetime("us", "Europe/Paris"),
            **{col: pl.Float64 for col in _COLONNES_TARIFS},
        },
    )


# FTA dont les règles portent un coefficient C5 (b) — le calcul fixe C5 exige b non-null
FTAS_C5 = REGLES.filter(pl.col("b").is_not_null())["Formule_Tarifaire_Acheminement"].unique().sort().to_list()

# Après le start le plus récent, chaque FTA a exactement une fenêtre applicable
# (toutes les grilles actuelles sont ouvertes, end=null)
_DEBUT_MIN = REGLES["start"].max().replace(tzinfo=None) + dt.timedelta(days=1)


def _periodes_abonnement(max_size: int = 10) -> st.SearchStrategy[pl.DataFrame]:
    """Périodes d'abonnement générées depuis le schéma Pandera.

    Les surcharges portent uniquement les contraintes que le schéma ne connaît pas :
    FTA existant dans les règles, puissance C5 plausible, durée ≥ 1 jour,
    début dans la fenêtre des grilles actuelles.
    """
    return strategie_depuis_schema(
        PeriodeAbonnement,
        colonnes=[
            "ref_situation_contractuelle",
            "pdl",
            "formule_tarifaire_acheminement",
            "puissance_souscrite_kva",
            "nb_jours",
            "debut",
        ],
        surcharges={
            "formule_tarifaire_acheminement": st.sampled_from(FTAS_C5),
            "puissance_souscrite_kva": st.floats(0.0, 36.0, allow_nan=False),
            "nb_jours": st.integers(1, 366),
            "debut": st.datetimes(
                _DEBUT_MIN,
                dt.datetime(2030, 12, 31),
                timezones=st.just(TZ_PARIS),
                allow_imaginary=False,
            ),
        },
        max_size=max_size,
    )


@pytest.mark.hypothesis
@given(periodes=_periodes_abonnement())
def test_turpe_fixe_contributions_positives(periodes: pl.DataFrame):
    """Puissances ≥ 0 et durées ≥ 1 jour → TURPE fixe ≥ 0 (les tarifs réels sont positifs)."""
    result = ajouter_turpe_fixe(periodes.lazy(), REGLES.lazy()).collect()

    assert result["turpe_fixe_eur"].null_count() == 0
    assert (result["turpe_fixe_eur"] >= 0).all()


@pytest.mark.hypothesis
@given(periodes=_periodes_abonnement())
def test_turpe_fixe_preserve_les_lignes(periodes: pl.DataFrame):
    """La jointure aux règles ne perd ni ne duplique aucune période."""
    avec_index = periodes.with_row_index("ligne")

    result = ajouter_turpe_fixe(avec_index.lazy(), REGLES.lazy()).collect()

    assert sorted(result["ligne"].to_list()) == list(range(periodes.height))


@pytest.mark.hypothesis
@given(
    periodes=strategie_depuis_schema(
        PeriodeAbonnement,
        colonnes=["pdl", "formule_tarifaire_acheminement", "puissance_souscrite_kva", "nb_jours", "debut"],
        surcharges={
            "formule_tarifaire_acheminement": st.just("BTTEST"),
            "puissance_souscrite_kva": st.floats(0.0, 36.0, allow_nan=False),
            "nb_jours": st.integers(1, 366),
            "debut": st.datetimes(
                dt.datetime(2020, 1, 1),
                dt.datetime(2030, 12, 31),
                timezones=st.just(TZ_PARIS),
                allow_imaginary=False,
            ),
        },
    )
)
def test_turpe_fixe_aucune_regle_hors_fenetre(periodes: pl.DataFrame):
    """Validité temporelle : une règle ne s'applique qu'aux périodes dont le début est dans [start, end)."""
    fenetre = _regles_synthetiques(
        start=dt.datetime(2024, 1, 1, tzinfo=TZ_PARIS),
        end=dt.datetime(2025, 1, 1, tzinfo=TZ_PARIS),
        cg=1.0, cc=1.0, b=1.0,
    )  # fmt: skip
    avec_index = periodes.with_row_index("ligne")

    result = ajouter_turpe_fixe(avec_index.lazy(), fenetre.lazy()).collect()

    attendues = avec_index.filter((pl.col("debut") >= fenetre["start"][0]) & (pl.col("debut") < fenetre["end"][0]))
    assert sorted(result["ligne"].to_list()) == sorted(attendues["ligne"].to_list())


# =============================================================================
# TURPE VARIABLE
# =============================================================================

_COLONNES_ENERGIE = [
    "energie_base_kwh", "energie_hp_kwh", "energie_hc_kwh",
    "energie_hph_kwh", "energie_hpb_kwh", "energie_hch_kwh", "energie_hcb_kwh",
]  # fmt: skip


def _periodes_energie(fta: st.SearchStrategy[str], debut_min: dt.datetime) -> st.SearchStrategy[pl.DataFrame]:
    """Périodes d'énergie générées depuis le schéma Pandera, énergies positives."""
    return strategie_depuis_schema(
        PeriodeEnergie,
        colonnes=["pdl", "formule_tarifaire_acheminement", "debut", *_COLONNES_ENERGIE],
        surcharges={
            "formule_tarifaire_acheminement": fta,
            "debut": st.datetimes(
                debut_min,
                dt.datetime(2030, 12, 31),
                timezones=st.just(TZ_PARIS),
                allow_imaginary=False,
            ),
            # prémisse de l'invariant : énergies positives (les nulls injectés par
            # la nullabilité du schéma restent possibles → contribution 0)
            **{col: st.floats(0.0, 1e6, allow_nan=False) for col in _COLONNES_ENERGIE},
        },
    )


@pytest.mark.hypothesis
@given(periodes=_periodes_energie(st.sampled_from(FTAS), _DEBUT_MIN))
def test_turpe_variable_contributions_positives(periodes: pl.DataFrame):
    """Énergies ≥ 0 → TURPE variable ≥ 0 (les tarifs réels sont positifs)."""
    result = ajouter_turpe_variable(periodes.lazy(), REGLES.lazy()).collect()

    assert result["turpe_variable_eur"].null_count() == 0
    assert (result["turpe_variable_eur"] >= 0).all()


@pytest.mark.hypothesis
@given(periodes=_periodes_energie(st.just("BTTEST"), dt.datetime(2024, 1, 2)))
def test_turpe_variable_tarifs_nuls_contribution_nulle(periodes: pl.DataFrame):
    """Tous les tarifs à zéro → contribution nulle, quelles que soient les énergies."""
    tarifs_nuls = _regles_synthetiques(
        start=dt.datetime(2024, 1, 1, tzinfo=TZ_PARIS),
        end=None,
        **{col: 0.0 for col in ["c_hph", "c_hch", "c_hpb", "c_hcb", "c_hp", "c_hc", "c_base"]},
    )

    result = ajouter_turpe_variable(periodes.lazy(), tarifs_nuls.lazy()).collect()

    assert (result["turpe_variable_eur"] == 0.0).all()
    assert result["turpe_variable_eur"].null_count() == 0


@pytest.mark.hypothesis
@given(periodes=_periodes_energie(st.sampled_from(FTAS), _DEBUT_MIN))
def test_turpe_variable_preserve_les_lignes(periodes: pl.DataFrame):
    """La jointure aux règles ne perd ni ne duplique aucune période d'énergie."""
    avec_index = periodes.with_row_index("ligne")

    result = ajouter_turpe_variable(avec_index.lazy(), REGLES.lazy()).collect()

    assert sorted(result["ligne"].to_list()) == list(range(periodes.height))
