"""
Property tests des invariants de conservation à l'agrégation facturation (issue #196).

`pipeline_facturation` agrège des sous-périodes plates en méta-périodes
mensuelles : c'est ici que le property-based testing attrape ce que les
fixtures choisies à la main ratent — erreurs de signe, périodes perdues dans
un group_by, doubles comptages.

Les clés (rsc, pdl, mois) sont tirées de petits pools pour provoquer de
vraies agrégations multi-lignes et des recouvrements partiels entre
abonnements et énergies (jointure full exercée dans les trois cas :
abo seul, énergie seule, les deux).
"""

import datetime as dt
import math
from zoneinfo import ZoneInfo

import hypothesis.strategies as st
import polars as pl
import pytest
from hypothesis import given, settings
from pandera.polars import DataFrameSchema
from polars.testing import assert_frame_equal

from electricore.core.models.periode_abonnement import PeriodeAbonnement
from electricore.core.models.periode_energie import PeriodeEnergie
from electricore.core.models.periode_meta import PeriodeMeta
from electricore.core.pipelines.facturation import (
    agreger_abonnements_mensuel,
    agreger_energies_mensuel,
    pipeline_facturation,
)
from tests.property.strategies import strategie_depuis_schema

# Pools réduits : groupes de plusieurs sous-périodes et recouvrements partiels
_RSC = st.sampled_from(["RSC1", "RSC2"])
_PDL = st.sampled_from(["PDL00000000001", "PDL00000000002"])
_MOIS = st.sampled_from(["2025-01", "2025-02"])
_CLES = ["ref_situation_contractuelle", "pdl", "mois_annee"]

# Plages disjointes : debut < fin tient structurellement, sans dépendance
# entre colonnes (garde-fou du socle — pas de génération ligne à ligne)
_TZ = ZoneInfo("Europe/Paris")
_DEBUTS = st.datetimes(dt.datetime(2025, 1, 1), dt.datetime(2025, 1, 31), timezones=st.just(_TZ), allow_imaginary=False)
_FINS = st.datetimes(dt.datetime(2025, 2, 2), dt.datetime(2025, 2, 28), timezones=st.just(_TZ), allow_imaginary=False)


def _abonnements(max_size: int = 12) -> st.SearchStrategy[pl.DataFrame]:
    return strategie_depuis_schema(
        PeriodeAbonnement,
        surcharges={
            "ref_situation_contractuelle": _RSC,
            "pdl": _PDL,
            "mois_annee": _MOIS,
            "nb_jours": st.integers(1, 31),  # la puissance moyenne est pondérée par nb_jours
            # PeriodeAbonnement ne borne pas la puissance alors que PeriodeMeta exige ≥ 0 :
            # on génère du réaliste (l'écart de schémas est signalé dans la PR #196)
            "puissance_souscrite_kva": st.floats(0.0, 250.0, allow_nan=False),
            "turpe_fixe_eur": st.floats(0.0, 1e5, allow_nan=False),
            "debut": _DEBUTS,
            "fin": _FINS,
            # `first()` du group_by suppose la FTA constante par groupe (cohérence métier réelle)
            "formule_tarifaire_acheminement": st.just("BTINFCUST"),
        },
        max_size=max_size,
    )


def _energies(max_size: int = 12) -> st.SearchStrategy[pl.DataFrame]:
    return strategie_depuis_schema(
        PeriodeEnergie,
        surcharges={
            "ref_situation_contractuelle": _RSC,
            "pdl": _PDL,
            "mois_annee": _MOIS,
            "debut": _DEBUTS,
            "fin": _FINS,
            # PeriodeEnergie ne borne ni énergies ni TURPE variable alors que
            # PeriodeMeta exige ≥ 0 : on génère du réaliste (écart signalé en PR)
            **{
                col: st.floats(0.0, 1e6, allow_nan=False)
                for col in ["energie_base_kwh", "energie_hp_kwh", "energie_hc_kwh", "turpe_variable_eur"]
            },
        },
        max_size=max_size,
    )


def _somme_par_groupe(df: pl.DataFrame, colonne: str) -> dict:
    """Oracle indépendant : somme (nulls = 0) par clé d'agrégation."""
    agg = df.group_by(_CLES).agg(pl.col(colonne).fill_null(0.0).sum())
    return {tuple(r[k] for k in _CLES): r[colonne] for r in agg.iter_rows(named=True)}


# =============================================================================
# Conservation à l'agrégation
# =============================================================================


@pytest.mark.hypothesis
@given(periodes=_abonnements())
def test_abonnements_conservation_des_montants_et_jours(periodes: pl.DataFrame):
    """La somme des sous-périodes d'un mois égale l'agrégat mensuel (turpe fixe, nb_jours)."""
    result = agreger_abonnements_mensuel(periodes.lazy()).collect()

    turpe_attendu = _somme_par_groupe(periodes, "turpe_fixe_eur")
    jours_attendus = _somme_par_groupe(periodes, "nb_jours")
    for ligne in result.iter_rows(named=True):
        cle = tuple(ligne[k] for k in _CLES)
        assert math.isclose(ligne["turpe_fixe_eur"], turpe_attendu[cle], rel_tol=1e-9, abs_tol=1e-6)
        assert ligne["nb_jours"] == jours_attendus[cle]


@pytest.mark.hypothesis
@given(periodes=_abonnements())
def test_abonnements_puissance_moyenne_ponderee(periodes: pl.DataFrame):
    """La puissance agrégée est la moyenne pondérée par nb_jours (linéarité de la tarification)."""
    result = agreger_abonnements_mensuel(periodes.lazy()).collect()

    oracle = periodes.group_by(_CLES).agg(
        ((pl.col("puissance_souscrite_kva") * pl.col("nb_jours")).sum() / pl.col("nb_jours").sum()).alias("pmoy")
    )
    attendu = {tuple(r[k] for k in _CLES): r["pmoy"] for r in oracle.iter_rows(named=True)}
    for ligne in result.iter_rows(named=True):
        cle = tuple(ligne[k] for k in _CLES)
        assert math.isclose(ligne["puissance_moyenne_kva"], attendu[cle], rel_tol=1e-9, abs_tol=1e-9)


@pytest.mark.hypothesis
@given(periodes=_energies())
def test_energies_conservation_des_sommes(periodes: pl.DataFrame):
    """Énergies et TURPE variable sont additifs : aucune perte ni double comptage au group_by."""
    result = agreger_energies_mensuel(periodes.lazy()).collect()

    for colonne in ["energie_base_kwh", "energie_hp_kwh", "energie_hc_kwh", "turpe_variable_eur"]:
        attendu = _somme_par_groupe(periodes, colonne)
        for ligne in result.iter_rows(named=True):
            cle = tuple(ligne[k] for k in _CLES)
            assert math.isclose(ligne[colonne], attendu[cle], rel_tol=1e-9, abs_tol=1e-6), colonne


# =============================================================================
# Aucune perte de (rsc, pdl, mois) à travers le pipeline complet
# =============================================================================


def _cles(df: pl.DataFrame) -> set[tuple]:
    return {tuple(r[k] for k in _CLES) for r in df.select(_CLES).iter_rows(named=True)}


@pytest.mark.hypothesis
@settings(max_examples=10)  # pipeline complet : plus lourd, on borne la durée CI
@given(abonnements=_abonnements(max_size=8), energies=_energies(max_size=8))
def test_pipeline_aucune_perte_de_cle(abonnements: pl.DataFrame, energies: pl.DataFrame):
    """Chaque (rsc, pdl, mois) présent en entrée apparaît en sortie, et réciproquement — une fois."""
    result = pipeline_facturation(abonnements.lazy(), energies.lazy()).collect()

    assert _cles(result) == _cles(abonnements) | _cles(energies)
    assert result.select(_CLES).is_unique().all(), "grain (rsc, pdl, mois) dupliqué"


@pytest.mark.hypothesis
@settings(max_examples=10)
@given(abonnements=_abonnements(max_size=8), energies=_energies(max_size=8))
def test_pipeline_stable_sous_permutation_des_entrees(abonnements: pl.DataFrame, energies: pl.DataFrame):
    """L'ordre des lignes d'entrée ne change pas le résultat."""
    tri = ["ref_situation_contractuelle", "pdl", "mois_annee", "debut"]
    endroit = pipeline_facturation(abonnements.lazy(), energies.lazy()).collect().sort(tri)
    envers = pipeline_facturation(abonnements.reverse().lazy(), energies.reverse().lazy()).collect().sort(tri)

    # memo_puissance concatène dans l'ordre d'arrivée : hors du contrat d'ordre ;
    # tolérance flottante : l'ordre de sommation peut différer au dernier ulp
    assert_frame_equal(endroit.drop("memo_puissance"), envers.drop("memo_puissance"), rel_tol=1e-12, abs_tol=1e-9)


# =============================================================================
# Contrat Pandera sous entrées générées (oracle gratuit)
# =============================================================================


@pytest.mark.hypothesis
@settings(max_examples=10)
@given(abonnements=_abonnements(max_size=8), energies=_energies(max_size=8))
def test_pipeline_respecte_le_schema_de_sortie(abonnements: pl.DataFrame, energies: pl.DataFrame):
    """Les contrats colonne de PeriodeMeta tiennent sur la sortie, quel que soit l'input valide.

    Les checks dataframe (debut < fin, nb_jours == fin - debut) supposent des
    sous-périodes métier cohérentes entre elles — hors périmètre des stratégies
    plates (garde-fou) : ils restent couverts par les fixtures + snapshots.
    """
    result = pipeline_facturation(abonnements.lazy(), energies.lazy()).collect()

    contrats_colonnes = DataFrameSchema(PeriodeMeta.to_schema().columns, strict=False)
    contrats_colonnes.validate(result, lazy=True)
