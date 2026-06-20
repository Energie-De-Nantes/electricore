"""
Property tests des invariants abonnements + énergie (issue #197) — le slice frontière.

Stratégie d'historique **volontairement minimale** (garde-fou du socle) :
un seul PDL, dates d'événements ordonnées et distinctes (jours civils),
séquence MES → … → RES, sans changement de calendrier ni de compteur,
index construits par cumul d'incréments positifs (monotones par construction).
Si un invariant exigeait davantage de cohérence métier que ça, il sortirait
du périmètre property-based : fixtures + snapshots.

Invariants abonnements :
- les périodes d'un même PDL ne se chevauchent jamais ;
- la couverture est contiguë (fin d'une période = début de la suivante) ;
- nb_jours ≥ 1 sur toute période émise.

Invariants énergie :
- index monotones croissants → toutes les énergies calculées ≥ 0 ;
- conservation des cadrans : energie_base = energie_hp + energie_hc
  quand les trois cadrans sont présents ;
- aucune période d'énergie hors des bornes du contrat (MES, RES).
"""

import datetime as dt
import math
from itertools import accumulate
from zoneinfo import ZoneInfo

import hypothesis.strategies as st
import polars as pl
import pytest
from hypothesis import given, settings

from electricore.core.pipelines.abonnements import pipeline_abonnements
from electricore.core.pipelines.energie import pipeline_energie

_TZ = ZoneInfo("Europe/Paris")
_PDL = "PDL00000000001"
_RSC = "RSC1"
_FTA = "BTINFCUST"  # FTA réelle : les périodes ≥ 2024 tombent toutes dans une fenêtre TURPE

_INDEX_CADRANS = ["base", "hp", "hc", "hch", "hph", "hpb", "hcb"]


def _ts(jour: dt.date) -> dt.datetime:
    return dt.datetime(jour.year, jour.month, jour.day, tzinfo=_TZ)


def _colonnes_historique(n: int) -> dict:
    """Colonnes communes d'un historique enrichi mono-PDL (schéma Historique)."""
    return {
        "pdl": [_PDL] * n,
        "ref_situation_contractuelle": [_RSC] * n,
        "segment_clientele": ["C5"] * n,
        "type_evenement": ["contractuel"] * n,
        "formule_tarifaire_acheminement": [_FTA] * n,
        "type_compteur": ["CCB"] * n,
        "num_compteur": ["COMPTEUR1"] * n,
        "resume_modification": [""] * n,
        "avant_date_releve": [None] * n,
        "apres_date_releve": [None] * n,
        "avant_nature_index": [None] * n,
        "apres_nature_index": [None] * n,
        "avant_id_calendrier_fournisseur": [None] * n,
        "apres_id_calendrier_fournisseur": [None] * n,
    }


def _schema_overrides() -> dict:
    overrides = {
        "date_evenement": pl.Datetime("us", "Europe/Paris"),
        "avant_date_releve": pl.Datetime("us", "Europe/Paris"),
        "apres_date_releve": pl.Datetime("us", "Europe/Paris"),
        "puissance_souscrite_kva": pl.Float64,
        "avant_id_calendrier_distributeur": pl.Utf8,
        "apres_id_calendrier_distributeur": pl.Utf8,
        "avant_nature_index": pl.Utf8,
        "apres_nature_index": pl.Utf8,
        "avant_id_calendrier_fournisseur": pl.Utf8,
        "apres_id_calendrier_fournisseur": pl.Utf8,
    }
    for cadran in _INDEX_CADRANS:
        overrides[f"avant_index_{cadran}_kwh"] = pl.Int64
        overrides[f"apres_index_{cadran}_kwh"] = pl.Int64
    return overrides


# =============================================================================
# Abonnements — MES → MCT… → RES, puissances quelconques
# =============================================================================


@st.composite
def _scenario_abonnements(draw):
    jours = sorted(
        draw(st.lists(st.dates(dt.date(2024, 1, 2), dt.date(2026, 12, 1)), min_size=2, max_size=6, unique=True))
    )
    puissances = draw(st.lists(st.floats(1.0, 36.0, allow_nan=False), min_size=len(jours), max_size=len(jours)))
    return jours, puissances


def _historique_abonnements(jours: list[dt.date], puissances: list[float]) -> pl.LazyFrame:
    n = len(jours)
    evenements = ["MES"] + ["MCT"] * (n - 2) + ["RES"]
    data = {
        "date_evenement": [_ts(j) for j in jours],
        "evenement_declencheur": evenements,
        "etat_contractuel": ["EN SERVICE"] * (n - 1) + ["RESILIE"],
        "puissance_souscrite_kva": puissances,
        "impacte_abonnement": [True] * n,
        "impacte_energie": [False] * n,
        **_colonnes_historique(n),
    }
    for prefixe in ("avant", "apres"):
        data[f"{prefixe}_id_calendrier_distributeur"] = [None] * n
        for cadran in _INDEX_CADRANS:
            data[f"{prefixe}_index_{cadran}_kwh"] = [None] * n
    return pl.DataFrame(data, schema_overrides=_schema_overrides()).lazy()


@pytest.mark.hypothesis
@settings(max_examples=20)
@given(scenario=_scenario_abonnements())
def test_abonnements_periodes_contigues_sans_chevauchement(scenario):
    """Sur un PDL : couverture contiguë, aucun chevauchement, nb_jours ≥ 1, bornées par MES/RES."""
    jours, puissances = scenario
    result = pipeline_abonnements(_historique_abonnements(jours, puissances)).collect().sort("debut")

    assert result.height == len(jours) - 1, "une période par intervalle entre événements"
    debuts, fins = result["debut"].to_list(), result["fin"].to_list()
    for i in range(result.height - 1):
        assert fins[i] == debuts[i + 1], "couverture contiguë : fin = début suivant"
    assert (result["nb_jours"] >= 1).all()
    assert debuts[0] == _ts(jours[0]) and fins[-1] == _ts(jours[-1]), "bornes du contrat respectées"


@pytest.mark.hypothesis
@settings(max_examples=20)
@given(scenario=_scenario_abonnements())
def test_abonnements_puissance_de_la_periode(scenario):
    """Chaque période porte la puissance de l'événement qui l'ouvre."""
    jours, puissances = scenario
    result = pipeline_abonnements(_historique_abonnements(jours, puissances)).collect().sort("debut")

    assert result["puissance_souscrite_kva"].to_list() == puissances[:-1]


# =============================================================================
# Énergie — MES → FACTURATION… → RES, index monotones, base = hp + hc
# =============================================================================


@st.composite
def _scenario_energie(draw):
    jours = sorted(
        draw(st.lists(st.dates(dt.date(2024, 1, 2), dt.date(2026, 12, 1)), min_size=2, max_size=7, unique=True))
    )
    # Index en kWh entiers (ADR-0034) : valeurs entières → cumuls Int64 exacts.
    hp0 = draw(st.integers(0, 100_000))
    hc0 = draw(st.integers(0, 100_000))
    increments = draw(
        st.lists(
            st.tuples(st.integers(0, 500), st.integers(0, 500)),
            min_size=len(jours) - 1,
            max_size=len(jours) - 1,
        )
    )
    hp = list(accumulate([hp0] + [i[0] for i in increments]))
    hc = list(accumulate([hc0] + [i[1] for i in increments]))
    return jours, hp, hc


def _chronologie_energie(jours: list[dt.date], hp: list[float], hc: list[float]) -> pl.LazyFrame:
    """*Chronologie des relevés* synthétique (ADR-0041) — ce que le mart dbt produit :
    relevés C15 aux bornes de vie (après MES, avant RES) + R151 aux dates intermédiaires
    (bornes FACTURATION), situation attribuée, dédoublonnée. Entrée directe de
    `pipeline_energie` depuis #377 (l'assemblage ne vit plus au cœur)."""
    n = len(jours)
    interieurs = list(range(1, n - 1))
    # MES après (ordre True) à la 1ʳᵉ borne, R151 (ordre False) au milieu, RES avant
    # (ordre False) à la dernière borne.
    idxs = [0, *interieurs, n - 1]
    sources = ["flux_C15", *["flux_R151"] * len(interieurs), "flux_C15"]
    ordres = [True, *[False] * len(interieurs), False]
    nb = len(idxs)
    data = {
        "pdl": [_PDL] * nb,
        "ref_situation_contractuelle": [_RSC] * nb,
        "formule_tarifaire_acheminement": [_FTA] * nb,
        "niveau_ouverture_services": ["2"] * nb,
        "date_releve": [_ts(jours[i]) for i in idxs],
        "source": sources,
        "releve_id": [None] * nb,
        "nature_index": ["réel"] * nb,
        "evenement_declencheur": [None] * nb,
        "ordre_index": ordres,
        "id_calendrier_distributeur": ["DI000002"] * nb,
        "releve_manquant": [False] * nb,
        "index_base_kwh": [None] * nb,
        "index_hp_kwh": [hp[i] for i in idxs],
        "index_hc_kwh": [hc[i] for i in idxs],
        "index_hch_kwh": [None] * nb,
        "index_hph_kwh": [None] * nb,
        "index_hpb_kwh": [None] * nb,
        "index_hcb_kwh": [None] * nb,
    }
    return pl.DataFrame(
        data,
        schema_overrides={
            "date_releve": pl.Datetime("us", "Europe/Paris"),
            # Index relevés en kWh entiers (ADR-0034) : Int64, type émis nativement par dbt.
            **{f"index_{c}_kwh": pl.Int64 for c in _INDEX_CADRANS},
            # dtypes explicites : un scénario MES → RES sans relevé intermédiaire
            # produit un frame vide, dont les colonnes seraient sinon typées Null
            **{
                col: pl.Utf8
                for col in [
                    "pdl",
                    "ref_situation_contractuelle",
                    "formule_tarifaire_acheminement",
                    "niveau_ouverture_services",
                    "source",
                    "releve_id",
                    "nature_index",
                    "evenement_declencheur",
                    "id_calendrier_distributeur",
                ]
            },
            "ordre_index": pl.Boolean,
            "releve_manquant": pl.Boolean,
        },
    ).lazy()


@pytest.mark.hypothesis
@settings(max_examples=20)
@given(scenario=_scenario_energie())
def test_energie_index_monotones_energies_positives(scenario):
    """Index croissants → toutes les énergies calculées sont ≥ 0 (jamais de delta négatif)."""
    jours, hp, hc = scenario
    result = pipeline_energie(_chronologie_energie(jours, hp, hc)).collect()

    assert result.height >= 1, "au moins une période entre MES et RES"
    for colonne in ["energie_base_kwh", "energie_hp_kwh", "energie_hc_kwh"]:
        serie = result[colonne].drop_nulls()
        assert (serie >= 0).all(), f"{colonne} négative avec des index monotones"


@pytest.mark.hypothesis
@settings(max_examples=20)
@given(scenario=_scenario_energie())
def test_energie_conservation_des_cadrans(scenario):
    """Compteur HP/HC : energie_base (synthèse des cadrans) = energie_hp + energie_hc, période par période."""
    jours, hp, hc = scenario
    result = pipeline_energie(_chronologie_energie(jours, hp, hc)).collect()

    for ligne in result.iter_rows(named=True):
        base, e_hp, e_hc = ligne["energie_base_kwh"], ligne["energie_hp_kwh"], ligne["energie_hc_kwh"]
        if base is not None and e_hp is not None and e_hc is not None:
            assert math.isclose(base, e_hp + e_hc, rel_tol=1e-9, abs_tol=1e-6)


@pytest.mark.hypothesis
@settings(max_examples=20)
@given(scenario=_scenario_energie())
def test_energie_periodes_dans_les_bornes_du_contrat(scenario):
    """Aucune période d'énergie hors [MES, RES]."""
    jours, hp, hc = scenario
    result = pipeline_energie(_chronologie_energie(jours, hp, hc)).collect()

    assert (result["debut"] >= _ts(jours[0])).all()
    assert (result["fin"] <= _ts(jours[-1])).all()
