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
        overrides[f"avant_index_{cadran}_kwh"] = pl.Float64
        overrides[f"apres_index_{cadran}_kwh"] = pl.Float64
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
    hp0 = draw(st.floats(0.0, 1e5, allow_nan=False))
    hc0 = draw(st.floats(0.0, 1e5, allow_nan=False))
    increments = draw(
        st.lists(
            st.tuples(st.floats(0.0, 500.0, allow_nan=False), st.floats(0.0, 500.0, allow_nan=False)),
            min_size=len(jours) - 1,
            max_size=len(jours) - 1,
        )
    )
    hp = list(accumulate([hp0] + [i[0] for i in increments]))
    hc = list(accumulate([hc0] + [i[1] for i in increments]))
    return jours, hp, hc


def _historique_energie(jours: list[dt.date], hp: list[float], hc: list[float]) -> pl.LazyFrame:
    """MES (index après) → FACTURATION (relevés R151) → RES (index avant)."""
    n = len(jours)
    evenements = ["MES"] + ["FACTURATION"] * (n - 2) + ["RES"]
    data = {
        "date_evenement": [_ts(j) for j in jours],
        "evenement_declencheur": evenements,
        "etat_contractuel": ["EN SERVICE"] * (n - 1) + ["RESILIE"],
        "puissance_souscrite_kva": [6.0] * n,
        "impacte_abonnement": [False] * n,
        "impacte_energie": [True] * n,
        **_colonnes_historique(n),
    }
    nuls = [None] * n
    for prefixe, position, idx in (("apres", 0, 0), ("avant", n - 1, -1)):
        data[f"{prefixe}_id_calendrier_distributeur"] = ["DI000002" if i == position else None for i in range(n)]
        # Calendrier HP/HC (DI000002) : pas d'index base brut — l'energie_base de
        # sortie est synthétisée par le pipeline comme somme des cadrans
        valeurs = {"hp": hp[idx], "hc": hc[idx]}
        for cadran in _INDEX_CADRANS:
            data[f"{prefixe}_index_{cadran}_kwh"] = [valeurs.get(cadran) if i == position else None for i in range(n)]
    autre = {"avant": "apres", "apres": "avant"}
    for prefixe in ("avant", "apres"):
        for cadran in _INDEX_CADRANS:
            data.setdefault(f"{prefixe}_index_{cadran}_kwh", nuls)
        data.setdefault(f"{prefixe}_id_calendrier_distributeur", nuls)
    del autre
    return pl.DataFrame(data, schema_overrides=_schema_overrides()).lazy()


def _releves_r151(jours: list[dt.date], hp: list[float], hc: list[float]) -> pl.LazyFrame:
    """Relevés R151 aux dates intermédiaires (celles des événements FACTURATION)."""
    interieurs = list(range(1, len(jours) - 1))
    data = {
        "pdl": [_PDL] * len(interieurs),
        "ref_situation_contractuelle": [_RSC] * len(interieurs),
        "formule_tarifaire_acheminement": [_FTA] * len(interieurs),
        "date_releve": [_ts(jours[i]) for i in interieurs],
        "source": ["flux_R151"] * len(interieurs),
        "unite": ["kWh"] * len(interieurs),
        "precision": ["kWh"] * len(interieurs),
        "ordre_index": [False] * len(interieurs),
        "id_calendrier_distributeur": ["DI000002"] * len(interieurs),
        "index_base_kwh": [None] * len(interieurs),
        "index_hp_kwh": [hp[i] for i in interieurs],
        "index_hc_kwh": [hc[i] for i in interieurs],
        "index_hch_kwh": [None] * len(interieurs),
        "index_hph_kwh": [None] * len(interieurs),
        "index_hpb_kwh": [None] * len(interieurs),
        "index_hcb_kwh": [None] * len(interieurs),
    }
    return pl.DataFrame(
        data,
        schema_overrides={
            "date_releve": pl.Datetime("us", "Europe/Paris"),
            **{f"index_{c}_kwh": pl.Float64 for c in _INDEX_CADRANS},
            # dtypes explicites : un scénario MES → RES sans relevé intermédiaire
            # produit un frame vide, dont les colonnes seraient sinon typées Null
            **{
                col: pl.Utf8
                for col in [
                    "pdl",
                    "ref_situation_contractuelle",
                    "formule_tarifaire_acheminement",
                    "source",
                    "unite",
                    "precision",
                    "id_calendrier_distributeur",
                ]
            },
            "ordre_index": pl.Boolean,
        },
    ).lazy()


@pytest.mark.hypothesis
@settings(max_examples=20)
@given(scenario=_scenario_energie())
def test_energie_index_monotones_energies_positives(scenario):
    """Index croissants → toutes les énergies calculées sont ≥ 0 (jamais de delta négatif)."""
    jours, hp, hc = scenario
    result = pipeline_energie(_historique_energie(jours, hp, hc), _releves_r151(jours, hp, hc)).collect()

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
    result = pipeline_energie(_historique_energie(jours, hp, hc), _releves_r151(jours, hp, hc)).collect()

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
    result = pipeline_energie(_historique_energie(jours, hp, hc), _releves_r151(jours, hp, hc)).collect()

    assert (result["debut"] >= _ts(jours[0])).all()
    assert (result["fin"] <= _ts(jours[-1])).all()
