"""
Property tests des invariants de `pipeline_historique` (issue #198).

Le pipeline est devenu **pur** (issue #179 / PR #235) : son unique source de temps
est l'`horizon` passé en argument — il ne lit plus l'horloge murale. Cette pureté
rend les invariants suivants vérifiables par génération :

1. déterminisme : à horizon fixé, deux exécutions donnent une sortie identique,
   indépendamment de l'heure murale ;
2. borne haute : aucun événement retenu après l'horizon ;
3. monotonie de l'horizon : reculer l'horizon ne peut que *retirer* des événements,
   jamais en ajouter ni en modifier ;
4. contrat de sortie : l'enrichissement satisfait le schéma Pandera `Historique`.

Stratégie d'entrée **volontairement minimale** (garde-fou du socle, cf. tests/README.md) :
un seul PDL, une séquence d'événements C15 bruts aux dates ordonnées et distinctes
(jours civils), MES → … → RES. Pas de cohérence de calendrier/index : ce serait le
territoire des fixtures + snapshots. Les colonnes de l'entrée brute (sous-ensemble du
schéma `Historique`) portent leurs dtypes via `schema_overrides` ; l'enrichissement
ajoute les colonnes `impacte_*` / `resume_modification` que le schéma valide en sortie.
"""

import datetime as dt
from zoneinfo import ZoneInfo

import hypothesis.strategies as st
import polars as pl
import pytest
from hypothesis import given, settings

from electricore.core.models.historique import Historique
from electricore.core.pipelines.historique import pipeline_historique

_TZ = ZoneInfo("Europe/Paris")
_PDL = "PDL00000000001"
_RSC = "RSC1"
_FTA = "BTINFCUST"

# Cadrans d'index portés par les colonnes avant_/apres_ d'un événement C15.
# Présents (à None) car le pipeline lit `avant_id_calendrier_distributeur` et les
# `avant_index_*` / `apres_index_*` pour détecter les ruptures d'énergie.
_INDEX_CADRANS = ["base", "hp", "hc", "hch", "hph", "hpb", "hcb"]

# Bornes des dates générées : on garde un horizon qui peut tomber avant, dans, ou
# après la séquence d'événements pour exercer le filtrage.
_JOUR_MIN = dt.date(2024, 1, 2)
_JOUR_MAX = dt.date(2026, 12, 1)


def _ts(jour: dt.date) -> dt.datetime:
    return dt.datetime(jour.year, jour.month, jour.day, tzinfo=_TZ)


def _historique_brut(jours: list[dt.date], puissances: list[float]) -> pl.LazyFrame:
    """Historique C15 brut mono-PDL : MES → MCT… → RES, colonnes minimales du contrat.

    Volontairement plat : aucune colonne de relevé avant/après, aucun changement de
    calendrier. Les colonnes non-nullables du schéma `Historique` (segment, état,
    type d'événement, compteur) portent des valeurs constantes ; l'enrichissement
    dérive les colonnes `impacte_*` / `resume_modification`.
    """
    n = len(jours)
    evenements = ["MES"] + ["MCT"] * (n - 2) + ["RES"]
    data = {
        "date_evenement": [_ts(j) for j in jours],
        "pdl": [_PDL] * n,
        "ref_situation_contractuelle": [_RSC] * n,
        "segment_clientele": ["C5"] * n,
        "etat_contractuel": ["EN SERVICE"] * (n - 1) + ["RESILIE"],
        "evenement_declencheur": evenements,
        "type_evenement": ["contractuel"] * n,
        "puissance_souscrite_kva": puissances,
        "formule_tarifaire_acheminement": [_FTA] * n,
        "type_compteur": ["CCB"] * n,
        "num_compteur": ["COMPTEUR1"] * n,
    }
    # Colonnes de relevé avant/après (à None) : lues par la détection de rupture
    # d'énergie *et* par les dataframe-checks du schéma Historique (cohérence des
    # dates, présence des mesures selon le calendrier). Toutes nulles → checks
    # satisfaits trivialement, frame restant volontairement plat.
    data["avant_date_releve"] = [None] * n
    data["apres_date_releve"] = [None] * n
    data["avant_nature_index"] = [None] * n
    data["apres_nature_index"] = [None] * n
    for prefixe in ("avant", "apres"):
        data[f"{prefixe}_id_calendrier_distributeur"] = [None] * n
        for cadran in _INDEX_CADRANS:
            data[f"{prefixe}_index_{cadran}_kwh"] = [None] * n

    overrides = {
        "date_evenement": pl.Datetime("us", "Europe/Paris"),
        "puissance_souscrite_kva": pl.Float64,
        "avant_date_releve": pl.Datetime("us", "Europe/Paris"),
        "apres_date_releve": pl.Datetime("us", "Europe/Paris"),
        "avant_nature_index": pl.Utf8,
        "apres_nature_index": pl.Utf8,
    }
    for prefixe in ("avant", "apres"):
        overrides[f"{prefixe}_id_calendrier_distributeur"] = pl.Utf8
        for cadran in _INDEX_CADRANS:
            overrides[f"{prefixe}_index_{cadran}_kwh"] = pl.Float64

    return pl.DataFrame(data, schema_overrides=overrides).lazy()


@st.composite
def _scenario(draw):
    """Une séquence MES → MCT… → RES sur un PDL : jours distincts ordonnés + puissances."""
    jours = sorted(draw(st.lists(st.dates(_JOUR_MIN, _JOUR_MAX), min_size=2, max_size=6, unique=True)))
    puissances = draw(st.lists(st.floats(1.0, 36.0, allow_nan=False), min_size=len(jours), max_size=len(jours)))
    return jours, puissances


def _horizon_strategy() -> st.SearchStrategy[dt.datetime]:
    """Horizon = 1er d'un mois, couvrant avant/pendant/après les séquences générées."""
    return st.dates(dt.date(2023, 12, 1), dt.date(2027, 6, 1)).map(
        lambda d: dt.datetime(d.year, d.month, 1, tzinfo=_TZ)
    )


@st.composite
def _scenario_avec_horizon(draw):
    """Un scénario + un horizon (1er d'un mois) couvrant avant/pendant/après la séquence."""
    jours, puissances = draw(_scenario())
    horizon = draw(_horizon_strategy())
    return jours, puissances, horizon


@st.composite
def _scenario_avec_deux_horizons(draw):
    """Un scénario + deux horizons ordonnés (tot ≤ tard), 1ers de mois."""
    jours, puissances = draw(_scenario())
    h1 = draw(_horizon_strategy())
    h2 = draw(_horizon_strategy())
    tot, tard = sorted([h1, h2])
    return jours, puissances, tot, tard


@pytest.mark.hypothesis
@settings(max_examples=20)
@given(scenario=_scenario_avec_horizon())
def test_historique_deterministe(scenario):
    """À horizon fixé, deux exécutions donnent une sortie identique (aucune lecture d'horloge)."""
    jours, puissances, horizon = scenario
    historique = _historique_brut(jours, puissances)

    premier = pipeline_historique(historique, horizon).collect()
    second = pipeline_historique(historique, horizon).collect()

    assert premier.equals(second)


@pytest.mark.hypothesis
@settings(max_examples=25)
@given(scenario=_scenario_avec_horizon())
def test_historique_aucun_evenement_apres_horizon(scenario):
    """Borne haute : aucun événement retenu (réel ou FACTURATION) après l'horizon."""
    jours, puissances, horizon = scenario
    result = pipeline_historique(_historique_brut(jours, puissances), horizon).collect()

    if result.height:
        assert (result["date_evenement"] <= horizon).all()


@pytest.mark.hypothesis
@settings(max_examples=25)
@given(scenario=_scenario_avec_deux_horizons())
def test_historique_monotonie_horizon(scenario):
    """Reculer l'horizon ne fait que *retirer* des événements : jamais ajouter ni modifier.

    Le scénario se termine toujours par une RES, donc la période est close et la
    génération des FACTURATION ne dépend pas de l'horizon (sa fin par défaut ne sert
    qu'aux contrats ouverts). Seul le filtre final `<= horizon` agit : reculer
    l'horizon est un sous-ensemble strict, ligne pour ligne identique.
    """
    jours, puissances, horizon_tot, horizon_tard = scenario
    historique = _historique_brut(jours, puissances)

    sortie_tot = pipeline_historique(historique, horizon_tot).collect().sort("date_evenement")
    sortie_tard = pipeline_historique(historique, horizon_tard).collect().sort("date_evenement")

    # Sous-ensemble : la sortie de l'horizon tardif, tronquée à l'horizon tôt, doit
    # coïncider exactement avec la sortie de l'horizon tôt (mêmes lignes, non modifiées).
    tard_tronque = sortie_tard.filter(pl.col("date_evenement") <= horizon_tot).sort("date_evenement")
    assert sortie_tot.equals(tard_tronque)


@pytest.mark.hypothesis
@settings(max_examples=25)
@given(scenario=_scenario())
def test_historique_respecte_le_schema_pandera(scenario):
    """L'enrichissement satisfait le contrat de sortie `Historique` (validation explicite).

    Horizon = mois suivant le dernier événement, pour garantir une sortie non vide
    (tous les événements réels survivent + au moins un FACTURATION) : la validation
    porte alors sur un frame réellement enrichi (impacte_*, resume_modification, avant_*).
    """
    jours, puissances = scenario
    horizon = dt.datetime(jours[-1].year, jours[-1].month, 1, tzinfo=_TZ) + dt.timedelta(days=40)
    horizon = dt.datetime(horizon.year, horizon.month, 1, tzinfo=_TZ)

    result = pipeline_historique(_historique_brut(jours, puissances), horizon).collect()

    assert result.height >= 1, "horizon postérieur aux événements → sortie non vide"
    # `@pa.check_types` valide déjà au collect ; on revalide explicitement pour que
    # l'invariant échoue ici (et non dans un détail interne) si le contrat casse.
    Historique.validate(result, lazy=True)
