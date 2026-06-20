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
un seul PDL, un fragment de **spine** (faits d'une RSC) aux dates ordonnées et distinctes
(jours civils), MES → … → RES. Depuis ADR-0041 (#378) `pipeline_historique` consomme la
spine ; l'enrichissement ajoute `impacte_abonnement` / `resume_modification` que le schéma
`Historique` valide en sortie.
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

# Bornes des dates générées : on garde un horizon qui peut tomber avant, dans, ou
# après la séquence d'événements pour exercer le filtrage.
_JOUR_MIN = dt.date(2024, 1, 2)
_JOUR_MAX = dt.date(2026, 12, 1)


def _ts(jour: dt.date) -> dt.datetime:
    return dt.datetime(jour.year, jour.month, jour.day, tzinfo=_TZ)


def _spine_evenements(jours: list[dt.date], puissances: list[float]) -> pl.LazyFrame:
    """Fragment de spine mono-RSC (événements réels) : MES → MCT… → RES (ADR-0041, #378).

    Depuis la descente d'ADR-0041, `pipeline_historique` consomme la spine et ne fait que
    filtrer l'horizon + détecter les ruptures d'abonnement : aucune colonne d'index/calendrier
    (branche énergie, hors spine). Les colonnes non-nullables du schéma `Historique` (segment,
    état, type d'événement, compteur) portent des valeurs constantes ; l'enrichissement dérive
    `avant_*` / `impacte_abonnement` / `resume_modification`.
    """
    n = len(jours)
    evenements = ["MES"] + ["MCT"] * (n - 2) + ["RES"]
    data = {
        "date_evenement": [_ts(j) for j in jours],
        "pdl": [_PDL] * n,
        "ref_situation_contractuelle": [_RSC] * n,
        "source": ["flux_C15"] * n,
        "type_fait": ["evenement"] * n,
        "segment_clientele": ["C5"] * n,
        "etat_contractuel": ["EN SERVICE"] * (n - 1) + ["RESILIE"],
        "evenement_declencheur": evenements,
        "type_evenement": ["contractuel"] * n,
        "puissance_souscrite_kva": puissances,
        "formule_tarifaire_acheminement": [_FTA] * n,
        "type_compteur": ["CCB"] * n,
        "num_compteur": ["COMPTEUR1"] * n,
    }
    overrides = {
        "date_evenement": pl.Datetime("us", "Europe/Paris"),
        "puissance_souscrite_kva": pl.Float64,
    }
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
    historique = _spine_evenements(jours, puissances)

    premier = pipeline_historique(historique, horizon).collect()
    second = pipeline_historique(historique, horizon).collect()

    assert premier.equals(second)


@pytest.mark.hypothesis
@settings(max_examples=25)
@given(scenario=_scenario_avec_horizon())
def test_historique_aucun_evenement_apres_horizon(scenario):
    """Borne haute : aucun fait retenu après l'horizon (filtre `date_evenement <= horizon`)."""
    jours, puissances, horizon = scenario
    result = pipeline_historique(_spine_evenements(jours, puissances), horizon).collect()

    if result.height:
        assert (result["date_evenement"] <= horizon).all()


@pytest.mark.hypothesis
@settings(max_examples=25)
@given(scenario=_scenario_avec_deux_horizons())
def test_historique_monotonie_horizon(scenario):
    """Reculer l'horizon ne fait que *retirer* des faits : jamais ajouter ni modifier.

    `pipeline_historique` ne génère rien (la grille FACTURATION est portée par la spine,
    #375) : seul le filtre `date_evenement <= horizon` agit. Reculer l'horizon produit donc
    un sous-ensemble strict, ligne pour ligne identique.
    """
    jours, puissances, horizon_tot, horizon_tard = scenario
    historique = _spine_evenements(jours, puissances)

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
    (tous les événements réels survivent) : la validation porte alors sur un frame
    réellement enrichi (impacte_abonnement, resume_modification, avant_*).
    """
    jours, puissances = scenario
    horizon = dt.datetime(jours[-1].year, jours[-1].month, 1, tzinfo=_TZ) + dt.timedelta(days=40)
    horizon = dt.datetime(horizon.year, horizon.month, 1, tzinfo=_TZ)

    result = pipeline_historique(_spine_evenements(jours, puissances), horizon).collect()

    assert result.height >= 1, "horizon postérieur aux événements → sortie non vide"
    # `@pa.check_types` valide déjà au collect ; on revalide explicitement pour que
    # l'invariant échoue ici (et non dans un détail interne) si le contrat casse.
    Historique.validate(result, lazy=True)
