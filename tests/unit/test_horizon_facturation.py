"""Tests de l'horizon de facturation du `pipeline_historique` (issue #179, ADR-0041 #378).

Depuis ADR-0041, `pipeline_historique` consomme la **spine** (mart `spine_contrat`, déjà
forward-fillée + grille FACTURATION générée en dbt, #375). L'horizon n'est plus un *input de
génération* mais un simple **filtre** : `date_evenement <= horizon`. La génération de la grille
FACTURATION (et son comportement de bord de mois en vraie heure de Paris) est désormais testée
côté dbt (`tests/ingestion/test_dbt_spine_contrat.py`).

Ces tests verrouillent le comportement déterministe du filtre : à horizon fixé, la sortie ne
dépend plus de l'heure d'exécution.
"""

import datetime as dt
from zoneinfo import ZoneInfo

import polars as pl

from electricore.core.pipelines.historique import pipeline_historique

PARIS = ZoneInfo("Europe/Paris")


def spine_brut(faits: list[dict]) -> pl.LazyFrame:
    """Construit une spine (forme du mart `spine_contrat`) à partir d'une liste de faits.

    Chaque dict porte au moins `ref`, `pdl`, `date` (datetime *naïf*, interprété Europe/Paris),
    `evenement`, `puissance`, `fta`. Pour une borne FACTURATION, `puissance`/`fta` valent la
    situation forward-fillée (comme le ferait le forward-fill SQL de la spine). Pas de colonne
    d'index/calendrier (branche énergie, hors spine)."""

    def _source(e: dict) -> str:
        return "synthese_mensuelle" if e["evenement"] == "FACTURATION" else "flux_C15"

    def _type_fait(e: dict) -> str:
        return "facturation" if e["evenement"] == "FACTURATION" else "evenement"

    def _type_evenement(e: dict) -> str:
        return e.get("type_evenement", "artificiel" if e["evenement"] == "FACTURATION" else "reel")

    data = {
        "date_evenement": [e["date"] for e in faits],
        "pdl": [e["pdl"] for e in faits],
        "ref_situation_contractuelle": [e["ref"] for e in faits],
        "source": [_source(e) for e in faits],
        "type_fait": [_type_fait(e) for e in faits],
        "evenement_declencheur": [e["evenement"] for e in faits],
        "type_evenement": [_type_evenement(e) for e in faits],
        "etat_contractuel": [e.get("etat", "EN SERVICE") for e in faits],
        "segment_clientele": [e.get("segment", "C5") for e in faits],
        "puissance_souscrite_kva": [e["puissance"] for e in faits],
        "formule_tarifaire_acheminement": [e["fta"] for e in faits],
        "type_compteur": [e.get("type_compteur", "LINKY") for e in faits],
        "num_compteur": [e.get("num_compteur", "C0000001") for e in faits],
    }
    return pl.LazyFrame(data).with_columns(pl.col("date_evenement").dt.replace_time_zone("Europe/Paris"))


def _pdl_ouvert_avec_grille() -> pl.LazyFrame:
    """Un PDL entré le 15/01/2024, jamais résilié, avec la grille FACTURATION pré-générée
    (fév→juin) telle que la spine la matérialise jusqu'à une borne généreuse."""
    base = {"ref": "REF001", "pdl": "PDL00001", "puissance": 6.0, "fta": "BTINFCUST"}
    faits = [{**base, "date": dt.datetime(2024, 1, 15, 9, 0, 0), "evenement": "MES"}]
    faits += [{**base, "date": dt.datetime(2024, mois, 1), "evenement": "FACTURATION"} for mois in (2, 3, 4, 5, 6)]
    return spine_brut(faits)


def test_horizon_filtre_les_bornes_facturation():
    """L'horizon filtre les bornes FACTURATION pré-générées de la spine : avec horizon =
    2024-04-01, seules les FACTURATION ≤ horizon survivent (fév, mars, avril)."""
    horizon = dt.datetime(2024, 4, 1, tzinfo=PARIS)

    resultat = pipeline_historique(_pdl_ouvert_avec_grille(), horizon=horizon).collect()

    facturation = resultat.filter(pl.col("evenement_declencheur") == "FACTURATION").sort("date_evenement")
    dates = facturation["date_evenement"].dt.strftime("%Y-%m-%d").to_list()

    assert dates == ["2024-02-01", "2024-03-01", "2024-04-01"]


def test_horizon_borne_les_evenements_reels():
    """Aucun événement réel postérieur à l'horizon n'est retenu.

    Trois événements réels (MES jan, MCT mars, MCT mai). Avec horizon = 1er avril,
    seuls les deux premiers (≤ horizon) survivent ; le MCT de mai est écarté.
    """
    horizon = dt.datetime(2024, 4, 1, tzinfo=PARIS)

    spine = spine_brut(
        [
            {
                "ref": "REF001",
                "pdl": "PDL00001",
                "date": dt.datetime(2024, 1, 15),
                "evenement": "MES",
                "puissance": 6.0,
                "fta": "BTINFCUST",
            },
            {
                "ref": "REF001",
                "pdl": "PDL00001",
                "date": dt.datetime(2024, 3, 10),
                "evenement": "MCT",
                "puissance": 9.0,
                "fta": "BTINFCUST",
            },
            {
                "ref": "REF001",
                "pdl": "PDL00001",
                "date": dt.datetime(2024, 5, 20),
                "evenement": "MCT",
                "puissance": 12.0,
                "fta": "BTINFCUST",
            },
        ]
    )

    resultat = pipeline_historique(spine, horizon=horizon).collect()

    reels = resultat.filter(pl.col("evenement_declencheur") != "FACTURATION")
    dates_reels = reels["date_evenement"].dt.strftime("%Y-%m-%d").sort().to_list()

    assert dates_reels == ["2024-01-15", "2024-03-10"]
    # Aucun événement (réel ou artificiel) ne dépasse l'horizon.
    assert resultat["date_evenement"].max() <= horizon


def _empreinte_evenements(resultat: pl.DataFrame) -> set[tuple[str, str, str]]:
    """Empreinte (ref, date, événement) d'un résultat de pipeline, pour comparaison d'ensembles."""
    rows = resultat.select(
        "ref_situation_contractuelle",
        pl.col("date_evenement").dt.strftime("%Y-%m-%dT%H:%M:%S").alias("d"),
        "evenement_declencheur",
    ).to_dicts()
    return {(r["ref_situation_contractuelle"], r["d"], r["evenement_declencheur"]) for r in rows}


def test_horizon_monotone_recule_seulement_retire():
    """Reculer l'horizon ne fait que *retirer* des faits — jamais ajouter ni modifier.

    On compare l'empreinte (ref, date, événement) à horizon tardif et à horizon plus
    tôt : l'ensemble du second doit être strictement inclus dans celui du premier.
    """
    base = {"ref": "REF001", "pdl": "PDL00001", "fta": "BTINFCUST"}
    spine = spine_brut(
        [
            {**base, "date": dt.datetime(2024, 1, 15), "evenement": "MES", "puissance": 6.0},
            {**base, "date": dt.datetime(2024, 4, 10), "evenement": "MCT", "puissance": 9.0},
            # Grille FACTURATION pré-générée (situation forward-fillée : 6 kVA jusqu'à avril exclus)
            {**base, "date": dt.datetime(2024, 2, 1), "evenement": "FACTURATION", "puissance": 6.0},
            {**base, "date": dt.datetime(2024, 3, 1), "evenement": "FACTURATION", "puissance": 6.0},
            {**base, "date": dt.datetime(2024, 4, 1), "evenement": "FACTURATION", "puissance": 6.0},
            {**base, "date": dt.datetime(2024, 5, 1), "evenement": "FACTURATION", "puissance": 9.0},
            {**base, "date": dt.datetime(2024, 6, 1), "evenement": "FACTURATION", "puissance": 9.0},
        ]
    )

    tardif = _empreinte_evenements(pipeline_historique(spine, horizon=dt.datetime(2024, 6, 1, tzinfo=PARIS)).collect())
    tot = _empreinte_evenements(pipeline_historique(spine, horizon=dt.datetime(2024, 3, 1, tzinfo=PARIS)).collect())

    # Tout ce que produit l'horizon précoce existe à l'identique dans l'horizon tardif.
    assert tot <= tardif
    # Et l'horizon tardif en produit strictement plus (FACTURATION avril–juin, MCT avril).
    assert tot < tardif


def test_sortie_conforme_au_schema_historique():
    """La sortie enrichie (avec horizon explicite) reste conforme au schéma `Historique`.

    `pipeline_historique` est décoré `@pa.check_types` : un `.collect()` qui réussit *est*
    la validation Pandera (sortie conforme à `Historique`). On vérifie en plus la présence
    des colonnes d'enrichissement abonnement (plus d'`impacte_energie`) et des FACTURATION.
    """
    resultat = pipeline_historique(_pdl_ouvert_avec_grille(), horizon=dt.datetime(2024, 4, 1, tzinfo=PARIS)).collect()

    for colonne in ("impacte_abonnement", "resume_modification", "type_evenement"):
        assert colonne in resultat.columns
    assert "impacte_energie" not in resultat.columns

    # Les bornes FACTURATION de la spine sont retenues (≤ horizon) et portent la situation.
    facturation = resultat.filter(pl.col("evenement_declencheur") == "FACTURATION")
    assert facturation.height == 3
    assert (facturation["type_evenement"] == "artificiel").all()
    assert (facturation["puissance_souscrite_kva"] == 6.0).all()
