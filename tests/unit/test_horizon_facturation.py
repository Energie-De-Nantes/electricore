"""Tests de l'horizon de facturation du `pipeline_historique` (issue #179).

L'horizon est un `datetime` unique en Europe/Paris qui borne *à la fois* les
événements retenus *et* la fin par défaut des périodes ouvertes (génération des
événements FACTURATION). Avant #179, le pipeline lisait l'horloge à deux endroits
avec deux sémantiques de fuseau divergentes (heure murale UTC renommée Paris) →
décalage permanent de 1–2 h et incohérence possible entre les événements retenus
et les FACTURATION générés autour de minuit le 1er du mois.

Ces tests verrouillent le comportement déterministe : à horizon fixé, la sortie ne
dépend plus de l'heure d'exécution.
"""

import datetime as dt
from zoneinfo import ZoneInfo

import polars as pl

from electricore.core.models.cadrans import CADRANS, col_index
from electricore.core.pipelines.historique import pipeline_historique

PARIS = ZoneInfo("Europe/Paris")


def historique_brut(evenements: list[dict]) -> pl.LazyFrame:
    """Construit un historique C15 brut complet à partir d'une liste d'événements.

    Chaque dict porte au moins `ref`, `pdl`, `date` (datetime *naïf*, interprété
    Europe/Paris), `evenement`, `puissance`, `fta`. Les colonnes `avant_/apres_`
    (calendrier distributeur + index par cadran) exigées par
    `detecter_points_de_rupture` sont renseignées à `None` (cas nominal sans
    rupture d'index), et les colonnes non-nullables du schéma `Historique` sont
    fournies avec des valeurs plausibles.
    """
    n = len(evenements)
    data: dict[str, list] = {
        "pdl": [e["pdl"] for e in evenements],
        "ref_situation_contractuelle": [e["ref"] for e in evenements],
        "date_evenement": [e["date"] for e in evenements],
        "evenement_declencheur": [e["evenement"] for e in evenements],
        "type_evenement": [e.get("type_evenement", "reel") for e in evenements],
        "etat_contractuel": [e.get("etat", "EN SERVICE") for e in evenements],
        "segment_clientele": [e.get("segment", "C5") for e in evenements],
        "puissance_souscrite_kva": [e["puissance"] for e in evenements],
        "formule_tarifaire_acheminement": [e["fta"] for e in evenements],
        "type_compteur": [e.get("type_compteur", "LINKY") for e in evenements],
        "num_compteur": [e.get("num_compteur", "C0000001") for e in evenements],
        # Colonnes de calendrier distributeur (avant/après) requises par l'enrichissement.
        "avant_id_calendrier_distributeur": [None] * n,
        "apres_id_calendrier_distributeur": [None] * n,
    }
    # Index avant/après par cadran (nullable Int64 — kWh entiers, ADR-0034/0035).
    schema_overrides: dict[str, pl.DataType] = {
        "avant_id_calendrier_distributeur": pl.Utf8,
        "apres_id_calendrier_distributeur": pl.Utf8,
    }
    for cadran in CADRANS:
        data[f"avant_{col_index(cadran)}"] = [None] * n
        data[f"apres_{col_index(cadran)}"] = [None] * n
        schema_overrides[f"avant_{col_index(cadran)}"] = pl.Int64
        schema_overrides[f"apres_{col_index(cadran)}"] = pl.Int64

    return pl.LazyFrame(data, schema_overrides=schema_overrides).with_columns(
        pl.col("date_evenement").dt.replace_time_zone("Europe/Paris")
    )


def _pdl_ouvert() -> pl.LazyFrame:
    """Un PDL entré en 2024 et jamais résilié (période ouverte → fin dépend de l'horizon)."""
    return historique_brut(
        [
            {
                "ref": "REF001",
                "pdl": "PDL00001",
                "date": dt.datetime(2024, 1, 15, 9, 0, 0),
                "evenement": "MES",
                "puissance": 6.0,
                "fta": "BTINFCUST",
            }
        ]
    )


def test_horizon_explicite_borne_facturation_independamment_de_l_horloge():
    """Avec un horizon explicite dans le passé, la dernière FACTURATION = l'horizon.

    Le PDL est ouvert (pas de RES) ; sans horizon, le pipeline générerait des
    FACTURATION jusqu'à aujourd'hui (2026). Avec horizon = 2024-04-01, la dernière
    FACTURATION doit être 2024-04-01 — preuve que la sortie est gouvernée par
    l'horizon explicite, pas par l'horloge murale.
    """
    horizon = dt.datetime(2024, 4, 1, tzinfo=PARIS)

    resultat = pipeline_historique(_pdl_ouvert(), horizon=horizon).collect()

    facturation = resultat.filter(pl.col("evenement_declencheur") == "FACTURATION").sort("date_evenement")
    dates = facturation["date_evenement"].dt.strftime("%Y-%m-%d").to_list()

    # MES le 15/01 → premier mois facturé = février ; horizon 1er avril → dernier = avril.
    assert dates == ["2024-02-01", "2024-03-01", "2024-04-01"]


def test_horizon_borne_les_evenements_reels():
    """Aucun événement réel postérieur à l'horizon n'est retenu.

    Trois événements réels (MES jan, MCT mars, MCT mai). Avec horizon = 1er avril,
    seuls les deux premiers (≤ horizon) survivent ; le MCT de mai est écarté.
    """
    horizon = dt.datetime(2024, 4, 1, tzinfo=PARIS)

    historique = historique_brut(
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

    resultat = pipeline_historique(historique, horizon=horizon).collect()

    reels = resultat.filter(pl.col("evenement_declencheur") != "FACTURATION")
    dates_reels = reels["date_evenement"].dt.strftime("%Y-%m-%d").sort().to_list()

    assert dates_reels == ["2024-01-15", "2024-03-10"]
    # Aucun événement (réel ou artificiel) ne dépasse l'horizon.
    assert resultat["date_evenement"].max() <= horizon


def test_fin_periode_ouverte_utilise_le_mois_de_l_horizon_en_vraie_heure_paris():
    """Le bug à deux fuseaux (#179) est corrigé : un horizon juste après minuit
    Paris le 1er du mois facture bien *ce* mois-là.

    Horizon = 2024-03-01 00:30 Paris (UTC+01). L'ancien code lisait l'heure murale
    UTC (2024-02-29 23:30) puis la *renommait* Paris → fin de période = février, donc
    la dernière FACTURATION d'un PDL ouvert se serait arrêtée à février. Avec
    l'horizon en vraie heure de Paris, le mois de l'horizon est mars : la facturation
    doit atteindre mars.
    """
    horizon = dt.datetime(2024, 3, 1, 0, 30, tzinfo=PARIS)

    resultat = pipeline_historique(_pdl_ouvert(), horizon=horizon).collect()

    facturation = resultat.filter(pl.col("evenement_declencheur") == "FACTURATION").sort("date_evenement")
    dates = facturation["date_evenement"].dt.strftime("%Y-%m-%d").to_list()

    # MES 15/01 → premier mois facturé février ; mois de l'horizon = mars (pas février).
    assert dates == ["2024-02-01", "2024-03-01"]
    assert "2024-03-01" in dates  # explicite : le mois de l'horizon est facturé


def _dates_evenements(lf_resultat: pl.DataFrame) -> set[tuple[str, str, str]]:
    """Empreinte (ref, date, événement) d'un résultat de pipeline, pour comparaison d'ensembles."""
    rows = lf_resultat.select(
        "ref_situation_contractuelle",
        pl.col("date_evenement").dt.strftime("%Y-%m-%dT%H:%M:%S").alias("d"),
        "evenement_declencheur",
    ).to_dicts()
    return {(r["ref_situation_contractuelle"], r["d"], r["evenement_declencheur"]) for r in rows}


def test_horizon_monotone_recule_seulement_retire():
    """Reculer l'horizon ne fait que *retirer* des événements — jamais ajouter ni modifier.

    On compare l'empreinte (ref, date, événement) à horizon tardif et à horizon plus
    tôt : l'ensemble du second doit être strictement inclus dans celui du premier.
    """
    historique = historique_brut(
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
                "date": dt.datetime(2024, 4, 10),
                "evenement": "MCT",
                "puissance": 9.0,
                "fta": "BTINFCUST",
            },
        ]
    )

    tardif = _dates_evenements(pipeline_historique(historique, horizon=dt.datetime(2024, 6, 1, tzinfo=PARIS)).collect())
    tot = _dates_evenements(pipeline_historique(historique, horizon=dt.datetime(2024, 3, 1, tzinfo=PARIS)).collect())

    # Tout ce que produit l'horizon précoce existe à l'identique dans l'horizon tardif.
    assert tot <= tardif
    # Et l'horizon tardif en produit strictement plus (FACTURATION avril–juin, MCT avril).
    assert tot < tardif


def test_sortie_conforme_au_schema_historique():
    """La sortie enrichie (avec horizon explicite) reste conforme au schéma `Historique`.

    `pipeline_historique` est décoré `@pa.check_types` : un `.collect()` qui réussit
    *est* la validation Pandera (sortie conforme à `Historique`). On vérifie en plus
    la présence des colonnes d'enrichissement et des FACTURATION artificiels.
    """
    # Le `.collect()` à travers le pipeline décoré valide la sortie contre `Historique` :
    # s'il n'échoue pas, la sortie est conforme au schéma.
    resultat = pipeline_historique(_pdl_ouvert(), horizon=dt.datetime(2024, 4, 1, tzinfo=PARIS)).collect()

    for colonne in ("impacte_abonnement", "impacte_energie", "resume_modification", "type_evenement"):
        assert colonne in resultat.columns

    # Les FACTURATION artificiels sont bien typés et propagent les données contractuelles.
    facturation = resultat.filter(pl.col("evenement_declencheur") == "FACTURATION")
    assert facturation.height == 3
    assert (facturation["type_evenement"] == "artificiel").all()
    assert (facturation["puissance_souscrite_kva"] == 6.0).all()
