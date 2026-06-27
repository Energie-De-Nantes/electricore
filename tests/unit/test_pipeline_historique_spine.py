"""Tests du `pipeline_historique` *rétréci* qui consomme la spine (ADR-0041, #378).

Après la descente d'assemblage d'ADR-0041, `pipeline_historique` ne ré-assemble plus
rien : il lit la **spine** (mart `spine_contrat`, déjà forward-fillée + grille FACTURATION
générée en dbt, #375) et ne fait que (1) **filtrer l'horizon** (#179) et (2) **détecter les
ruptures d'abonnement** (changement puissance/FTA, événement structurant). Les bornes
FACTURATION de la spine sont forcées à `impacte_abonnement=True` (elles créent des bornes de
période). L'énergie ne passe plus par ce pipeline (elle lit la *Chronologie des relevés*,
#377) — donc plus d'`impacte_energie` ni de colonnes d'index ici.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from electricore.core.pipelines.historique import pipeline_historique

PARIS = ZoneInfo("Europe/Paris")


def _spine_minimale() -> pl.LazyFrame:
    """Spine minimale d'une RSC : entrée MES (6 kVA) → borne FACTURATION (situation
    forward-fillée) → changement de puissance MCT (9 kVA). Forme du mart `spine_contrat` :
    épine + attributs de situation, SANS colonne d'index/calendrier (énergie-spécifique)."""
    return pl.LazyFrame(
        {
            "date_evenement": [
                datetime(2024, 1, 15, 0, 1, tzinfo=PARIS),
                datetime(2024, 2, 1, 0, 0, tzinfo=PARIS),
                datetime(2024, 3, 10, 0, 1, tzinfo=PARIS),
            ],
            "pdl": ["PDL00001"] * 3,
            "ref_situation_contractuelle": ["REF001"] * 3,
            "source": ["flux_C15", "synthese_mensuelle", "flux_C15"],
            "type_fait": ["evenement", "facturation", "evenement"],
            "evenement_declencheur": ["MES", "FACTURATION", "MCT"],
            "type_evenement": ["contractuel", "artificiel", "contractuel"],
            "segment_clientele": ["C5"] * 3,
            "etat_contractuel": ["EN SERVICE"] * 3,
            "puissance_souscrite_kva": [6.0, 6.0, 9.0],
            "formule_tarifaire_acheminement": ["BTINFCU4"] * 3,
            "type_compteur": ["LINKY"] * 3,
            "num_compteur": ["123456"] * 3,
            "categorie": [None, None, None],
            "ref_demandeur": [None, None, None],
            "id_affaire": [None, None, None],
            "niveau_ouverture_services": ["2", "2", "2"],
            "date_changement_niveau_ouverture_services": [None, None, None],
        },
        schema_overrides={
            "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "categorie": pl.Utf8,
            "ref_demandeur": pl.Utf8,
            "id_affaire": pl.Utf8,
            "date_changement_niveau_ouverture_services": pl.Date,
        },
    )


def test_pipeline_historique_force_facturation_a_impacter_abonnement():
    """Tracer bullet : `pipeline_historique` lit la spine et force les bornes FACTURATION
    (forward-fillées, donc « sans changement ») à `impacte_abonnement=True`, tout en
    détectant les ruptures réelles (MES structurant, MCT puissance)."""
    horizon = datetime(2024, 4, 1, tzinfo=PARIS)

    result = pipeline_historique(_spine_minimale(), horizon=horizon).collect()

    impacts = dict(zip(result["evenement_declencheur"], result["impacte_abonnement"], strict=True))
    assert impacts["FACTURATION"] is True  # borne de période, même sans changement de situation
    assert impacts["MES"] is True  # événement structurant d'entrée
    assert impacts["MCT"] is True  # changement de puissance 6 → 9


def _spine_collision_minuit_1er() -> pl.LazyFrame:
    """Spine où un **vrai** événement (MCT, 9 kVA) tombe pile au **même instant** qu'une
    borne FACTURATION (minuit le 1er) — la collision de l'issue #270.

    Les deux jumeaux sont fournis dans l'**ordre adverse** : la borne FACTURATION *avant*
    l'événement réel. C'est un ordre que la sortie non triée du mart `spine_contrat` peut
    produire (un mart dbt ne garantit pas l'ordre des lignes). Faithful au forward-fill SQL
    de la spine (départage événement-avant-facturation, `spine_contrat.sql`), les deux
    jumeaux portent déjà la situation post-changement (9.0 kVA).
    """
    return pl.LazyFrame(
        {
            "date_evenement": [
                datetime(2024, 1, 15, 0, 1, tzinfo=PARIS),  # MES entrée 6 kVA
                datetime(2024, 2, 1, 0, 0, tzinfo=PARIS),  # FACTURATION 6 kVA
                datetime(2024, 3, 1, 0, 0, tzinfo=PARIS),  # FACTURATION jumelle 9 kVA — placée AVANT
                datetime(2024, 3, 1, 0, 0, tzinfo=PARIS),  # MCT 9 kVA — collision pile minuit le 1er
            ],
            "pdl": ["PDL00001"] * 4,
            "ref_situation_contractuelle": ["REF001"] * 4,
            "source": ["flux_C15", "synthese_mensuelle", "synthese_mensuelle", "flux_C15"],
            "type_fait": ["evenement", "facturation", "facturation", "evenement"],
            "evenement_declencheur": ["MES", "FACTURATION", "FACTURATION", "MCT"],
            "type_evenement": ["contractuel", "artificiel", "artificiel", "contractuel"],
            "segment_clientele": ["C5"] * 4,
            "etat_contractuel": ["EN SERVICE"] * 4,
            "puissance_souscrite_kva": [6.0, 6.0, 9.0, 9.0],
            "formule_tarifaire_acheminement": ["BTINFCU4"] * 4,
            "type_compteur": ["LINKY"] * 4,
            "num_compteur": ["123456"] * 4,
            "categorie": [None] * 4,
            "ref_demandeur": [None] * 4,
            "id_affaire": [None] * 4,
            "niveau_ouverture_services": ["2"] * 4,
            "date_changement_niveau_ouverture_services": [None] * 4,
        },
        schema_overrides={
            "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "categorie": pl.Utf8,
            "ref_demandeur": pl.Utf8,
            "id_affaire": pl.Utf8,
            "date_changement_niveau_ouverture_services": pl.Date,
        },
    )


def test_collision_minuit_1er_attribue_le_changement_a_l_evenement_reel():
    """Régression #270 : à un instant partagé par un vrai événement (MCT) et une borne
    FACTURATION (minuit le 1er), le départage déterministe (événement **avant** facturation,
    comme le forward-fill SQL de la spine) attribue le changement 6 → 9 au **vrai** événement
    et laisse la borne FACTURATION « sans changement » — quel que soit l'ordre des lignes en
    entrée. Sans départage, la photo FACTURATION pouvait capter la valeur d'avant ou d'après
    le changement (6 vs 9 kVA) selon un ordre arbitraire."""
    horizon = datetime(2024, 4, 1, tzinfo=PARIS)

    result = pipeline_historique(_spine_collision_minuit_1er(), horizon=horizon).collect()

    collision = result.filter(pl.col("date_evenement") == datetime(2024, 3, 1, 0, 0, tzinfo=PARIS))
    mct = collision.filter(pl.col("evenement_declencheur") == "MCT").row(0, named=True)
    facturation = collision.filter(pl.col("evenement_declencheur") == "FACTURATION").row(0, named=True)

    # Le vrai événement porte le changement de puissance (6 → 9)
    assert mct["avant_puissance_souscrite"] == 6.0
    assert mct["resume_modification"] == "P: 6.0 → 9.0"

    # La borne FACTURATION jumelle voit déjà l'état post-changement → aucun changement
    assert facturation["avant_puissance_souscrite"] == 9.0
    assert facturation["resume_modification"] == ""
