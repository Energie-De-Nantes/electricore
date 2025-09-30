"""
Tests de non-régression avec snapshots pour les pipelines principaux.

Ces tests capturent l'état complet de la sortie des pipelines et alertent
automatiquement si le comportement change. Utilise syrupy pour la gestion
des snapshots.

Pour mettre à jour les snapshots : pytest --snapshot-update
"""

import pytest
import polars as pl
from datetime import datetime
from syrupy.assertion import SnapshotAssertion

from electricore.core.pipelines.perimetre import pipeline_perimetre
from electricore.core.pipelines.abonnements import pipeline_abonnements
from electricore.core.pipelines.energie import pipeline_energie


# =========================================================================
# FIXTURES SNAPSHOT - DONNÉES DE TEST
# =========================================================================


@pytest.fixture
def historique_snapshot_test() -> pl.LazyFrame:
    """
    Historique de test pour snapshots.

    Scénario réaliste avec :
    - 2 PDLs
    - Événements MES, MCT, changements puissance/FTA
    - Période 2024
    """
    return pl.LazyFrame({
        "pdl": [
            "PDL00001", "PDL00001", "PDL00001", "PDL00001",
            "PDL00002", "PDL00002", "PDL00002"
        ],
        "date_evenement": [
            datetime(2024, 1, 1, 9, 0, 0),   # MES
            datetime(2024, 3, 15, 10, 0, 0), # MCT changement calendrier
            datetime(2024, 6, 1, 14, 0, 0),  # MCT changement puissance
            datetime(2024, 9, 1, 11, 0, 0),  # MCT changement FTA
            datetime(2024, 2, 1, 9, 30, 0),  # MES
            datetime(2024, 5, 15, 15, 0, 0), # MCT changement puissance
            datetime(2024, 12, 31, 23, 0, 0) # RES
        ],
        "evenement_declencheur": [
            "MES", "MCT", "MCT", "MCT",
            "MES", "MCT", "RES"
        ],
        "etat_contractuel": [
            "EN SERVICE", "EN SERVICE", "EN SERVICE", "EN SERVICE",
            "EN SERVICE", "EN SERVICE", "RESILIE"
        ],
        "puissance_souscrite": [
            6.0, 6.0, 9.0, 9.0,
            3.0, 6.0, 6.0
        ],
        "formule_tarifaire_acheminement": [
            "BTINFCUST", "BTINFCUST", "BTINFCUST", "BTINFMU4",
            "BTINFCU4", "BTINFCU4", "BTINFCU4"
        ],
        "id_calendrier_distributeur": [
            "DI000001", "DI000002", "DI000002", "DI000002",
            "DI000001", "DI000001", "DI000001"
        ],
        "ref_situation_contractuelle": [
            "REF001", "REF001", "REF001", "REF001",
            "REF002", "REF002", "REF002"
        ],
    })


@pytest.fixture
def releves_snapshot_test() -> pl.LazyFrame:
    """
    Relevés de test pour snapshots.

    Relevés mensuels correspondant aux historiques, avec :
    - Calendrier BASE et HP/HC
    - Index croissants réalistes
    """
    return pl.LazyFrame({
        "pdl": [
            "PDL00001", "PDL00001", "PDL00001", "PDL00001",
            "PDL00001", "PDL00001", "PDL00001",
            "PDL00002", "PDL00002", "PDL00002", "PDL00002"
        ],
        "date_releve": [
            datetime(2024, 2, 1),
            datetime(2024, 4, 1),
            datetime(2024, 5, 1),
            datetime(2024, 7, 1),
            datetime(2024, 8, 1),
            datetime(2024, 10, 1),
            datetime(2024, 11, 1),
            datetime(2024, 3, 1),
            datetime(2024, 4, 1),
            datetime(2024, 6, 1),
            datetime(2024, 7, 1),
        ],
        "base": [
            1500.0, None, None, None, None, None, None,
            800.0, 1100.0, 1400.0, 1650.0
        ],
        "hp": [
            None, 1800.0, 2100.0, 2700.0, 3000.0, 3600.0, 3900.0,
            None, None, None, None
        ],
        "hc": [
            None, 1200.0, 1400.0, 1800.0, 2000.0, 2400.0, 2600.0,
            None, None, None, None
        ],
        "id_calendrier_distributeur": [
            "DI000001", "DI000002", "DI000002", "DI000002",
            "DI000002", "DI000002", "DI000002",
            "DI000001", "DI000001", "DI000001", "DI000001"
        ],
        "source": ["flux_R151"] * 11,
        "ref_situation_contractuelle": [
            "REF001", "REF001", "REF001", "REF001",
            "REF001", "REF001", "REF001",
            "REF002", "REF002", "REF002", "REF002"
        ],
    })


# =========================================================================
# TESTS SNAPSHOT - PIPELINE PÉRIMÈTRE
# =========================================================================


@pytest.mark.skip(reason="Needs snapshot generation")
@pytest.mark.integration
@pytest.mark.smoke
def test_pipeline_perimetre_snapshot(
    historique_snapshot_test: pl.LazyFrame,
    snapshot: SnapshotAssertion
):
    """
    Test snapshot du pipeline périmètre.

    Ce test capture le résultat complet du pipeline. Toute modification
    de la logique métier déclenchera une alerte nécessitant review.
    """
    result = pipeline_perimetre(historique_snapshot_test).collect()

    # Convertir en dict pour snapshot
    result_dict = result.to_dicts()

    # Comparer avec snapshot
    assert result_dict == snapshot


@pytest.mark.skip(reason="Needs snapshot generation")
@pytest.mark.integration
def test_pipeline_perimetre_structure_snapshot(
    historique_snapshot_test: pl.LazyFrame,
    snapshot: SnapshotAssertion
):
    """
    Test snapshot de la structure de sortie du pipeline périmètre.

    Capture uniquement les colonnes et types, pas les valeurs.
    """
    result = pipeline_perimetre(historique_snapshot_test).collect()

    structure = {
        "columns": result.columns,
        "dtypes": [str(dtype) for dtype in result.dtypes],
        "shape": result.shape,
    }

    assert structure == snapshot


# =========================================================================
# TESTS SNAPSHOT - PIPELINE ABONNEMENTS
# =========================================================================


@pytest.mark.skip(reason="Needs snapshot generation")
@pytest.mark.integration
@pytest.mark.smoke
def test_pipeline_abonnements_snapshot(
    historique_snapshot_test: pl.LazyFrame,
    snapshot: SnapshotAssertion
):
    """
    Test snapshot du pipeline abonnements.

    Capture le calcul complet des périodes d'abonnement.
    """
    result = pipeline_abonnements(historique_snapshot_test).collect()

    result_dict = result.to_dicts()

    assert result_dict == snapshot


@pytest.mark.skip(reason="Needs snapshot generation")
@pytest.mark.integration
def test_pipeline_abonnements_colonnes_critiques_snapshot(
    historique_snapshot_test: pl.LazyFrame,
    snapshot: SnapshotAssertion
):
    """
    Test snapshot des colonnes critiques du pipeline abonnements.

    Focus sur pdl, dates, puissance, FTA uniquement.
    """
    result = pipeline_abonnements(historique_snapshot_test).collect()

    colonnes_critiques = [
        "pdl",
        "debut",
        "fin",
        "puissance_souscrite",
        "formule_tarifaire_acheminement",
        "nb_jours",
    ]

    result_critique = result.select(colonnes_critiques).to_dicts()

    assert result_critique == snapshot


# =========================================================================
# TESTS SNAPSHOT - PIPELINE ÉNERGIE
# =========================================================================


@pytest.mark.skip(reason="Needs snapshot generation")
@pytest.mark.integration
@pytest.mark.smoke
@pytest.mark.slow
def test_pipeline_energie_snapshot(
    historique_snapshot_test: pl.LazyFrame,
    releves_snapshot_test: pl.LazyFrame,
    snapshot: SnapshotAssertion
):
    """
    Test snapshot du pipeline énergie.

    Capture le calcul complet des consommations par cadran.
    Ce test est marqué 'slow' car il peut prendre plusieurs secondes.
    """
    result = pipeline_energie(
        historique_snapshot_test,
        releves_snapshot_test
    ).collect()

    # Arrondir les valeurs d'énergie pour éviter les variations numériques
    result_rounded = result.with_columns([
        pl.col(col).round(2)
        for col in result.columns
        if result[col].dtype in [pl.Float64, pl.Float32]
    ])

    result_dict = result_rounded.to_dicts()

    assert result_dict == snapshot


@pytest.mark.skip(reason="Needs snapshot generation")
@pytest.mark.integration
def test_pipeline_energie_aggrege_snapshot(
    historique_snapshot_test: pl.LazyFrame,
    releves_snapshot_test: pl.LazyFrame,
    snapshot: SnapshotAssertion
):
    """
    Test snapshot des totaux agrégés du pipeline énergie.

    Capture uniquement les sommes par PDL pour détecter les régressions
    sur les calculs globaux.
    """
    result = pipeline_energie(
        historique_snapshot_test,
        releves_snapshot_test
    ).collect()

    # Agréger par PDL
    aggrege = (
        result
        .group_by("pdl")
        .agg([
            pl.col("BASE").sum().alias("total_BASE"),
            pl.col("HP").sum().alias("total_HP"),
            pl.col("HC").sum().alias("total_HC"),
        ])
        .sort("pdl")
    )

    aggrege_dict = aggrege.to_dicts()

    assert aggrege_dict == snapshot


# =========================================================================
# TESTS SNAPSHOT - CAS LIMITES
# =========================================================================


@pytest.mark.skip(reason="Needs snapshot generation")
@pytest.mark.integration
def test_pipeline_perimetre_pdl_unique_snapshot(snapshot: SnapshotAssertion):
    """Test snapshot avec un seul PDL et événement."""
    historique_minimal = pl.LazyFrame({
        "pdl": ["PDL99999"],
        "date_evenement": [datetime(2024, 1, 1)],
        "evenement_declencheur": ["MES"],
        "etat_contractuel": ["EN SERVICE"],
        "puissance_souscrite": [6.0],
        "formule_tarifaire_acheminement": ["BTINFCUST"],
        "id_calendrier_distributeur": ["DI000001"],
        "ref_situation_contractuelle": ["REF999"],
    })

    result = pipeline_perimetre(historique_minimal).collect()

    assert result.to_dicts() == snapshot


@pytest.mark.skip(reason="Needs snapshot generation")
@pytest.mark.integration
def test_pipeline_abonnements_annee_complete_snapshot(snapshot: SnapshotAssertion):
    """Test snapshot d'une année complète sans changement."""
    historique_stable = pl.LazyFrame({
        "pdl": ["PDL88888", "PDL88888"],
        "date_evenement": [
            datetime(2024, 1, 1),
            datetime(2024, 12, 31, 23, 59, 59)
        ],
        "evenement_declencheur": ["MES", "RES"],
        "etat_contractuel": ["EN SERVICE", "RESILIE"],
        "puissance_souscrite": [9.0, 9.0],
        "formule_tarifaire_acheminement": ["BTINFCU4", "BTINFCU4"],
        "id_calendrier_distributeur": ["DI000002", "DI000002"],
        "ref_situation_contractuelle": ["REF888", "REF888"],
    })

    result = pipeline_abonnements(historique_stable).collect()

    assert result.to_dicts() == snapshot