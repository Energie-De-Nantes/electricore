"""Tests de non-régression par snapshot des pipelines principaux (#250).

Capturent l'état complet de la sortie des pipelines `historique` / `abonnements` /
`energie` et alertent si le comportement change. Utilise syrupy.

Le harnais compose les pipelines comme la production (`pipeline_historique` →
`pipeline_abonnements` / `pipeline_energie`) sur des **fixtures conformes** (historique
C15 brut complet, relevés au **modèle canonique** ADR-0029 avec C15), avec un **horizon
de facturation explicite** : à horizon fixé, la sortie ne dépend pas de l'heure
d'exécution (ADR-0019, #179) — condition *sine qua non* d'un snapshot déterministe.

Pour (re)générer les snapshots : `pytest tests/integration/test_pipelines_snapshot.py --snapshot-update`
"""

import datetime as dt
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from syrupy.assertion import SnapshotAssertion

from electricore.core.models.cadrans import CADRANS, col_index
from electricore.core.pipelines.abonnements import pipeline_abonnements
from electricore.core.pipelines.energie import pipeline_energie
from electricore.core.pipelines.historique import pipeline_historique

PARIS = ZoneInfo("Europe/Paris")

# Horizon de facturation figé : rend la génération des FACTURATION (et donc le snapshot)
# déterministe, indépendante de l'horloge murale (#179).
HORIZON = dt.datetime(2025, 1, 1, tzinfo=PARIS)


# =========================================================================
# CONSTRUCTEURS DE FIXTURES CONFORMES
# =========================================================================


def historique_brut(evenements: list[dict]) -> pl.LazyFrame:
    """Historique C15 brut complet (entrée de `pipeline_historique`).

    Chaque dict porte `ref`, `pdl`, `date` (datetime *naïf* → Europe/Paris),
    `evenement`, `puissance`, `fta`. Les colonnes `avant_/apres_` (calendrier + index
    par cadran) exigées par l'enrichissement sont à `None` (cas nominal sans rupture
    d'index) ; les non-nullables du schéma `Historique` ont des valeurs plausibles.
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
        "avant_id_calendrier_distributeur": [None] * n,
        "apres_id_calendrier_distributeur": [None] * n,
    }
    schema_overrides: dict[str, pl.DataType] = {
        "avant_id_calendrier_distributeur": pl.Utf8,
        "apres_id_calendrier_distributeur": pl.Utf8,
    }
    for cadran in CADRANS:
        data[f"avant_{col_index(cadran)}"] = [None] * n
        data[f"apres_{col_index(cadran)}"] = [None] * n
        schema_overrides[f"avant_{col_index(cadran)}"] = pl.Float64
        schema_overrides[f"apres_{col_index(cadran)}"] = pl.Float64

    return pl.LazyFrame(data, schema_overrides=schema_overrides).with_columns(
        pl.col("date_evenement").dt.replace_time_zone("Europe/Paris")
    )


def releves_periodiques(
    pdl: str, ref: str, calendrier: str, dates: list[dt.datetime], **index: list[float]
) -> pl.LazyFrame:
    """Relevés R151 au modèle canonique (ADR-0029), conformes à `RelevéIndex`.

    `index` porte les cadrans renseignés (ex. `base=[...]` ou `hp=[...], hc=[...]`) ;
    les autres cadrans sont `None` (Float64). `unite`/`precision` à `kWh`.
    """
    n = len(dates)
    data: dict[str, list] = {
        "pdl": [pdl] * n,
        "date_releve": dates,
        "source": ["flux_R151"] * n,
        "ordre_index": [False] * n,
        "ref_situation_contractuelle": [ref] * n,
        "id_calendrier_distributeur": [calendrier] * n,
        "unite": ["kWh"] * n,
        "precision": ["kWh"] * n,
    }
    schema_overrides = {col_index(cadran): pl.Float64 for cadran in CADRANS}
    for cadran in CADRANS:
        data[col_index(cadran)] = index.get(cadran, [None] * n)

    return pl.LazyFrame(data, schema_overrides=schema_overrides).with_columns(
        pl.col("date_releve").dt.replace_time_zone("Europe/Paris")
    )


@pytest.fixture
def historique_snapshot_test() -> pl.LazyFrame:
    """Scénario : 2 PDL en 2024, un en BASE (ouvert), un en HP/HC (résilié)."""
    return historique_brut(
        [
            {
                "ref": "REF001",
                "pdl": "PDL00001",
                "date": dt.datetime(2024, 1, 1, 9, 0),
                "evenement": "MES",
                "puissance": 6.0,
                "fta": "BTINFCUST",
            },
            {
                "ref": "REF002",
                "pdl": "PDL00002",
                "date": dt.datetime(2024, 2, 1, 9, 30),
                "evenement": "MES",
                "puissance": 9.0,
                "fta": "BTINFMUDT",
            },
            {
                "ref": "REF002",
                "pdl": "PDL00002",
                "date": dt.datetime(2024, 12, 31, 23, 0),
                "evenement": "RES",
                "puissance": 9.0,
                "fta": "BTINFMUDT",
                "etat": "RESILIE",
            },
        ]
    )


@pytest.fixture
def releves_snapshot_test() -> pl.LazyFrame:
    """Relevés mensuels alignés sur les bornes de facturation (index croissants)."""
    mois_2024 = [dt.datetime(2024, m, 1) for m in range(1, 13)]
    base = releves_periodiques(
        "PDL00001", "REF001", "DI000001", mois_2024, base=[1000.0 + 150.0 * i for i in range(12)]
    )
    # PDL00002 : HP/HC, présent de février à décembre (11 relevés).
    mois_ref2 = [dt.datetime(2024, m, 1) for m in range(2, 13)]
    hphc = releves_periodiques(
        "PDL00002",
        "REF002",
        "DI000002",
        mois_ref2,
        hp=[500.0 + 120.0 * i for i in range(11)],
        hc=[300.0 + 80.0 * i for i in range(11)],
    )
    return pl.concat([base, hphc], how="diagonal_relaxed")


# =========================================================================
# TESTS SNAPSHOT - PIPELINE HISTORIQUE
# =========================================================================


@pytest.mark.integration
@pytest.mark.smoke
def test_pipeline_historique_snapshot(historique_snapshot_test: pl.LazyFrame, snapshot: SnapshotAssertion):
    """Snapshot complet de l'historique enrichi (horizon figé → déterministe)."""
    result = pipeline_historique(historique_snapshot_test, horizon=HORIZON).collect()
    assert result.to_dicts() == snapshot


@pytest.mark.integration
def test_pipeline_historique_structure_snapshot(historique_snapshot_test: pl.LazyFrame, snapshot: SnapshotAssertion):
    """Snapshot de la structure (colonnes, types, forme) de l'historique enrichi."""
    result = pipeline_historique(historique_snapshot_test, horizon=HORIZON).collect()
    structure = {
        "columns": result.columns,
        "dtypes": [str(dtype) for dtype in result.dtypes],
        "shape": result.shape,
    }
    assert structure == snapshot


# =========================================================================
# TESTS SNAPSHOT - PIPELINE ABONNEMENTS
# =========================================================================


@pytest.mark.integration
@pytest.mark.smoke
def test_pipeline_abonnements_snapshot(historique_snapshot_test: pl.LazyFrame, snapshot: SnapshotAssertion):
    """Snapshot du calcul des périodes d'abonnement (sur historique enrichi)."""
    enrichi = pipeline_historique(historique_snapshot_test, horizon=HORIZON)
    result = pipeline_abonnements(enrichi).collect()
    assert result.to_dicts() == snapshot


@pytest.mark.integration
def test_pipeline_abonnements_colonnes_critiques_snapshot(
    historique_snapshot_test: pl.LazyFrame, snapshot: SnapshotAssertion
):
    """Snapshot des colonnes critiques des abonnements (pdl, dates, puissance, FTA)."""
    enrichi = pipeline_historique(historique_snapshot_test, horizon=HORIZON)
    result = pipeline_abonnements(enrichi).collect()
    colonnes_critiques = [
        "pdl",
        "debut",
        "fin",
        "puissance_souscrite_kva",
        "formule_tarifaire_acheminement",
        "nb_jours",
    ]
    assert result.select(colonnes_critiques).to_dicts() == snapshot


# =========================================================================
# TESTS SNAPSHOT - PIPELINE ÉNERGIE
# =========================================================================


@pytest.mark.integration
@pytest.mark.smoke
@pytest.mark.slow
def test_pipeline_energie_snapshot(
    historique_snapshot_test: pl.LazyFrame, releves_snapshot_test: pl.LazyFrame, snapshot: SnapshotAssertion
):
    """Snapshot complet des périodes d'énergie (historique enrichi → énergie)."""
    enrichi = pipeline_historique(historique_snapshot_test, horizon=HORIZON)
    result = pipeline_energie(enrichi, releves_snapshot_test).collect()
    # Arrondir les flottants pour neutraliser le bruit numérique.
    result_rounded = result.with_columns(
        [pl.col(col).round(2) for col in result.columns if result[col].dtype in (pl.Float64, pl.Float32)]
    )
    assert result_rounded.to_dicts() == snapshot


@pytest.mark.integration
def test_pipeline_energie_aggrege_snapshot(
    historique_snapshot_test: pl.LazyFrame, releves_snapshot_test: pl.LazyFrame, snapshot: SnapshotAssertion
):
    """Snapshot des totaux d'énergie par PDL (noms canoniques `energie_*_kwh`)."""
    enrichi = pipeline_historique(historique_snapshot_test, horizon=HORIZON)
    result = pipeline_energie(enrichi, releves_snapshot_test).collect()
    aggrege = (
        result.group_by("pdl")
        .agg(
            pl.col("energie_base_kwh").sum().round(2).alias("total_energie_base_kwh"),
            pl.col("energie_hp_kwh").sum().round(2).alias("total_energie_hp_kwh"),
            pl.col("energie_hc_kwh").sum().round(2).alias("total_energie_hc_kwh"),
        )
        .sort("pdl")
    )
    assert aggrege.to_dicts() == snapshot


# =========================================================================
# TESTS SNAPSHOT - CAS LIMITES
# =========================================================================


@pytest.mark.integration
def test_pipeline_historique_pdl_unique_snapshot(snapshot: SnapshotAssertion):
    """Snapshot avec un seul PDL et un seul événement (MES)."""
    historique_minimal = historique_brut(
        [
            {
                "ref": "REF999",
                "pdl": "PDL99999",
                "date": dt.datetime(2024, 1, 1),
                "evenement": "MES",
                "puissance": 6.0,
                "fta": "BTINFCUST",
            }
        ]
    )
    result = pipeline_historique(historique_minimal, horizon=HORIZON).collect()
    assert result.to_dicts() == snapshot


@pytest.mark.integration
def test_pipeline_abonnements_annee_complete_snapshot(snapshot: SnapshotAssertion):
    """Snapshot d'une année complète sans changement (MES → RES)."""
    historique_stable = historique_brut(
        [
            {
                "ref": "REF888",
                "pdl": "PDL88888",
                "date": dt.datetime(2024, 1, 1),
                "evenement": "MES",
                "puissance": 9.0,
                "fta": "BTINFCU4",
            },
            {
                "ref": "REF888",
                "pdl": "PDL88888",
                "date": dt.datetime(2024, 12, 31, 23, 59, 59),
                "evenement": "RES",
                "puissance": 9.0,
                "fta": "BTINFCU4",
                "etat": "RESILIE",
            },
        ]
    )
    enrichi = pipeline_historique(historique_stable, horizon=HORIZON)
    result = pipeline_abonnements(enrichi).collect()
    assert result.to_dicts() == snapshot
