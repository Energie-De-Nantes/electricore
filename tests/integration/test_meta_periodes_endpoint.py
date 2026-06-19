"""Tests d'intégration de l'endpoint `GET /facturation/meta-periodes` (ADR-0027, #227).

Endpoint de lecture des méta-périodes mensuelles : Odoo tire d'electricore. Le seam
de test est la fonction de service `meta_periodes` référencée dans le router (même
patron que `tests/integration/test_facturation_arrow_endpoint.py`) — on court-circuite
l'I/O DuckDB, le chemin transport + enveloppe reste réel.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key

PARIS = ZoneInfo("Europe/Paris")


@pytest.fixture
def meta_periodes_synthetiques() -> pl.DataFrame:
    """Mime la sortie projetée de `meta_periodes` (champs `PeriodeMeta` du contrat v3).

    Porte le bloc imbriqué `releves_utilises` (ADR-0038, #360) en colonne `pl.Object` :
    RSC-1 (réelle) a ses relevés bornants, RSC-2 (incalculable) a `[]`.
    """
    df = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-1", "RSC-2"],
            "pdl": ["12345678901234", "12345678905678"],
            "mois_annee": ["2026-05", "2026-05"],
            "debut": [datetime(2026, 5, 1, tzinfo=PARIS), datetime(2026, 5, 1, tzinfo=PARIS)],
            "fin": [datetime(2026, 6, 1, tzinfo=PARIS), datetime(2026, 6, 1, tzinfo=PARIS)],
            "nb_jours": [31, 31],
            "puissance_moyenne_kva": [6.0, 9.0],
            "formule_tarifaire_acheminement": ["BTINFCUST", "BTINFCUST"],
            "energie_base_kwh": [None, 420.0],
            "energie_hp_kwh": [312.4, None],
            "energie_hc_kwh": [145.2, None],
            "turpe_fixe_eur": [9.13, 12.0],
            "turpe_variable_eur": [18.4, 22.0],
            "has_changement": [False, False],
            "qualite": ["réelle", "incalculable"],
            "statut_communication": ["communicante", "non_communicante"],
        }
    )
    releves_utilises = [
        [
            {
                "releve_id": "a1b2c3d4e5f60718",
                "date_releve": "2026-05-01T00:00:00+02:00",
                "nature_index": "réel",
                "index_hp_kwh": 1000,
                "index_hc_kwh": 500,
            }
        ],
        [],
    ]
    return df.with_columns(pl.Series("releves_utilises", releves_utilises, dtype=pl.Object))


def test_meta_periodes_retourne_enveloppe_json(monkeypatch, meta_periodes_synthetiques):
    """Tracer bullet : GET sert les méta-périodes du mois en JSON enveloppé."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.meta_periodes.meta_periodes",
        lambda mois=None, rsc=None: ("2026-05-01", meta_periodes_synthetiques),
    )
    try:
        response = TestClient(app).get("/facturation/meta-periodes", params={"mois": "2026-05-01"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()

    # Enveloppe
    assert body["mois"] == "2026-05-01"
    assert set(body["pagination"]) >= {"limit", "offset", "returned", "total"}
    assert body["pagination"]["total"] == 2
    assert body["pagination"]["returned"] == 2

    # Données
    assert len(body["data"]) == 2
    row = body["data"][0]
    assert row["ref_situation_contractuelle"] == "RSC-1"
    assert row["energie_hp_kwh"] == 312.4
    assert row["turpe_fixe_eur"] == 9.13
    assert row["qualite"] == "réelle"
    assert row["statut_communication"] == "communicante"


def test_meta_periodes_imbrique_releves_utilises(monkeypatch, meta_periodes_synthetiques):
    """ADR-0038 (#360) : chaque méta-période porte un tableau JSON `releves_utilises`
    imbriqué (objets `{ releve_id, date_releve, nature_index, registres réels }`) ;
    l'enveloppe et la pagination restent intactes (additif strict)."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.meta_periodes.meta_periodes",
        lambda mois=None, rsc=None: ("2026-05-01", meta_periodes_synthetiques),
    )
    try:
        response = TestClient(app).get("/facturation/meta-periodes", params={"mois": "2026-05-01"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    body = response.json()

    # Enveloppe inchangée (additif strict) + colonnes existantes intactes.
    assert set(body) == {"mois", "contract_version", "filters", "pagination", "data"}
    row = body["data"][0]
    assert row["ref_situation_contractuelle"] == "RSC-1"
    assert row["energie_hp_kwh"] == 312.4

    # Bloc imbriqué présent et structuré.
    assert "releves_utilises" in row
    trace = row["releves_utilises"]
    assert isinstance(trace, list) and len(trace) == 1
    assert trace[0] == {
        "releve_id": "a1b2c3d4e5f60718",
        "date_releve": "2026-05-01T00:00:00+02:00",
        "nature_index": "réel",
        "index_hp_kwh": 1000,
        "index_hc_kwh": 500,
    }
    # Mois incalculable → tableau vide.
    assert body["data"][1]["releves_utilises"] == []


def test_meta_periodes_refuse_sans_api_key():
    """Endpoint sécurisé : 401 sans clé."""
    response = TestClient(app).get("/facturation/meta-periodes")
    assert response.status_code == 401


def test_meta_periodes_enveloppe_porte_contract_version(monkeypatch, meta_periodes_synthetiques):
    """L'enveloppe expose `contract_version` (assertion de compat côté consommateur)."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.meta_periodes.meta_periodes",
        lambda mois=None, rsc=None: ("2026-05-01", meta_periodes_synthetiques),
    )
    try:
        response = TestClient(app).get("/facturation/meta-periodes")
    finally:
        app.dependency_overrides.clear()

    assert response.json()["contract_version"] == 3


def test_meta_periodes_propage_mois_et_rsc(monkeypatch, meta_periodes_synthetiques):
    """`mois` et les `rsc=` répétés atteignent le service (rsc en liste)."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    appels: list[tuple] = []

    def _capture(mois=None, rsc=None):
        appels.append((mois, rsc))
        return "2026-05-01", meta_periodes_synthetiques

    monkeypatch.setattr("electricore.api.routers.meta_periodes.meta_periodes", _capture)
    try:
        response = TestClient(app).get(
            "/facturation/meta-periodes",
            params={"mois": "2026-05-01", "rsc": ["RSC-1", "RSC-2"]},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert appels == [("2026-05-01", ["RSC-1", "RSC-2"])]
    assert response.json()["filters"] == {"rsc": ["RSC-1", "RSC-2"]}


def test_meta_periodes_pagine(monkeypatch, meta_periodes_synthetiques):
    """`limit`/`offset` tranchent les lignes ; `total` reste le total non paginé."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.meta_periodes.meta_periodes",
        lambda mois=None, rsc=None: ("2026-05-01", meta_periodes_synthetiques),
    )
    try:
        response = TestClient(app).get(
            "/facturation/meta-periodes",
            params={"limit": 1, "offset": 1},
        )
    finally:
        app.dependency_overrides.clear()

    body = response.json()
    assert body["pagination"] == {"limit": 1, "offset": 1, "returned": 1, "total": 2}
    assert len(body["data"]) == 1
    assert body["data"][0]["ref_situation_contractuelle"] == "RSC-2"
