"""Tests d'intégration de `POST /facturation/rsc` (résolution RSC, #282/#5).

Résolution **sans état** : Odoo POST un lot d'`id_Affaire`, electricore recoupe X12
(`flux_affaires`) ⨝ C15 (`flux_c15`) et renvoie le `ref_situation_contractuelle` par id —
ou un motif d'erreur (xor), jamais de silent-drop. Les deux seams de chargement DuckDB
(`_charger_c15` / `_charger_affaires`) sont monkeypatchés : on vérifie le câblage transport
+ enveloppe, pas le loader (couvert par `tests/core/loaders/test_loader_affaires.py`). Le
seam d'auth est surchargé comme pour les méta-périodes / turpe-variable.
"""

import polars as pl
import pytest
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key


@pytest.fixture
def client():
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


def _brancher(monkeypatch, *, c15: pl.DataFrame, affaires: pl.DataFrame):
    """Court-circuite les deux seams DuckDB du router avec des frames synthétiques."""
    monkeypatch.setattr("electricore.api.routers.rsc._charger_c15", lambda ids: c15)
    monkeypatch.setattr("electricore.api.routers.rsc._charger_affaires", lambda ids: affaires)


def test_resout_lot_xor_par_id(client, monkeypatch):
    """Tracer : un lot mixte → enveloppe `{contract_version, results}`, un résultat par id."""
    _brancher(
        monkeypatch,
        c15=pl.DataFrame({"id_affaire": ["38233180"], "ref_situation_contractuelle": ["248912973"]}),
        affaires=pl.DataFrame({"affaire_id": ["38233180", "AME001"]}),
    )
    response = client.post("/facturation/rsc", json={"ids": ["38233180", "AME001", "ZZZ999"]})
    assert response.status_code == 200
    body = response.json()
    assert body["contract_version"] == 1
    assert response.headers["X-Contract-Version"] == "1"

    par_id = {r["id_affaire"]: r for r in body["results"]}
    assert par_id["38233180"] == {"id_affaire": "38233180", "ref_situation_contractuelle": "248912973"}
    assert "situation" in par_id["AME001"]["error"].lower()  # connue X12, pas de RSC
    assert "inconnue" in par_id["ZZZ999"]["error"].lower()  # absente des deux flux


def test_lot_vide_court_circuite_sans_toucher_duckdb(client, monkeypatch):
    """Lot vide → `results: []` sans appeler les seams (un `IN ()` casserait le SQL)."""

    def _interdit(ids):
        raise AssertionError("le loader ne doit pas être appelé pour un lot vide")

    monkeypatch.setattr("electricore.api.routers.rsc._charger_c15", _interdit)
    monkeypatch.setattr("electricore.api.routers.rsc._charger_affaires", _interdit)

    response = client.post("/facturation/rsc", json={"ids": []})
    assert response.status_code == 200
    assert response.json() == {"contract_version": 1, "results": []}


def test_refuse_sans_api_key():
    """Endpoint sécurisé : 401 sans clé (pas d'override d'auth ici)."""
    response = TestClient(app).post("/facturation/rsc", json={"ids": []})
    assert response.status_code == 401
