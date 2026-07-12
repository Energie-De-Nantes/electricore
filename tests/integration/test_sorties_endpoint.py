"""Tests d'intégration de `POST /perimetre/sorties` (fin de souscription, #632, ADR-0052).

Sans état : Odoo POST un lot de RSC, electricore renvoie uniquement celles qui sont
**sorties** du périmètre (code C15 RES/CFNS) — une RSC encore présente ou inconnue
n'apparaît simplement pas (pas d'erreur, cas nominal). Le seam de chargement DuckDB
(`_charger_c15_sorties`) est monkeypatché : on vérifie le câblage transport + enveloppe,
pas le loader. Le seam d'auth est surchargé comme pour la résolution RSC / turpe-variable.
"""

from datetime import datetime

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


def _brancher(monkeypatch, c15_sorties: pl.DataFrame):
    """Court-circuite le seam DuckDB du router avec une frame synthétique déjà filtrée
    aux codes de sortie (comme le fait réellement `c15().filter(...).sorties()`)."""
    monkeypatch.setattr("electricore.api.routers.sorties._charger_c15_sorties", lambda rsc: c15_sorties)


def _c15_sorties() -> pl.DataFrame:
    return pl.DataFrame(
        [
            ("rA", "PDL-A", datetime(2026, 3, 15), "RES"),
            ("rC", "PDL-C", datetime(2026, 5, 2), "CFNS"),
        ],
        schema={
            "ref_situation_contractuelle": pl.Utf8,
            "pdl": pl.Utf8,
            "date_evenement": pl.Datetime,
            "evenement_declencheur": pl.Utf8,
        },
        orient="row",
    )


def test_lot_mixte_ne_rend_que_les_sorties_avec_code_et_date_exacts(client, monkeypatch):
    """rA et rC sont sorties, rB (encore présente) n'est pas renvoyée par le seam (déjà
    filtrée côté loader), rZ est inconnue — les deux dernières sont simplement absentes."""
    _brancher(monkeypatch, _c15_sorties())

    response = client.post("/perimetre/sorties", json={"rsc": ["rA", "rB", "rC", "rZ"]})
    assert response.status_code == 200
    assert response.headers["X-Contract-Version"] == "1"

    body = response.json()
    assert body["contract_version"] == 1
    par_rsc = {s["ref_situation_contractuelle"]: s for s in body["sorties"]}
    assert set(par_rsc) == {"rA", "rC"}  # rB et rZ absentes, pas d'erreur
    assert par_rsc["rA"] == {
        "ref_situation_contractuelle": "rA",
        "pdl": "PDL-A",
        "evenement_declencheur": "RES",
        "date_sortie": "2026-03-15",
    }
    assert par_rsc["rC"]["evenement_declencheur"] == "CFNS"
    assert par_rsc["rC"]["date_sortie"] == "2026-05-02"


def test_lot_vide_court_circuite_sans_toucher_duckdb(client, monkeypatch):
    """Lot vide → `sorties: []` sans appeler le seam (un `IN ()` casserait le SQL)."""

    def _interdit(rsc):
        raise AssertionError("le loader ne doit pas être appelé pour un lot vide")

    monkeypatch.setattr("electricore.api.routers.sorties._charger_c15_sorties", _interdit)

    response = client.post("/perimetre/sorties", json={"rsc": []})
    assert response.status_code == 200
    assert response.json() == {"contract_version": 1, "sorties": []}


def test_refuse_sans_api_key():
    """Endpoint sécurisé : 401 sans clé (pas d'override d'auth ici)."""
    response = TestClient(app).post("/perimetre/sorties", json={"rsc": []})
    assert response.status_code == 401
