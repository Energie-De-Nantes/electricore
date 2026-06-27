"""Tests d'intégration de `GET /provision/estimation?pdl=…` (estimation de provision, #487).

Estimation **cœur-pure** depuis flux_r67 (cold-start, ADR-0048) : provision d'énergie d'un
lissé en kWh (annuel par cadran + provision mensuelle `/12` plate) + couverture / profondeur /
qualité / signal alertable. Le seam de build (`_estimer`) est monkeypatché : on vérifie le
câblage transport + enveloppe (`contract_version`, en-tête, sérialisation), pas le cœur
(couvert par `tests/unit/test_pipeline_provision.py`). Auth surchargée comme pour rsc.
"""

import datetime as dt

import polars as pl
import pytest
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key
from electricore.core.builds.rapport_provision import RapportProvision


@pytest.fixture
def client():
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


def _rapport(pdl: str, *, trouve: bool) -> RapportProvision:
    if not trouve:
        return RapportProvision(pdl=pdl, as_of=dt.date(2026, 6, 27), estimation=pl.DataFrame())
    estimation = pl.DataFrame(
        {
            "pdl": [pdl],
            "energie_base_kwh": [1200.0],
            "energie_hp_kwh": [None],
            "energie_hc_kwh": [None],
            "energie_base_mensuel_kwh": [100.0],
            "energie_hp_mensuel_kwh": [None],
            "energie_hc_mensuel_kwh": [None],
            "couverture_debut": [dt.date(2025, 6, 1)],
            "couverture_fin": [dt.date(2026, 6, 1)],
            "couverture_mois": [12.0],
            "couverture_suffisante": [True],
            "profondeur_cadran": ["base"],
            "qualite": ["réelle"],
            "presence_regularisation": [False],
            "signal_alertable": [False],
        }
    )
    return RapportProvision(pdl=pdl, as_of=dt.date(2026, 6, 27), estimation=estimation)


def test_estimation_renvoie_enveloppe_kwh(client, monkeypatch):
    """Tracer : un PDL trouvé → enveloppe `{contract_version, …, estimation}` en kWh."""
    monkeypatch.setattr(
        "electricore.api.routers.provision._estimer",
        lambda pdl: _rapport(pdl, trouve=True),
    )
    response = client.get("/provision/estimation", params={"pdl": "12345678901234"})
    assert response.status_code == 200
    assert response.headers["X-Contract-Version"] == "1"
    body = response.json()
    assert body["contract_version"] == 1
    assert body["pdl"] == "12345678901234"
    assert body["trouve"] is True
    est = body["estimation"]
    # kWh annuel + provision mensuelle /12 plate + couverture.
    assert est["energie_base_kwh"] == 1200.0
    assert est["energie_base_mensuel_kwh"] == 100.0
    assert est["couverture_debut"] == "2025-06-01"
    assert est["couverture_mois"] == 12.0
    assert est["couverture_suffisante"] is True
    # Zéro € exposé (convention de colonne € = suffixe `_eur`, ADR-0016/0027).
    assert not any(cle.endswith("_eur") or "_eur_" in cle for cle in est)


def test_estimation_pdl_sans_r67_renvoie_trouve_false(client, monkeypatch):
    monkeypatch.setattr(
        "electricore.api.routers.provision._estimer",
        lambda pdl: _rapport(pdl, trouve=False),
    )
    response = client.get("/provision/estimation", params={"pdl": "00000000000000"})
    assert response.status_code == 200
    body = response.json()
    assert body["trouve"] is False
    assert body["estimation"] is None


def test_estimation_exige_le_parametre_pdl(client):
    """Sans `pdl` → 422 (validation FastAPI)."""
    response = client.get("/provision/estimation")
    assert response.status_code == 422


def test_estimation_refuse_sans_api_key():
    """Endpoint sécurisé : 401 sans clé (pas d'override d'auth ici)."""
    response = TestClient(app).get("/provision/estimation", params={"pdl": "1"})
    assert response.status_code == 401
