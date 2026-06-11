"""Tests du contrat de modes de `POST /etl/run` (#152).

L'API accepte les modes réels du runner (`test`, `all`, `rebuild`, `resync`,
liste de flux) — pas seulement l'enum historique. Le pipeline subprocess est
court-circuité : on teste le contrat HTTP, pas l'ingestion.
"""

import pytest
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key
from electricore.api.services import etl_service


@pytest.fixture(autouse=True)
def _pipeline_courtcircuite(monkeypatch):
    """Aucun subprocess : le job passe directement à `completed`."""

    def _fake_run(job):
        job.status = etl_service.ETLStatus.completed

    monkeypatch.setattr(etl_service, "_run_pipeline", _fake_run)
    monkeypatch.setattr(etl_service, "is_running", lambda: False)
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    yield
    app.dependency_overrides.clear()


@pytest.mark.parametrize("mode", ["test", "all", "rebuild", "resync", "r151", "c15", "r151 c15", "R151 C15"])
def test_run_accepte_les_modes_reels(mode):
    response = TestClient(app).post("/etl/run", json={"mode": mode})
    assert response.status_code == 202, response.text


@pytest.mark.parametrize("mode", ["bogus", "", "r151 bogus", "drop tables"])
def test_run_refuse_les_modes_inconnus(mode):
    response = TestClient(app).post("/etl/run", json={"mode": mode})
    assert response.status_code == 422


def test_run_normalise_le_mode_dans_le_job():
    """La liste de flux est normalisée (minuscules, espaces simples) pour le runner."""
    response = TestClient(app).post("/etl/run", json={"mode": "  R151   c15 "})
    assert response.status_code == 202
    assert response.json()["mode"] == "r151 c15"
