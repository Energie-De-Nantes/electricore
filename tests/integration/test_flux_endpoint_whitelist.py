"""Tests d'intégration : `/flux/{table_name}*` reposent sur le whitelist FLUX_CONFIGS."""

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


class TestUnknownTableReturns404:
    """Une table absente de FLUX_CONFIGS renvoie 404 sur tous les exports."""

    def test_get_flux_unknown(self, client):
        response = client.get("/flux/totalement_inexistant")
        assert response.status_code == 404

    def test_get_flux_xlsx_unknown(self, client):
        response = client.get("/flux/totalement_inexistant.xlsx")
        assert response.status_code == 404

    def test_get_flux_arrow_unknown(self, client):
        response = client.get("/flux/totalement_inexistant.arrow")
        assert response.status_code == 404
