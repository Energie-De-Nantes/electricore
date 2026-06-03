"""Tests d'intégration de l'endpoint `GET /facturation/arrow`."""

import io

import polars as pl
import pytest
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal

from electricore.api.config import settings
from electricore.api.main import app
from electricore.api.security import get_current_api_key


@pytest.fixture(autouse=True)
def _mock_odoo_configured(monkeypatch):
    """Force `settings.is_odoo_configured` à True (sinon les endpoints renvoient 501 en CI)."""
    monkeypatch.setattr(type(settings), "get_odoo_config", lambda self: {})


@pytest.fixture
def lignes_rapprochees_attendues() -> pl.DataFrame:
    """Petit DataFrame qui mime la sortie de rapprocher_facturation_mensuelle."""
    return pl.DataFrame(
        {
            "invoice_line_ids": [101, 102],
            "x_pdl": ["12345678901234", "12345678905678"],
            "x_lisse": [False, True],
            "name_account_move": ["INV/2025/0001", "INV/2025/0002"],
            "name_product_category": ["HP", "Base"],
            "name_product_product": ["Énergie HP", "Énergie Base"],
            "quantity": [100.0, 50.0],
            "quantite_enedis": [123.45, None],
            "memo_puissance": ["", ""],
        }
    )


@pytest.fixture
def client_avec_service_mock(monkeypatch, lignes_rapprochees_attendues):
    """TestClient avec auth + service de facturation moqués."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.services.facturation_service.calculer_lignes_facture_rapprochees",
        lambda mois=None: lignes_rapprochees_attendues,
    )
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_endpoint_retourne_arrow_ipc_stream(client_avec_service_mock, lignes_rapprochees_attendues):
    """L'endpoint sert le DataFrame `lignes_facture_rapprochees` en Arrow IPC."""
    response = client_avec_service_mock.get("/facturation/arrow")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"

    df = pl.read_ipc_stream(io.BytesIO(response.content))
    assert_frame_equal(df, lignes_rapprochees_attendues)


def test_endpoint_refuse_sans_api_key():
    """Sans header `X-API-Key`, l'endpoint répond 401."""
    response = TestClient(app).get("/facturation/arrow")
    assert response.status_code == 401


def test_endpoint_propage_le_parametre_mois_au_service(monkeypatch, lignes_rapprochees_attendues):
    """Le query param `mois` est transmis au service de calcul."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    appels_avec_mois: list[str | None] = []

    def fake_calculer(mois: str | None = None) -> pl.DataFrame:
        appels_avec_mois.append(mois)
        return lignes_rapprochees_attendues

    monkeypatch.setattr(
        "electricore.api.services.facturation_service.calculer_lignes_facture_rapprochees",
        fake_calculer,
    )

    try:
        response = TestClient(app).get("/facturation/arrow", params={"mois": "2025-03-01"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert appels_avec_mois == ["2025-03-01"]
