"""Tests d'intégration des endpoints `/facturation/*` (issue #64).

3 endpoints : `rapport.xlsx` (facturiste), `detail.xlsx` + `detail.arrow` (technique).
"""

import io
from contextlib import contextmanager

import polars as pl
import pytest
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal

from electricore.api.config import settings
from electricore.api.main import app
from electricore.api.security import get_current_api_key

ARROW_IPC = "application/vnd.apache.arrow.stream"
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


@pytest.fixture(autouse=True)
def _mock_odoo_configured(monkeypatch):
    """Force `settings.is_odoo_configured` à True (sinon les endpoints renvoient 501 en CI)."""
    monkeypatch.setattr(type(settings), "get_odoo_config", lambda self: {})


@pytest.fixture
def _mock_odoo_reader(monkeypatch):
    """Court-circuite `OdooReader` au sein du décorateur `@with_odoo` (#67)."""

    @contextmanager
    def _fake_reader(config):
        yield None

    monkeypatch.setattr("electricore.integrations.odoo.decorators.OdooReader", _fake_reader)


@pytest.fixture
def lignes_rapprochees_attendues() -> pl.DataFrame:
    """Petit DataFrame qui mime la sortie de `facturation_du_mois`."""
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


# =============================================================================
# /facturation/detail.arrow
# =============================================================================


def test_detail_arrow_retourne_arrow_ipc_stream(monkeypatch, _mock_odoo_reader, lignes_rapprochees_attendues):
    """L'endpoint sert le DataFrame brut en Arrow IPC."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.main.facturation_du_mois",
        lambda odoo, mois=None: lignes_rapprochees_attendues,
    )
    try:
        response = TestClient(app).get("/facturation/detail.arrow")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"] == ARROW_IPC

    df = pl.read_ipc_stream(io.BytesIO(response.content))
    assert_frame_equal(df, lignes_rapprochees_attendues)


def test_detail_arrow_refuse_sans_api_key():
    response = TestClient(app).get("/facturation/detail.arrow")
    assert response.status_code == 401


def test_detail_arrow_propage_mois(monkeypatch, _mock_odoo_reader, lignes_rapprochees_attendues):
    """Le query param `mois` est transmis à `facturation_du_mois`."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    appels: list[str | None] = []

    def fake_calculer(odoo, mois: str | None = None) -> pl.DataFrame:
        appels.append(mois)
        return lignes_rapprochees_attendues

    monkeypatch.setattr(
        "electricore.api.main.facturation_du_mois",
        fake_calculer,
    )
    try:
        response = TestClient(app).get("/facturation/detail.arrow", params={"mois": "2025-03-01"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert appels == ["2025-03-01"]


# =============================================================================
# /facturation/detail.xlsx
# =============================================================================


def test_detail_xlsx_retourne_xlsx_mono_onglet(monkeypatch, _mock_odoo_reader, lignes_rapprochees_attendues):
    """L'endpoint sert les lignes brutes en XLSX mono-onglet."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.main.facturation_du_mois",
        lambda odoo, mois=None: lignes_rapprochees_attendues,
    )
    try:
        response = TestClient(app).get("/facturation/detail.xlsx")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(XLSX_MIME)
    assert "attachment" in response.headers.get("content-disposition", "")


# =============================================================================
# /facturation/rapport.xlsx
# =============================================================================


@pytest.fixture
def fake_rapport_facturation(lignes_rapprochees_attendues):
    """`RapportFacturation` synthétique pour la sérialisation XLSX."""
    from electricore.integrations.odoo.facturation import RapportFacturation

    return RapportFacturation(
        resume=pl.DataFrame(
            {
                "mois": ["2025-03-01"],
                "nb_pdl": [2],
                "total_a_facturer": [2],
                "total_a_supprimer": [0],
            }
        ),
        lignes=lignes_rapprochees_attendues,
        changements_puissance=lignes_rapprochees_attendues.head(0),
    )


def test_rapport_xlsx_retourne_xlsx_multi_onglets(monkeypatch, _mock_odoo_reader, fake_rapport_facturation):
    """L'endpoint sert le rapport multi-onglets (Résumé / Lignes / Changements puissance)."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.main.rapport_facturation",
        lambda odoo, mois=None: fake_rapport_facturation,
    )
    try:
        response = TestClient(app).get("/facturation/rapport.xlsx")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(XLSX_MIME)
    assert "attachment" in response.headers.get("content-disposition", "")


def test_rapport_xlsx_propage_mois(monkeypatch, _mock_odoo_reader, fake_rapport_facturation):
    """Le query param `mois` est propagé jusqu'à `rapport_facturation`."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    appels: list[str | None] = []

    def _capture(odoo, mois=None):
        appels.append(mois)
        return fake_rapport_facturation

    monkeypatch.setattr("electricore.api.main.rapport_facturation", _capture)
    try:
        response = TestClient(app).get("/facturation/rapport.xlsx", params={"mois": "2025-04-01"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert appels == ["2025-04-01"]


def test_anciens_endpoints_facturation_404(_mock_odoo_reader):
    """Les anciens paths `/facturation/xlsx` et `/facturation/arrow` sont supprimés."""
    for path in ("/facturation/xlsx", "/facturation/arrow"):
        response = TestClient(app).get(path)
        assert response.status_code == 404, f"{path} devrait être 404"
