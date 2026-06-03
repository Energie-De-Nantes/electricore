"""Tests d'intégration des endpoints `/taxes/accise/arrow` et `/taxes/cta/arrow`."""

import io

import polars as pl
import pytest
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal

from electricore.api.main import app
from electricore.api.security import get_current_api_key

ARROW_IPC = "application/vnd.apache.arrow.stream"


@pytest.fixture
def df_accise_detail() -> pl.DataFrame:
    """Échantillon ressemblant à la sortie de pipeline_accise."""
    return pl.DataFrame(
        {
            "pdl": ["12345678901234", "12345678901234"],
            "mois_consommation": ["2025-01", "2025-02"],
            "trimestre": ["2025-T1", "2025-T1"],
            "energie_mwh": [1.234, 2.345],
            "taux_accise_eur_mwh": [22.5, 22.5],
            "accise_eur": [27.77, 52.76],
        }
    )


# =============================================================================
# /taxes/accise/arrow
# =============================================================================


def test_accise_endpoint_retourne_arrow_ipc_stream(monkeypatch, df_accise_detail):
    """L'endpoint sert le détail accise en Arrow IPC."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.services.taxes_service.calculer_accise_detail",
        lambda trimestre=None: df_accise_detail,
    )

    try:
        response = TestClient(app).get("/taxes/accise/arrow")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"] == ARROW_IPC

    df = pl.read_ipc_stream(io.BytesIO(response.content))
    assert_frame_equal(df, df_accise_detail)


def test_accise_endpoint_refuse_sans_api_key():
    """Sans clé API → 401."""
    response = TestClient(app).get("/taxes/accise/arrow")
    assert response.status_code == 401


def test_accise_endpoint_propage_trimestre(monkeypatch, df_accise_detail):
    """Le query param `trimestre` est transmis au service de calcul."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    appels: list[str | None] = []

    def fake_calculer(trimestre: str | None = None) -> pl.DataFrame:
        appels.append(trimestre)
        return df_accise_detail

    monkeypatch.setattr(
        "electricore.api.services.taxes_service.calculer_accise_detail", fake_calculer
    )

    try:
        response = TestClient(app).get("/taxes/accise/arrow", params={"trimestre": "2025-T1"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert appels == ["2025-T1"]


# =============================================================================
# /taxes/cta/arrow
# =============================================================================


@pytest.fixture
def df_cta_detail() -> pl.DataFrame:
    """Échantillon ressemblant à la sortie mensuelle de la pipeline CTA."""
    return pl.DataFrame(
        {
            "pdl": ["12345678901234"],
            "order_name": ["SO/2025/0001"],
            "mois_annee": ["janvier 2025"],
            "trimestre": ["2025-T1"],
            "turpe_fixe_eur": [42.50],
            "cta_eur": [9.18],
            "taux_cta_pct": [21.61],
        }
    )


def test_cta_endpoint_retourne_arrow_ipc_stream(monkeypatch, df_cta_detail):
    """L'endpoint sert le détail CTA en Arrow IPC."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.services.taxes_service.calculer_cta_detail",
        lambda trimestre=None: df_cta_detail,
    )

    try:
        response = TestClient(app).get("/taxes/cta/arrow")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"] == ARROW_IPC

    df = pl.read_ipc_stream(io.BytesIO(response.content))
    assert_frame_equal(df, df_cta_detail)


def test_cta_endpoint_refuse_sans_api_key():
    response = TestClient(app).get("/taxes/cta/arrow")
    assert response.status_code == 401


def test_cta_endpoint_propage_trimestre(monkeypatch, df_cta_detail):
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    appels: list[str | None] = []

    def fake_calculer(trimestre: str | None = None) -> pl.DataFrame:
        appels.append(trimestre)
        return df_cta_detail

    monkeypatch.setattr(
        "electricore.api.services.taxes_service.calculer_cta_detail", fake_calculer
    )

    try:
        response = TestClient(app).get("/taxes/cta/arrow", params={"trimestre": "2025-T2"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert appels == ["2025-T2"]
