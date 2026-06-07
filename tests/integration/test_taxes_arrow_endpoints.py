"""Tests d'intégration des endpoints `/taxes/accise/*` et `/taxes/cta/arrow`."""

import io

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
    from contextlib import contextmanager

    @contextmanager
    def _fake_reader(config):
        yield None

    monkeypatch.setattr("electricore.integrations.odoo.decorators.OdooReader", _fake_reader)


@pytest.fixture
def df_accise_detail() -> pl.DataFrame:
    """Échantillon ressemblant à la sortie de `accise_par_contrat`."""
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
# /taxes/accise/detail.arrow
# =============================================================================


def test_accise_detail_arrow_retourne_arrow_ipc_stream(monkeypatch, _mock_odoo_reader, df_accise_detail):
    """L'endpoint sert le détail accise en Arrow IPC."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.taxes.accise_par_contrat",
        lambda odoo, trimestre=None: df_accise_detail,
    )

    try:
        response = TestClient(app).get("/taxes/accise/detail.arrow")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"] == ARROW_IPC

    df = pl.read_ipc_stream(io.BytesIO(response.content))
    assert_frame_equal(df, df_accise_detail)


def test_accise_detail_arrow_refuse_sans_api_key():
    """Sans clé API → 401."""
    response = TestClient(app).get("/taxes/accise/detail.arrow")
    assert response.status_code == 401


def test_accise_detail_arrow_propage_trimestre(monkeypatch, _mock_odoo_reader, df_accise_detail):
    """Le query param `trimestre` est transmis à `accise_par_contrat`."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    appels: list[str | None] = []

    def _capture(odoo, trimestre=None):
        appels.append(trimestre)
        return df_accise_detail

    monkeypatch.setattr("electricore.api.routers.taxes.accise_par_contrat", _capture)

    try:
        response = TestClient(app).get("/taxes/accise/detail.arrow", params={"trimestre": "2025-T1"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert appels == ["2025-T1"]


# =============================================================================
# /taxes/accise/detail.xlsx
# =============================================================================


def test_accise_detail_xlsx_retourne_xlsx_mono_onglet(monkeypatch, _mock_odoo_reader, df_accise_detail):
    """L'endpoint sert le détail accise en XLSX mono-onglet."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.taxes.accise_par_contrat",
        lambda odoo, trimestre=None: df_accise_detail,
    )

    try:
        response = TestClient(app).get("/taxes/accise/detail.xlsx")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(XLSX_MIME)
    assert "attachment" in response.headers.get("content-disposition", "")


# =============================================================================
# /taxes/accise/rapport.xlsx
# =============================================================================


@pytest.fixture
def fake_rapport_accise(df_accise_detail):
    """`RapportAccise` synthétique (les 3 onglets minimaux pour la sérialisation XLSX)."""
    from electricore.integrations.odoo.taxes import RapportAccise

    return RapportAccise(
        resume=pl.DataFrame(
            {"trimestre": ["2025-T1"], "nb_pdl": [1], "energie_mwh_total": [3.579], "accise_eur_total": [80.53]}
        ),
        par_taux=pl.DataFrame(
            {"taux_accise_eur_mwh": [22.5], "energie_mwh": [3.579], "accise_eur": [80.53], "nb_pdl": [1]}
        ),
        detail=df_accise_detail,
    )


def test_accise_rapport_xlsx_retourne_xlsx_multi_onglets(monkeypatch, _mock_odoo_reader, fake_rapport_accise):
    """L'endpoint sert le rapport multi-onglets (Résumé / Par taux / Détail)."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.taxes.rapport_accise",
        lambda odoo, trimestre=None: fake_rapport_accise,
    )

    try:
        response = TestClient(app).get("/taxes/accise/rapport.xlsx")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(XLSX_MIME)
    assert "attachment" in response.headers.get("content-disposition", "")


def test_accise_rapport_xlsx_propage_trimestre(monkeypatch, _mock_odoo_reader, fake_rapport_accise):
    """Le query param `trimestre` est propagé jusqu'à `rapport_accise`."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    appels: list[str | None] = []

    def _capture(odoo, trimestre=None):
        appels.append(trimestre)
        return fake_rapport_accise

    monkeypatch.setattr("electricore.api.routers.taxes.rapport_accise", _capture)

    try:
        response = TestClient(app).get("/taxes/accise/rapport.xlsx", params={"trimestre": "2025-T2"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert appels == ["2025-T2"]


def test_anciens_endpoints_accise_404(_mock_odoo_reader):
    """Les anciens paths sont supprimés."""
    for path in ("/taxes/accise/xlsx", "/taxes/accise/arrow"):
        response = TestClient(app).get(path)
        assert response.status_code == 404, f"{path} devrait être 404"


# =============================================================================
# /taxes/cta/detail.arrow
# =============================================================================


@pytest.fixture
def df_cta_detail() -> pl.DataFrame:
    """Échantillon ressemblant à la sortie mensuelle de `cta_par_contrat`."""
    return pl.DataFrame(
        {
            "pdl": ["12345678901234"],
            "order_name": ["SO/2025/0001"],
            "trimestre": ["2025-T1"],
            "turpe_fixe_eur": [42.50],
            "cta_eur": [9.18],
            "taux_cta_pct": [21.61],
        }
    )


def test_cta_detail_arrow_retourne_arrow_ipc_stream(monkeypatch, _mock_odoo_reader, df_cta_detail):
    """L'endpoint sert le détail CTA en Arrow IPC."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.taxes.cta_par_contrat",
        lambda odoo, trimestre=None: df_cta_detail,
    )

    try:
        response = TestClient(app).get("/taxes/cta/detail.arrow")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"] == ARROW_IPC

    df = pl.read_ipc_stream(io.BytesIO(response.content))
    assert_frame_equal(df, df_cta_detail)


def test_cta_detail_arrow_refuse_sans_api_key():
    response = TestClient(app).get("/taxes/cta/detail.arrow")
    assert response.status_code == 401


def test_cta_detail_arrow_propage_trimestre(monkeypatch, _mock_odoo_reader, df_cta_detail):
    """Le query param `trimestre` est transmis à `cta_par_contrat`."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    appels: list[str | None] = []

    def _capture(odoo, trimestre=None):
        appels.append(trimestre)
        return df_cta_detail

    monkeypatch.setattr("electricore.api.routers.taxes.cta_par_contrat", _capture)

    try:
        response = TestClient(app).get("/taxes/cta/detail.arrow", params={"trimestre": "2025-T2"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert appels == ["2025-T2"]


# =============================================================================
# /taxes/cta/detail.xlsx
# =============================================================================


def test_cta_detail_xlsx_retourne_xlsx_mono_onglet(monkeypatch, _mock_odoo_reader, df_cta_detail):
    """L'endpoint sert le détail CTA en XLSX mono-onglet."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.taxes.cta_par_contrat",
        lambda odoo, trimestre=None: df_cta_detail,
    )

    try:
        response = TestClient(app).get("/taxes/cta/detail.xlsx")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(XLSX_MIME)
    assert "attachment" in response.headers.get("content-disposition", "")


# =============================================================================
# /taxes/cta/rapport.xlsx
# =============================================================================


@pytest.fixture
def fake_rapport_cta(df_cta_detail):
    """`RapportCta` synthétique pour la sérialisation XLSX."""
    from electricore.integrations.odoo.taxes import RapportCta

    return RapportCta(
        resume=pl.DataFrame(
            {
                "trimestre": ["2025-T1"],
                "nb_pdl": [1],
                "turpe_fixe_total_eur": [42.50],
                "cta_total_eur": [9.18],
            }
        ),
        par_taux=pl.DataFrame(
            {
                "trimestre": ["2025-T1"],
                "taux_cta_pct": [21.61],
                "nb_pdl": [1],
                "turpe_fixe_eur": [42.50],
                "cta_eur": [9.18],
            }
        ),
        detail=pl.DataFrame(
            {
                "pdl": ["12345678901234"],
                "order_name": ["SO/2025/0001"],
                "turpe_fixe_total_eur": [42.50],
                "cta_total_eur": [9.18],
                "taux_cta_appliques": ["21.61"],
            }
        ),
    )


def test_cta_rapport_xlsx_retourne_xlsx_multi_onglets(monkeypatch, _mock_odoo_reader, fake_rapport_cta):
    """L'endpoint sert le rapport multi-onglets (Résumé / Par taux / Détail)."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.taxes.rapport_cta",
        lambda odoo, trimestre=None: fake_rapport_cta,
    )

    try:
        response = TestClient(app).get("/taxes/cta/rapport.xlsx")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(XLSX_MIME)
    assert "attachment" in response.headers.get("content-disposition", "")


def test_cta_rapport_xlsx_propage_trimestre(monkeypatch, _mock_odoo_reader, fake_rapport_cta):
    """Le query param `trimestre` est propagé jusqu'à `rapport_cta`."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    appels: list[str | None] = []

    def _capture(odoo, trimestre=None):
        appels.append(trimestre)
        return fake_rapport_cta

    monkeypatch.setattr("electricore.api.routers.taxes.rapport_cta", _capture)

    try:
        response = TestClient(app).get("/taxes/cta/rapport.xlsx", params={"trimestre": "2025-T2"})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert appels == ["2025-T2"]


def test_anciens_endpoints_cta_404(_mock_odoo_reader):
    """Les anciens paths CTA sont supprimés."""
    for path in ("/taxes/cta/xlsx", "/taxes/cta/arrow"):
        response = TestClient(app).get(path)
        assert response.status_code == 404, f"{path} devrait être 404"
