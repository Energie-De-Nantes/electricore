"""Tests unitaires d'`ElectricoreClient`.

Le client est testé contre l'app FastAPI réelle via `TestClient` (qui hérite de
`httpx.Client`) : on monkeypatch le service de calcul pour fournir un DataFrame
déterministe, et on vérifie la boucle complète client → endpoint → client.
"""

import httpx
import polars as pl
import pytest
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal

from electricore.api.config import settings
from electricore.api.main import app
from electricore.api.security import get_current_api_key
from electricore.client import ElectricoreClient


@pytest.fixture(autouse=True)
def _mock_odoo_configured(monkeypatch):
    """Force `settings.is_odoo_configured` à True (sinon les endpoints renvoient 501 en CI)."""
    monkeypatch.setattr(type(settings), "get_odoo_config", lambda self: {})


@pytest.fixture
def df_attendu() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "invoice_line_ids": [101],
            "x_pdl": ["12345678901234"],
            "x_lisse": [False],
            "name_account_move": ["INV/2025/0001"],
            "name_product_category": ["HP"],
            "name_product_product": ["Énergie HP"],
            "quantity": [100.0],
            "quantite_enedis": [123.45],
            "memo_puissance": [""],
        }
    )


@pytest.fixture
def client_electricore(monkeypatch, df_attendu):
    """ElectricoreClient pointé sur l'app FastAPI avec orchestration moquée.

    Depuis #77, l'endpoint stacke @arrow_endpoint + @with_odoo : on court-circuite
    `OdooReader` et on patche `facturation_du_mois` au point d'import dans `main.py`.
    """
    from contextlib import contextmanager

    @contextmanager
    def _fake_reader(config):
        yield None

    monkeypatch.setattr("electricore.integrations.odoo.decorators.OdooReader", _fake_reader)
    monkeypatch.setattr(
        "electricore.api.routers.facturation.facturation_du_mois",
        lambda odoo, mois=None: df_attendu,
    )
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    transport = TestClient(app)
    yield ElectricoreClient(url="http://testserver", api_key="key", http_client=transport)
    app.dependency_overrides.clear()


def test_facturation_retourne_le_dataframe_servi_par_endpoint(client_electricore, df_attendu):
    df = client_electricore.facturation(mois="2025-01-01")
    assert_frame_equal(df, df_attendu)


def test_accise_round_trip_via_endpoint(monkeypatch):
    """`client.accise(trimestre)` round-trip un DataFrame servi par /taxes/accise/detail.arrow."""
    from contextlib import contextmanager

    df_attendu = pl.DataFrame(
        {
            "pdl": ["12345678901234"],
            "trimestre": ["2025-T1"],
            "energie_mwh": [1.5],
            "accise_eur": [33.75],
        }
    )

    # Depuis #75, l'endpoint stacke @xlsx_endpoint + @with_odoo : il faut
    # court-circuiter `OdooReader` *et* mocker `accise_par_contrat` au point
    # d'import dans `main.py`.
    @contextmanager
    def _fake_reader(config):
        yield None

    monkeypatch.setattr("electricore.integrations.odoo.decorators.OdooReader", _fake_reader)
    monkeypatch.setattr(
        "electricore.api.routers.taxes.accise_par_contrat_service",
        lambda odoo, trimestre=None: df_attendu,
    )
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        client = ElectricoreClient(url="http://testserver", api_key="key", http_client=TestClient(app))
        df = client.accise(trimestre="2025-T1")
    finally:
        app.dependency_overrides.clear()

    assert_frame_equal(df, df_attendu)


def test_cta_round_trip_via_endpoint(monkeypatch):
    """`client.cta(trimestre)` round-trip un DataFrame servi par /taxes/cta/detail.arrow."""
    from contextlib import contextmanager

    df_attendu = pl.DataFrame(
        {
            "pdl": ["12345678901234"],
            "order_name": ["SO/2025/0001"],
            "trimestre": ["2025-T1"],
            "turpe_fixe_eur": [42.50],
            "cta_eur": [9.18],
            "taux_cta_pct": [21.61],
        }
    )

    # Depuis #77, l'endpoint stacke @arrow_endpoint + @with_odoo : court-circuit
    # `OdooReader` et patche `cta_par_contrat` au point d'import dans `main.py`.
    @contextmanager
    def _fake_reader(config):
        yield None

    monkeypatch.setattr("electricore.integrations.odoo.decorators.OdooReader", _fake_reader)
    monkeypatch.setattr(
        "electricore.api.routers.taxes.cta_par_contrat_service",
        lambda odoo, trimestre=None: df_attendu,
    )
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        client = ElectricoreClient(url="http://testserver", api_key="key", http_client=TestClient(app))
        df = client.cta(trimestre="2025-T1")
    finally:
        app.dependency_overrides.clear()

    assert_frame_equal(df, df_attendu)


def test_flux_round_trip_via_endpoint(monkeypatch):
    """`client.flux(table_name)` round-trip un DataFrame servi par /flux/{name}.arrow."""
    df_attendu = pl.DataFrame(
        {
            "pdl": ["12345678901234"],
            "date_evenement": ["2025-01-15"],
            "evenement_declencheur": ["MES"],
        }
    )

    monkeypatch.setattr(
        "electricore.api.routers.flux._load_flux_df",
        lambda table_name, prm, limit: df_attendu,
    )

    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        client = ElectricoreClient(url="http://testserver", api_key="key", http_client=TestClient(app))
        df = client.flux("c15")
    finally:
        app.dependency_overrides.clear()

    assert_frame_equal(df, df_attendu)


def test_flux_propage_filtre_prm_a_lendpoint(monkeypatch):
    """`client.flux(table, prm=...)` propage le PRM en query string."""
    captures: list[tuple[str, str | None, int]] = []

    def _capture(table_name: str, prm: str | None, limit: int):
        captures.append((table_name, prm, limit))
        return pl.DataFrame({"x": [1]})

    monkeypatch.setattr("electricore.api.routers.flux._load_flux_df", _capture)

    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        client = ElectricoreClient(url="http://testserver", api_key="key", http_client=TestClient(app))
        client.flux("r151", prm="12345678901234")
    finally:
        app.dependency_overrides.clear()

    assert captures == [("r151", "12345678901234", 1_000_000)]


def test_flux_renvoie_404_pour_table_inconnue():
    """Le client propage une `HTTPStatusError` quand la table n'est pas dans FLUX_CONFIGS."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        client = ElectricoreClient(url="http://testserver", api_key="key", http_client=TestClient(app))
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.flux("inexistante")
    finally:
        app.dependency_overrides.clear()

    assert exc_info.value.response.status_code == 404


def test_facturation_envoie_la_cle_api_dans_le_header():
    """Le header `X-API-Key` est positionné par le client."""
    headers_recus: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        headers_recus.append(request.headers.get("x-api-key", ""))
        return httpx.Response(401)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = ElectricoreClient(url="http://x", api_key="secret-key", http_client=http_client)

    with pytest.raises(httpx.HTTPStatusError):
        client.facturation(mois="2025-01-01")

    assert headers_recus == ["secret-key"]
