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

from electricore.api.main import app
from electricore.api.security import get_current_api_key
from electricore.client import ElectricoreClient


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
    """ElectricoreClient pointé sur l'app FastAPI avec service moqué."""
    monkeypatch.setattr(
        "electricore.api.services.facturation_service.calculer_lignes_facture_rapprochees",
        lambda mois=None: df_attendu,
    )
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    transport = TestClient(app)
    yield ElectricoreClient(url="http://testserver", api_key="key", http_client=transport)
    app.dependency_overrides.clear()


def test_facturation_retourne_le_dataframe_servi_par_endpoint(client_electricore, df_attendu):
    df = client_electricore.facturation(mois="2025-01-01")
    assert_frame_equal(df, df_attendu)


def test_accise_round_trip_via_endpoint(monkeypatch):
    """`client.accise(trimestre)` round-trip un DataFrame servi par /taxes/accise/arrow."""
    df_attendu = pl.DataFrame(
        {
            "pdl": ["12345678901234"],
            "trimestre": ["2025-T1"],
            "energie_mwh": [1.5],
            "accise_eur": [33.75],
        }
    )
    monkeypatch.setattr(
        "electricore.api.services.taxes_service.calculer_accise_detail",
        lambda trimestre=None: df_attendu,
    )
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        client = ElectricoreClient(
            url="http://testserver", api_key="key", http_client=TestClient(app)
        )
        df = client.accise(trimestre="2025-T1")
    finally:
        app.dependency_overrides.clear()

    assert_frame_equal(df, df_attendu)


def test_cta_round_trip_via_endpoint(monkeypatch):
    """`client.cta(trimestre)` round-trip un DataFrame servi par /taxes/cta/arrow."""
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
    monkeypatch.setattr(
        "electricore.api.services.taxes_service.calculer_cta_detail",
        lambda trimestre=None: df_attendu,
    )
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        client = ElectricoreClient(
            url="http://testserver", api_key="key", http_client=TestClient(app)
        )
        df = client.cta(trimestre="2025-T1")
    finally:
        app.dependency_overrides.clear()

    assert_frame_equal(df, df_attendu)


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
