"""Tests d'intégration de l'endpoint relevés canoniques (`/releves*`, ADR-0032).

Le mart `releves` (ADR-0029) est exposé hors `/flux/*` : route nommée par le
domaine, adossée au loader `releves()` (pas au registre `FLUX_CONFIGS`).

Convention de test : on monkeypatche le helper de chargement
`_load_releves_df` pour court-circuiter l'IO DuckDB (CI sans base), comme les
tests d'endpoint `/flux/*` monkeypatchent `_load_flux_df`.
"""

import io

import polars as pl
import pytest
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal

from electricore.api.main import app
from electricore.api.security import get_current_api_key

ARROW_IPC = "application/vnd.apache.arrow.stream"


@pytest.fixture
def client():
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def df_releves() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "pdl": ["12345678901234", "12345678901234"],
            "source": ["flux_R151", "flux_C15"],
            "date_releve": ["2025-01-01", "2025-02-01"],
            "index_base_kwh": [100.0, 250.0],
        }
    )


def test_releves_arrow_returns_arrow_stream_of_the_mart(client, monkeypatch, df_releves):
    """`GET /releves.arrow` sert le mart `releves` en flux Arrow IPC typé."""
    monkeypatch.setattr(
        "electricore.api.routers.releves._load_releves_df",
        lambda limit: df_releves,
    )

    response = client.get("/releves.arrow")

    assert response.status_code == 200
    assert response.headers["content-type"] == ARROW_IPC
    df = pl.read_ipc_stream(io.BytesIO(response.content))
    assert_frame_equal(df, df_releves)


def test_releves_arrow_requires_api_key():
    """Sans `X-API-Key`, `/releves.arrow` est refusé (401) — sécurisé comme tout endpoint data."""
    # Pas d'override d'auth : la dépendance réelle s'applique.
    response = TestClient(app).get("/releves.arrow")
    assert response.status_code == 401


def test_releves_not_served_by_flux_catchall(client):
    """`/releves` n'est PAS routé via `/flux/{table_name}` (ADR-0032).

    `releves` est absent de `FLUX_CONFIGS` : le catch-all flux le rejette en 404,
    preuve que le mart n'emprunte pas le namespace des flux bruts.
    """
    assert client.get("/flux/releves.arrow").status_code == 404
    assert client.get("/flux/releves").status_code == 404
