"""Tests d'intégration de l'endpoint relevés canoniques (`/releves*`, ADR-0032).

Le mart `releves` (ADR-0029) est exposé hors `/flux/*` : route nommée par le
domaine, adossée au loader `releves()` (pas au registre `FLUX_DESCRIPTORS`).

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
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


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
        lambda **kwargs: df_releves,
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

    `releves` est absent de `FLUX_DESCRIPTORS` : le catch-all flux le rejette en 404,
    preuve que le mart n'emprunte pas le namespace des flux bruts.
    """
    assert client.get("/flux/releves.arrow").status_code == 404
    assert client.get("/flux/releves").status_code == 404


# =============================================================================
# #265 — garde de validation : source hors enum → 422
# =============================================================================


def test_releves_source_invalide_renvoie_422(client):
    """Une `source` hors `flux_R151`/`flux_R64`/`flux_C15` est rejetée en 422.

    La validation précède tout accès DuckDB (aucune base requise ici) et vaut
    pour les trois formats.
    """
    for path in ("/releves", "/releves.arrow", "/releves.xlsx"):
        response = client.get(path, params={"source": "flux_PIRATE"})
        assert response.status_code == 422, f"{path} devrait rejeter une source invalide"


# =============================================================================
# #264 — parité de formats : JSON / XLSX / info
# =============================================================================


def test_releves_json_enveloped_and_paginated(client, monkeypatch, df_releves):
    """`GET /releves` sert le JSON enveloppé paginé, shape calquée sur `/flux/{name}`."""
    monkeypatch.setattr(
        "electricore.api.routers.releves._load_releves_df",
        lambda **kwargs: df_releves,
    )

    response = client.get("/releves", params={"limit": 1, "offset": 1})

    assert response.status_code == 200
    payload = response.json()
    assert payload["table"] == "releves"
    assert payload["pagination"] == {"limit": 1, "offset": 1, "returned": 1}
    # offset=1, limit=1 → la 2e ligne du mart (flux_C15)
    assert payload["data"] == [df_releves.slice(1, 1).to_dicts()[0]]


def test_releves_xlsx_export(client, monkeypatch, df_releves):
    """`GET /releves.xlsx` sert un export XLSX du mart (téléchargement)."""
    monkeypatch.setattr(
        "electricore.api.routers.releves._load_releves_df",
        lambda **kwargs: df_releves,
    )

    response = client.get("/releves.xlsx")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(XLSX_MIME)
    assert "attachment" in response.headers.get("content-disposition", "")


def test_releves_info_metadata_targets_prefixless_mart(client, monkeypatch):
    """`GET /releves/info` sert colonnes/types, count et dernière `date_releve`.

    Le route adresse le mart sans préfixe `flux_` (`prefix=""`) et désigne
    `date_releve` comme colonne de date métier.
    """
    captured: dict = {}

    def _fake_info(table_name, **kwargs):
        captured["table_name"] = table_name
        captured["kwargs"] = kwargs
        return {
            "table": "releves",
            "schema": "flux_enedis",
            "count": 2,
            "columns": [
                {"name": "pdl", "type": "VARCHAR"},
                {"name": "date_releve", "type": "TIMESTAMP"},
            ],
            "derniere_date": "2025-02-01",
        }

    monkeypatch.setattr("electricore.api.services.duckdb_service.get_table_info", _fake_info)

    response = client.get("/releves/info")

    assert response.status_code == 200
    body = response.json()
    assert body["table"] == "releves"
    assert body["count"] == 2
    assert body["derniere_date"] == "2025-02-01"
    assert {"name": "date_releve", "type": "TIMESTAMP"} in body["columns"]
    # Le route demande bien le mart prefix-less avec sa colonne de date métier
    assert captured["kwargs"].get("prefix") == ""
    assert captured["kwargs"].get("date_column") == "date_releve"
