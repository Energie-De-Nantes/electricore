"""Tests d'intégration pour la convention extension-suffix `/flux/*` (issue #65).

Cible :
- `/flux/c15/entrees.xlsx` et `.sorties.xlsx` (anciens `/xlsx` segment supprimés)
- `/flux/{table_name}.xlsx` et `.arrow` (anciens `/xlsx`, `/arrow` segment supprimés)
- `/flux/{table_name}/info` reste segment (métadonnée, pas un format de fichier)
- L'ordre de déclaration des routes doit garantir que `/flux/{name}.xlsx`
  ne soit pas confondu avec `/flux/{name}` qui matche en JSON.
"""

import io

import polars as pl
import pytest
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal

from electricore.api.main import app
from electricore.api.security import get_current_api_key

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
ARROW_IPC = "application/vnd.apache.arrow.stream"


@pytest.fixture
def client():
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


@pytest.fixture
def _mock_c15(monkeypatch):
    """Court-circuite `c15().entrees()/.sorties().limit().collect()` (CI sans DuckDB)."""

    class _FakeBuilder:
        def entrees(self):
            return self

        def sorties(self):
            return self

        def limit(self, _n):
            return self

        def collect(self):
            return pl.DataFrame({"pdl": ["X"], "evenement_declencheur": ["MES"]})

    monkeypatch.setattr("electricore.core.loaders.duckdb.c15", lambda: _FakeBuilder())


def test_c15_entrees_xlsx_extension_suffix_returns_200(client, _mock_c15):
    """`GET /flux/c15/entrees.xlsx` sert le XLSX (PMES, MES, CFNE)."""
    response = client.get("/flux/c15/entrees.xlsx")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(XLSX_MIME)
    assert "attachment" in response.headers.get("content-disposition", "")


def test_c15_sorties_xlsx_extension_suffix_returns_200(client, _mock_c15):
    """`GET /flux/c15/sorties.xlsx` sert le XLSX (RES, CFNS)."""
    response = client.get("/flux/c15/sorties.xlsx")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(XLSX_MIME)
    assert "attachment" in response.headers.get("content-disposition", "")


# =============================================================================
# /flux/{table_name}.xlsx et .arrow (extension-suffix générique)
# =============================================================================


@pytest.fixture
def df_flux_synthetique() -> pl.DataFrame:
    return pl.DataFrame({"pdl": ["A", "B"], "date": ["2025-01-01", "2025-01-02"]})


class _FakeQuery:
    """Double du `DuckDBQuery` substitué au seam du loader (`flux`) — chaînable, sans IO."""

    def __init__(self, df: pl.DataFrame):
        self._df = df

    def filter(self, _filters):
        return self

    def limit(self, _n):
        return self

    def collect(self) -> pl.DataFrame:
        return self._df


def test_generic_flux_xlsx_extension_suffix_returns_200(client, monkeypatch, df_flux_synthetique):
    """`GET /flux/{table_name}.xlsx` (extension-suffix) sert le XLSX du flux."""
    monkeypatch.setattr("electricore.core.loaders.duckdb.flux", lambda name: _FakeQuery(df_flux_synthetique))

    response = client.get("/flux/c15.xlsx")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(XLSX_MIME)


def test_generic_flux_arrow_extension_suffix_returns_200(client, monkeypatch, df_flux_synthetique):
    """`GET /flux/{table_name}.arrow` (extension-suffix) sert l'Arrow IPC du flux."""
    monkeypatch.setattr("electricore.core.loaders.duckdb.flux", lambda name: _FakeQuery(df_flux_synthetique))

    response = client.get("/flux/c15.arrow")

    assert response.status_code == 200
    assert response.headers["content-type"] == ARROW_IPC
    df = pl.read_ipc_stream(io.BytesIO(response.content))
    assert_frame_equal(df, df_flux_synthetique)


def test_generic_flux_extension_suffix_whitelist_unknown_returns_404(client):
    """`GET /flux/inconnu.xlsx` (table absente de FLUX_DESCRIPTORS) → 404."""
    response = client.get("/flux/inconnu.xlsx")
    assert response.status_code == 404


# =============================================================================
# Anciens paths supprimés (404)
# =============================================================================


def test_anciens_paths_flux_segment_404(client):
    """Les anciens `/flux/.../xlsx` et `/.../arrow` (segment) sont supprimés."""
    for path in (
        "/flux/c15/entrees/xlsx",
        "/flux/c15/sorties/xlsx",
        "/flux/c15/xlsx",
        "/flux/c15/arrow",
    ):
        response = client.get(path)
        assert response.status_code == 404, f"{path} devrait être 404"


# =============================================================================
# Route ordering : /flux/{table_name} (JSON) coexiste avec .xlsx/.arrow
# =============================================================================


def test_flux_json_endpoint_still_works(client, monkeypatch, df_flux_synthetique):
    """`GET /flux/{table_name}` (sans extension) sert toujours le JSON."""
    monkeypatch.setattr("electricore.core.loaders.duckdb.flux", lambda name: _FakeQuery(df_flux_synthetique))

    response = client.get("/flux/c15", params={"limit": 10})

    assert response.status_code == 200
    payload = response.json()
    assert payload["table"] == "flux_c15"
    assert isinstance(payload["data"], list)


def test_flux_info_endpoint_still_at_segment_path(client, monkeypatch):
    """`GET /flux/{table_name}/info` reste en segment (métadonnée, pas un format)."""
    monkeypatch.setattr(
        "electricore.api.services.duckdb_service.get_table_info",
        lambda name: {"table": f"flux_{name}", "schema": "flux_enedis", "count": 0, "columns": []},
    )
    response = client.get("/flux/c15/info")

    assert response.status_code == 200
    assert response.json()["table"] == "flux_c15"
