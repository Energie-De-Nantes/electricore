"""Endpoint CSV des PDL du périmètre actif (`GET /perimetre/pdls.csv`, ADR-0052).

Liste (une colonne `pdl`) pour une demande M023. On court-circuite l'IO DuckDB en
monkeypatchant le loader `c15`.
"""

from datetime import datetime

import polars as pl
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


class _FakeC15:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def lazy(self) -> pl.LazyFrame:
        return self._df.lazy()


def _c15() -> pl.DataFrame:
    return pl.DataFrame(
        [
            ("A", "rA", datetime(2020, 1, 1), "MES"),
            ("A", "rA", datetime(2021, 1, 1), "RES"),  # sorti en 2021
            ("B", "rB", datetime(2020, 1, 1), "MES"),  # ouvert
        ],
        schema={
            "pdl": pl.Utf8,
            "ref_situation_contractuelle": pl.Utf8,
            "date_evenement": pl.Datetime,
            "evenement_declencheur": pl.Utf8,
        },
        orient="row",
    )


def test_pdls_csv_perimetre_du_jour(client, monkeypatch):
    monkeypatch.setattr("electricore.core.loaders.duckdb.c15", lambda: _FakeC15(_c15()))

    response = client.get("/perimetre/pdls.csv")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    lignes = response.text.splitlines()
    assert lignes[0] == "pdl"  # une seule colonne
    assert lignes[1:] == ["B"]  # A (sorti en 2021) absent aujourd'hui


def test_pdls_csv_a_une_date_passee(client, monkeypatch):
    monkeypatch.setattr("electricore.core.loaders.duckdb.c15", lambda: _FakeC15(_c15()))

    response = client.get("/perimetre/pdls.csv", params={"jour": "2020-06-01"})

    assert response.status_code == 200
    assert response.text.splitlines()[1:] == ["A", "B"]  # A encore présent à cette date


def test_pdls_csv_exige_la_cle_api():
    assert TestClient(app).get("/perimetre/pdls.csv").status_code == 401
