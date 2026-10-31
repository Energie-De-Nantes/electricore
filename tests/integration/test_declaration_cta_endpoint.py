"""Tests d'intégration de `/taxes/cta/declaration.{arrow,xlsx}` (#651, ADR-0058).

Seam : `declaration_cta` référencé dans le router (I/O DuckDB court-circuitée) ; la
précondition « trimestre non terminé » passe, elle, par le **vrai** service — elle est
levée avant tout chargement.
"""

import io
import zipfile

import polars as pl
import pytest
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key

ROUTER = "electricore.api.routers.declaration_cta"


@pytest.fixture
def client():
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def declaration() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-1"],
            "pdl": ["12345678901234"],
            "mois_annee": ["2025-03"],
            "trimestre": ["2025-T1"],
            "turpe_fixe_eur": [10.0],
            "taux_cta_pct": [21.93],
            "cta_eur": [2.19],
            "source_hash": ["deadbeefcafe0001"],
            "empreinte_trimestre": ["0123456789abcdef"],
            "version_empreinte": [1],
        }
    )


def test_arrow_round_trip_et_propage_trimestre(monkeypatch, client, declaration):
    appels: list[str | None] = []

    def _fake(trimestre=None):
        appels.append(trimestre)
        return declaration

    monkeypatch.setattr(f"{ROUTER}.declaration_cta", _fake)
    response = client.get("/taxes/cta/declaration.arrow", params={"trimestre": "2025-T1"})

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"
    assert pl.read_ipc_stream(io.BytesIO(response.content)).equals(declaration)
    assert appels == ["2025-T1"]


def test_xlsx_trois_onglets(monkeypatch, client, declaration):
    monkeypatch.setattr(f"{ROUTER}.declaration_cta", lambda trimestre=None: declaration)
    response = client.get("/taxes/cta/declaration.xlsx", params={"trimestre": "2025-T1"})

    assert response.status_code == 200
    assert "declaration_cta2025-T1.xlsx" in response.headers["content-disposition"]
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        workbook_xml = zf.read("xl/workbook.xml").decode()
    for onglet in ("Résumé", "Par taux", "Détail"):
        assert onglet in workbook_xml


@pytest.mark.parametrize("chemin", ["/taxes/cta/declaration.arrow", "/taxes/cta/declaration.xlsx"])
def test_trimestre_non_termine_422_precondition(client, chemin):
    response = client.get(chemin, params={"trimestre": "2099-T1"})
    assert response.status_code == 422
    assert response.headers["x-error-kind"] == "precondition"


def test_trimestre_mal_forme_refuse(client):
    assert client.get("/taxes/cta/declaration.arrow", params={"trimestre": "2025-Q1"}).status_code == 422


@pytest.mark.parametrize("chemin", ["/taxes/cta/declaration.arrow", "/taxes/cta/declaration.xlsx"])
def test_refuse_sans_api_key(chemin):
    assert TestClient(app).get(chemin).status_code == 401


@pytest.mark.parametrize("chemin", ["/taxes/cta/declaration.arrow", "/taxes/cta/declaration.xlsx"])
def test_tag_taxes_pas_legacy(chemin):
    assert app.openapi()["paths"][chemin]["get"]["tags"] == ["taxes"]
