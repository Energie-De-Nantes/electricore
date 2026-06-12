"""GET /taxes/millesimes : millésimes des taux régulés (#185, ADR-0024).

Endpoint JSON sans dépendance Odoo — le millésime est un savoir de la lib
(CSV versionnés), exposable même sur une instance sans ERP.
"""

import pytest
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key


@pytest.fixture
def client_authentifie():
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_millesimes_retourne_les_trois_taxes(client_authentifie):
    response = client_authentifie.get("/taxes/millesimes")

    assert response.status_code == 200, response.text
    data = response.json()
    assert [m["taxe"] for m in data] == ["TURPE", "Accise", "CTA"]
    for m in data:
        assert m["reference"], f"{m['taxe']} : référence vide"
        assert m["date_vigueur"], f"{m['taxe']} : date manquante"


def test_millesimes_accise_et_cta_portent_un_taux(client_authentifie):
    data = client_authentifie.get("/taxes/millesimes").json()
    par_taxe = {m["taxe"]: m for m in data}

    assert par_taxe["TURPE"]["valeur"] is None  # grille, pas de scalaire
    assert par_taxe["Accise"]["unite"] == "€/MWh"
    assert par_taxe["CTA"]["unite"] == "%"


def test_millesimes_exige_une_cle_api():
    response = TestClient(app).get("/taxes/millesimes")

    assert response.status_code in (401, 403)
