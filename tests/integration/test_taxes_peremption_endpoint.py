"""GET /taxes/peremption : check de péremption des taux régulés (#186, ADR-0024).

Endpoint JSON sans dépendance Odoo : le check est dérivé des CSV versionnés
embarqués dans la lib, à la date du jour côté serveur.
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


def test_peremption_repond_une_liste_d_avertissements(client_authentifie):
    response = client_authentifie.get("/taxes/peremption")

    assert response.status_code == 200, response.text
    data = response.json()
    assert isinstance(data, list)
    for a in data:
        assert a["taxe"] in ("TURPE", "Accise"), "seules les taxes à rythme connu sont vérifiées"
        assert a["message"] and "vérifier" in a["message"]
        assert a["attendu_depuis"] and a["dernier_start"]


def test_peremption_exige_une_cle_api():
    response = TestClient(app).get("/taxes/peremption")

    assert response.status_code in (401, 403)
