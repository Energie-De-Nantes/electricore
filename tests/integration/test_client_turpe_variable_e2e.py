"""Bout-en-bout `ElectricoreClient.turpe_variable` ↔ endpoint POST RPC (#409).

Boucle complète client → endpoint → client contre le **vrai** calcul (turpe_rules.csv,
déterministe) : on POST un lot, on récupère des `ResultatTurpeVariable` typés appariés
par l'`id` opaque échoé. Pas de mock du calcul — l'enjeu est de vérifier le round-trip
typé du calculateur réel.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from electricore_client import ElectricoreClient
from electricore_client.models import LigneTurpeVariable, ResultatTurpeVariable
from fastapi.testclient import TestClient

from electricore.api.main import app
from electricore.api.security import get_current_api_key

PARIS = ZoneInfo("Europe/Paris")


@pytest.fixture
def client() -> ElectricoreClient:
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    try:
        yield ElectricoreClient(url="http://testserver", api_key="key", http_client=TestClient(app))
    finally:
        app.dependency_overrides.clear()


def _ligne(id_: str, fta: str, **energies) -> LigneTurpeVariable:
    return LigneTurpeVariable(
        id=id_, formule_tarifaire_acheminement=fta, debut=datetime(2025, 8, 15, tzinfo=PARIS), **energies
    )


def test_client_turpe_variable_lot_apparie_par_id(client):
    """Un lot mixte revient typé, apparié par id (montant correct face au barème réel)."""
    resultats = client.turpe_variable(
        [
            _ligne("L-base", "BTINFCUST", energie_base_kwh=1000.0),
            _ligne("L-hphc", "BTINFMUDT", energie_hp_kwh=312.4, energie_hc_kwh=145.2),
        ]
    )
    assert all(isinstance(r, ResultatTurpeVariable) for r in resultats)
    par_id = {r.id: r.turpe_variable_eur for r in resultats}
    # BTINFCUST c_base=4.84 → 48.4 ; BTINFMUDT c_hp=4.94/c_hc=3.5 → 20.51.
    assert par_id == {"L-base": 48.4, "L-hphc": 20.51}


def test_client_turpe_variable_erreur_par_id(client):
    """FTA inconnue → `error` (xor montant), pas un drop ; l'id valide garde son montant."""
    resultats = client.turpe_variable(
        [
            _ligne("ok", "BTINFCUST", energie_base_kwh=1000.0),
            _ligne("ko", "FTA_QUI_NEXISTE_PAS", energie_base_kwh=1000.0),
        ]
    )
    par_id = {r.id: r for r in resultats}
    assert par_id["ok"].turpe_variable_eur == 48.4
    assert par_id["ok"].error is None
    assert par_id["ko"].turpe_variable_eur is None
    assert par_id["ko"].error is not None
