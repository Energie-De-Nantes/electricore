"""Tests du client `turpe_variable` (POST RPC, sans serveur).

Le calculateur est un POST batch (pas un stream) : on envoie un lot d'assiettes,
on reçoit un résultat typé par `id` opaque (jamais positionnel). MockTransport
échoue/réussit l'écho ; on vérifie le typage, l'indexation par id, la garde de
version, et le rejet d'un id non échoé.
"""

from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest
from electricore_client import ElectricoreClient
from electricore_client.exceptions import ContractVersionError
from electricore_client.models import LigneTurpeVariable, ResultatTurpeVariable

PARIS = ZoneInfo("Europe/Paris")


def _handler(results, *, version="1"):
    def handler(request: httpx.Request) -> httpx.Response:
        # Echo : le serveur renvoie autant de résultats que d'entrées, par id.
        return httpx.Response(
            200,
            json={"contract_version": int(version), "results": results},
            headers={"X-Contract-Version": version},
        )

    return handler


def _client(handler) -> ElectricoreClient:
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return ElectricoreClient(url="http://testserver", api_key="key", http_client=http)


def _ligne(id_: str, fta="BTINFCUST", **energies) -> LigneTurpeVariable:
    return LigneTurpeVariable(
        id=id_, formule_tarifaire_acheminement=fta, debut=datetime(2025, 8, 15, tzinfo=PARIS), **energies
    )


def test_turpe_variable_retourne_resultats_typees():
    """Le client POST le lot et renvoie des `ResultatTurpeVariable` typés."""
    handler = _handler([{"id": "abc", "turpe_variable_eur": 48.4}])
    client = _client(handler)
    resultats = client.turpe_variable([_ligne("abc", energie_base_kwh=1000.0)])
    assert all(isinstance(r, ResultatTurpeVariable) for r in resultats)
    assert resultats[0].id == "abc"
    assert resultats[0].turpe_variable_eur == 48.4
    assert resultats[0].error is None


def test_turpe_variable_indexe_par_id_echoe_pas_par_position():
    """Les résultats sont appariés par l'`id` échoé, pas par l'ordre d'envoi."""
    # Serveur renvoie dans l'ordre INVERSE des entrées : l'appariement est par id.
    handler = _handler(
        [
            {"id": "L-hphc", "turpe_variable_eur": 20.51},
            {"id": "L-base", "turpe_variable_eur": 48.4},
        ]
    )
    client = _client(handler)
    resultats = client.turpe_variable(
        [_ligne("L-base", energie_base_kwh=1000.0), _ligne("L-hphc", "BTINFMUDT", energie_hp_kwh=312.4)]
    )
    par_id = {r.id: r.turpe_variable_eur for r in resultats}
    assert par_id == {"L-base": 48.4, "L-hphc": 20.51}


def test_turpe_variable_porte_les_erreurs_par_id():
    """Une FTA inconnue revient avec `error` (xor `turpe_variable_eur`), pas un drop."""
    handler = _handler(
        [
            {"id": "ok", "turpe_variable_eur": 48.4},
            {"id": "inconnue", "error": "FTA inconnue : FOO"},
        ]
    )
    client = _client(handler)
    resultats = client.turpe_variable([_ligne("ok"), _ligne("inconnue", "FOO")])
    par_id = {r.id: r for r in resultats}
    assert par_id["ok"].turpe_variable_eur == 48.4
    assert par_id["inconnue"].turpe_variable_eur is None
    assert "FOO" in par_id["inconnue"].error


def test_turpe_variable_envoie_le_lot_en_post():
    """Le corps POST contient `lignes`, chaque ligne sérialisée (debut ISO, cadrans)."""
    captures: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captures.append(json.loads(request.content))
        return httpx.Response(
            200, json={"results": [{"id": "x", "turpe_variable_eur": 1.0}]}, headers={"X-Contract-Version": "1"}
        )

    client = _client(handler)
    client.turpe_variable([_ligne("x", energie_base_kwh=10.0)])
    corps = captures[0]
    assert "lignes" in corps
    assert corps["lignes"][0]["id"] == "x"
    assert corps["lignes"][0]["formule_tarifaire_acheminement"] == "BTINFCUST"


def test_turpe_variable_accepte_des_dicts():
    """Par commodité, le client accepte aussi des dicts (validés en LigneTurpeVariable)."""
    handler = _handler([{"id": "d", "turpe_variable_eur": 5.0}])
    client = _client(handler)
    resultats = client.turpe_variable(
        [
            {
                "id": "d",
                "formule_tarifaire_acheminement": "BTINFCUST",
                "debut": "2025-08-15T00:00:00+02:00",
                "energie_base_kwh": 100.0,
            }
        ]
    )
    assert resultats[0].id == "d"


def test_turpe_variable_garde_de_version():
    """Serveur en retard (v0 < v1) : la garde lève."""
    handler = _handler([{"id": "x", "turpe_variable_eur": 1.0}], version="0")
    client = _client(handler)
    with pytest.raises(ContractVersionError):
        client.turpe_variable([_ligne("x")])
