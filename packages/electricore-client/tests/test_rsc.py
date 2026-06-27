"""Tests du client `resoudre_rsc` (POST RPC, sans serveur).

La résolution RSC est un POST batch (pas un stream) : on envoie un lot d'`id_Affaire`,
on reçoit un résultat typé par `id_affaire` opaque (jamais positionnel). MockTransport
échoue/réussit l'écho ; on vérifie le typage, l'indexation par id, et la garde de version.
"""

from __future__ import annotations

import json

import httpx
import pytest
from electricore_client import ElectricoreClient
from electricore_client.exceptions import ContractVersionError
from electricore_client.models import ResultatResolutionRsc


def _handler(results, *, version="1"):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"contract_version": int(version), "results": results},
            headers={"X-Contract-Version": version},
        )

    return handler


def _client(handler) -> ElectricoreClient:
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return ElectricoreClient(url="http://testserver", api_key="key", http_client=http)


def test_resoudre_rsc_retourne_resultats_typees():
    """Le client POST le lot et renvoie des `ResultatResolutionRsc` typés."""
    client = _client(_handler([{"id_affaire": "38233180", "ref_situation_contractuelle": "248912973"}]))
    resultats = client.resoudre_rsc(["38233180"])
    assert all(isinstance(r, ResultatResolutionRsc) for r in resultats)
    assert resultats[0].id_affaire == "38233180"
    assert resultats[0].ref_situation_contractuelle == "248912973"
    assert resultats[0].error is None


def test_resoudre_rsc_envoie_le_lot_dans_le_corps():
    """Le corps POST porte bien `{ids: [...]}` (contrat figé)."""
    capture = {}

    def handler(request: httpx.Request) -> httpx.Response:
        capture["body"] = json.loads(request.content)
        return httpx.Response(200, json={"contract_version": 1, "results": []}, headers={"X-Contract-Version": "1"})

    client = _client(handler)
    client.resoudre_rsc(["38233180", "AME001"])
    assert capture["body"] == {"ids": ["38233180", "AME001"]}


def test_resoudre_rsc_indexe_par_id_echoe_pas_par_position():
    """Les résultats sont appariés par l'`id_affaire` échoé, pas par l'ordre d'envoi."""
    client = _client(
        _handler(
            [
                {"id_affaire": "B", "ref_situation_contractuelle": "RSC-B"},
                {"id_affaire": "A", "ref_situation_contractuelle": "RSC-A"},
            ]
        )
    )
    resultats = client.resoudre_rsc(["A", "B"])
    par_id = {r.id_affaire: r.ref_situation_contractuelle for r in resultats}
    assert par_id == {"A": "RSC-A", "B": "RSC-B"}


def test_resoudre_rsc_porte_les_erreurs_par_id():
    """Une affaire inconnue revient avec `error` (xor `ref_situation_contractuelle`)."""
    client = _client(
        _handler(
            [
                {"id_affaire": "ok", "ref_situation_contractuelle": "RSC-1"},
                {"id_affaire": "ko", "error": "Affaire inconnue : ko"},
            ]
        )
    )
    par_id = {r.id_affaire: r for r in client.resoudre_rsc(["ok", "ko"])}
    assert par_id["ok"].ref_situation_contractuelle == "RSC-1"
    assert par_id["ko"].ref_situation_contractuelle is None
    assert "inconnue" in par_id["ko"].error.lower()


def test_resoudre_rsc_garde_de_version():
    """Un serveur **en retard** (contrat servi < attendu) lève `ContractVersionError`."""
    client = _client(_handler([], version="0"))
    with pytest.raises(ContractVersionError):
        client.resoudre_rsc(["x"])
