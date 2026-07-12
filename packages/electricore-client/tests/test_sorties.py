"""Tests du client `sorties` (POST RPC, sans serveur, #632).

Fin de souscription gouvernée par le fait C15 (souscriptions_odoo#21, ADR 0031 côté
addon) : on envoie un lot de RSC, on reçoit une ligne par RSC **sortie** — une RSC
encore présente ou inconnue n'apparaît simplement pas. MockTransport échoue/réussit
l'écho ; on vérifie le typage, le corps envoyé, et la garde de version.
"""

from __future__ import annotations

import json

import httpx
import pytest
from electricore_client import ElectricoreClient
from electricore_client.exceptions import ContractVersionError
from electricore_client.models import LigneSortie


def _handler(sorties, *, version="1"):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"contract_version": int(version), "sorties": sorties},
            headers={"X-Contract-Version": version},
        )

    return handler


def _client(handler) -> ElectricoreClient:
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return ElectricoreClient(url="http://testserver", api_key="key", http_client=http)


def test_sorties_retourne_des_lignes_typees():
    """Le client POST le lot et renvoie des `LigneSortie` typées."""
    client = _client(
        _handler(
            [
                {
                    "ref_situation_contractuelle": "rA",
                    "pdl": "PDL-A",
                    "evenement_declencheur": "RES",
                    "date_sortie": "2026-03-15",
                }
            ]
        )
    )
    resultats = client.sorties(["rA"])
    assert all(isinstance(r, LigneSortie) for r in resultats)
    assert resultats[0].ref_situation_contractuelle == "rA"
    assert resultats[0].evenement_declencheur == "RES"
    assert resultats[0].date_sortie.isoformat() == "2026-03-15"


def test_sorties_envoie_le_lot_dans_le_corps():
    """Le corps POST porte bien `{rsc: [...]}` (contrat figé)."""
    capture = {}

    def handler(request: httpx.Request) -> httpx.Response:
        capture["body"] = json.loads(request.content)
        return httpx.Response(200, json={"contract_version": 1, "sorties": []}, headers={"X-Contract-Version": "1"})

    client = _client(handler)
    client.sorties(["rA", "rB"])
    assert capture["body"] == {"rsc": ["rA", "rB"]}


def test_sorties_seules_les_rsc_sorties_reviennent():
    """Un lot mixte : seules les RSC sorties reviennent (rB, encore présente, absente)."""
    client = _client(
        _handler(
            [
                {
                    "ref_situation_contractuelle": "rA",
                    "pdl": "PDL-A",
                    "evenement_declencheur": "CFNS",
                    "date_sortie": "2026-05-02",
                }
            ]
        )
    )
    resultats = client.sorties(["rA", "rB"])
    assert [r.ref_situation_contractuelle for r in resultats] == ["rA"]


def test_sorties_lot_vide():
    """Lot vide → liste vide (le round-trip HTTP a lieu, le court-circuit est côté serveur)."""
    client = _client(_handler([]))
    assert client.sorties([]) == []


def test_sorties_garde_de_version():
    """Un serveur **en retard** (contrat servi < attendu) lève `ContractVersionError`."""
    client = _client(_handler([], version="0"))
    with pytest.raises(ContractVersionError):
        client.sorties(["rA"])
