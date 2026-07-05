"""Tests du client `prestations` (flux JSONL typé, sans serveur).

MockTransport sert un corps JSONL (une ligne = une `PrestationF15`) + l'en-tête
de version ; on vérifie le typage des lignes, le passage du filtre `rsc` et la
garde de version de contrat.
"""

from __future__ import annotations

import json

import httpx
import pytest
from electricore_client import ElectricoreClient
from electricore_client.models import PrestationF15

_LIGNES = [
    {
        "reference": "a1b2c3d4e5f60718",
        "pdl": "99510061232830",
        "ref_situation_contractuelle": "833195558",
        "id_ev": "DCOUP_PEN",
        "nature_ev": "03",
        "libelle_ev": "Pénalité pour coupure réseau",
        "taux_tva_applicable": "NS",
        "prix_unitaire": -24.0,
        "quantite": 2.0,
        "montant_ht": -48.0,
        "date_debut": "2025-01-17",
        "date_fin": "2025-01-17",
        "num_facture": "3210619182009",
        "date_facture": "2025-02-05",
    },
    {
        "reference": "00ff00ff00ff00ff",
        "pdl": "99510099999999",
        "ref_situation_contractuelle": "835000001",
        "id_ev": "F180B",
        "nature_ev": "01",
        "libelle_ev": "Mise en service",
        "taux_tva_applicable": "20.00",
        "prix_unitaire": 30.37,
        "quantite": 1.0,
        "montant_ht": 30.37,
        "date_debut": "2025-02-03",
        "date_fin": "2025-02-03",
        "num_facture": "3210619182010",
        "date_facture": "2025-02-05",
    },
]


def _jsonl(lignes: list[dict]) -> bytes:
    return ("\n".join(json.dumps(ligne) for ligne in lignes) + "\n").encode()


def _handler(*, lignes=_LIGNES, version="1", requetes: list[httpx.Request] | None = None):
    def handler(request: httpx.Request) -> httpx.Response:
        if requetes is not None:
            requetes.append(request)
        return httpx.Response(
            200,
            content=_jsonl(lignes),
            headers={
                "Content-Type": "application/x-ndjson",
                "X-Contract-Version": version,
            },
        )

    return handler


def _client(handler) -> ElectricoreClient:
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return ElectricoreClient(url="http://testserver", api_key="key", http_client=http)


def test_prestations_streame_des_lignes_typees():
    """Le flux rend des `PrestationF15` typées ; `NS` reste une chaîne brute."""
    client = _client(_handler())
    with client.prestations() as stream:
        prestas = list(stream)

    assert all(isinstance(p, PrestationF15) for p in prestas)
    assert [p.reference for p in prestas] == ["a1b2c3d4e5f60718", "00ff00ff00ff00ff"]
    assert prestas[0].taux_tva_applicable == "NS"
    assert prestas[0].montant_ht == -48.0
    assert isinstance(prestas[1].prix_unitaire, float)


def test_prestations_passe_le_filtre_rsc():
    """Le paramètre `rsc` (répétable) part dans la query string."""
    requetes: list[httpx.Request] = []
    client = _client(_handler(requetes=requetes))
    with client.prestations(rsc=["833195558", "835000001"]) as stream:
        list(stream)

    assert requetes[0].url.params.get_list("rsc") == ["833195558", "835000001"]


def test_prestations_garde_de_version_asymetrique():
    """Serveur en avance (v2 > v1 attendu) : warn, pas d'échec — le contrat est
    additif-tolérant (`extra="ignore"`). La branche « serveur en retard » (erreur)
    n'est pas atteignable avec un contrat attendu v1."""
    client = _client(_handler(version="2"))
    with pytest.warns(UserWarning, match="contrat v2"):
        with client.prestations() as stream:
            prestas = list(stream)

    assert len(prestas) == 2
