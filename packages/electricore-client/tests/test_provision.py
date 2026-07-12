"""Tests du client `provision_estimation` (GET RPC, sans serveur, #487/#630).

L'estimation de provision est une lecture GET simple (pas un stream) : on envoie un
`pdl`, on reçoit un rapport typé `{pdl, as_of, trouve, estimation}`. MockTransport sert
la réponse ; on vérifie le typage, le paramètre envoyé, la garde de version, et les
deux mappings d'erreur (précondition flux R67 absent, verrou d'ingestion).
"""

from __future__ import annotations

import httpx
import pytest
from electricore_client import ElectricoreClient
from electricore_client.exceptions import ContractVersionError, IngestionEnCours, PreconditionNonRemplie
from electricore_client.models import EstimationProvision, RapportProvision

_ESTIMATION = {
    "pdl": "12345678901234",
    "energie_base_kwh": 1200.0,
    "energie_hp_kwh": None,
    "energie_hc_kwh": None,
    "energie_hph_kwh": None,
    "energie_hpb_kwh": None,
    "energie_hch_kwh": None,
    "energie_hcb_kwh": None,
    "energie_base_mensuel_kwh": 100.0,
    "energie_hp_mensuel_kwh": None,
    "energie_hc_mensuel_kwh": None,
    "energie_hph_mensuel_kwh": None,
    "energie_hpb_mensuel_kwh": None,
    "energie_hch_mensuel_kwh": None,
    "energie_hcb_mensuel_kwh": None,
    "couverture_debut": "2025-06-01",
    "couverture_fin": "2026-06-01",
    "couverture_mois": 12.0,
    "couverture_suffisante": True,
    "profondeur_cadran": "base",
    "qualite": "réelle",
    "presence_regularisation": False,
    "signal_alertable": False,
}


def _handler(body, *, version="1"):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body, headers={"X-Contract-Version": version})

    return handler


def _client(handler) -> ElectricoreClient:
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return ElectricoreClient(url="http://testserver", api_key="key", http_client=http)


def test_provision_estimation_retourne_rapport_type():
    """Le client GET et renvoie un `RapportProvision` typé, `estimation` incluse."""
    body = {"pdl": "12345678901234", "as_of": "2026-06-27", "trouve": True, "estimation": _ESTIMATION}
    client = _client(_handler(body))
    rapport = client.provision_estimation("12345678901234")
    assert isinstance(rapport, RapportProvision)
    assert rapport.pdl == "12345678901234"
    assert rapport.trouve is True
    assert isinstance(rapport.estimation, EstimationProvision)
    assert rapport.estimation.energie_base_kwh == 1200.0
    assert rapport.estimation.energie_base_mensuel_kwh == 100.0
    assert rapport.estimation.couverture_suffisante is True


def test_provision_estimation_pdl_non_trouve_estimation_none():
    """`trouve == False` → `estimation` reste `None` (aucune période R67 dans la fenêtre)."""
    body = {"pdl": "00000000000000", "as_of": "2026-06-27", "trouve": False, "estimation": None}
    client = _client(_handler(body))
    rapport = client.provision_estimation("00000000000000")
    assert rapport.trouve is False
    assert rapport.estimation is None


def test_provision_estimation_envoie_le_pdl_en_parametre():
    """Le `pdl` est envoyé en query param (pas dans un corps — GET, pas POST)."""
    capture = {}

    def handler(request: httpx.Request) -> httpx.Response:
        capture["params"] = dict(request.url.params)
        body = {"pdl": "X", "as_of": "2026-06-27", "trouve": False, "estimation": None}
        return httpx.Response(200, json=body, headers={"X-Contract-Version": "1"})

    client = _client(handler)
    client.provision_estimation("X")
    assert capture["params"] == {"pdl": "X"}


def test_provision_estimation_garde_de_version():
    """Un serveur **en retard** (contrat servi < attendu) lève `ContractVersionError`."""
    body = {"pdl": "X", "as_of": "2026-06-27", "trouve": False, "estimation": None}
    client = _client(_handler(body, version="0"))
    with pytest.raises(ContractVersionError):
        client.provision_estimation("X")


def test_provision_estimation_flux_r67_absent_leve_precondition_non_remplie():
    """503 + X-Error-Kind: precondition (flux_r67 non matérialisé) → `PreconditionNonRemplie`,
    détail du message serveur préservé (#630 : precondition n'est plus 422-only)."""
    detail_serveur = (
        "Données R67 indisponibles : le flux flux_r67 n'est pas matérialisé. "
        "Déclencher une demande M023 sur le portail SGE, l'ingérer, puis réessayer."
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            headers={"X-Error-Kind": "precondition"},
            json={"detail": detail_serveur},
        )

    client = _client(handler)
    with pytest.raises(PreconditionNonRemplie, match="M023"):
        client.provision_estimation("12345678901234")


def test_provision_estimation_verrou_ingestion_leve_ingestion_en_cours():
    """Non-régression : le 503 verrou d'ingestion (handler d'app, main.py) continue
    d'arriver en `IngestionEnCours`, pas affecté par le tag `precondition` du flux R67."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, headers={"X-Error-Kind": "ingestion-lock"})

    client = _client(handler)
    with pytest.raises(IngestionEnCours):
        client.provision_estimation("12345678901234")
