"""Tests du substrat de transport (`_BaseClient`).

Couvre les invariants partagés par tous les endpoints : en-tête `X-API-Key`,
conversion 503 → `IngestionEnCours`, propagation des autres erreurs HTTP, et la
garde de version de contrat asymétrique (warn si serveur en avance, raise si en
retard). Tout est testé via `httpx.MockTransport` (aucun serveur réel).
"""

import warnings

import httpx
import pytest
from electricore_client.exceptions import ContractVersionError, IngestionEnCours
from electricore_client.headers import EnTetesMeta
from electricore_client.transport import _BaseClient


def _client(handler) -> _BaseClient:
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return _BaseClient(url="http://testserver", api_key="secret-key", http_client=http)


def test_raise_for_status_mappe_503_sur_ingestion_en_cours():
    """Un 503 (base verrouillée par l'ingestion) → `IngestionEnCours`, pas une HTTPStatusError."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    client = _client(handler)
    response = client._http.get("http://testserver/x")
    with pytest.raises(IngestionEnCours):
        client._raise_for_status(response)


def test_raise_for_status_propage_les_autres_erreurs():
    """Tout autre statut d'erreur reste une `httpx.HTTPStatusError`."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    client = _client(handler)
    response = client._http.get("http://testserver/x")
    with pytest.raises(httpx.HTTPStatusError):
        client._raise_for_status(response)


def test_headers_portent_la_cle_api():
    """Le client positionne l'en-tête `X-API-Key`."""
    client = _client(lambda r: httpx.Response(200))
    assert client._headers == {"X-API-Key": "secret-key"}


def test_version_guard_warn_si_serveur_en_avance():
    """Serveur en avance (servie > attendue) : warn, pas d'erreur (additif toléré)."""
    with pytest.warns(UserWarning, match="contrat v4"):
        _BaseClient._verifier_version(attendue=3, servie=4)


def test_version_guard_raise_si_serveur_en_retard():
    """Serveur en retard (servie < attendue) : `ContractVersionError`."""
    with pytest.raises(ContractVersionError):
        _BaseClient._verifier_version(attendue=3, servie=2)


def test_version_guard_silencieux_si_egal():
    """Versions égales : ni warn ni raise."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _BaseClient._verifier_version(attendue=3, servie=3)


def test_entetes_meta_parse_les_headers():
    """`EnTetesMeta.from_headers` extrait version + mois/grain (insensible à la casse)."""
    meta = EnTetesMeta.from_headers({"X-Contract-Version": "3", "X-Mois": "2026-05-01", "X-Grain": "point"})
    assert meta.contract_version == 3
    assert meta.mois == "2026-05-01"
    assert meta.grain == "point"


def test_entetes_meta_exige_la_version():
    """Sans `X-Contract-Version`, le parse échoue (en-tête requis)."""
    with pytest.raises(ValueError, match="X-Contract-Version"):
        EnTetesMeta.from_headers({})
