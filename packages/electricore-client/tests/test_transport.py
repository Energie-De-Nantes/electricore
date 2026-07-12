"""Tests du substrat de transport (`_BaseClient`).

Couvre les invariants partagés par tous les endpoints : en-tête `X-API-Key`,
discrimination via X-Error-Kind (#424), propagation des autres erreurs HTTP, et la
garde de version de contrat asymétrique (warn si serveur en avance, raise si en
retard). Tout est testé via `httpx.MockTransport` (aucun serveur réel).

Contrat X-Error-Kind (3 familles, #424) :
1. 503 + X-Error-Kind: ingestion-lock → IngestionEnCours (verrou, réessayable).
2. 503 sans header               → httpx.HTTPStatusError (erreur générique).
3. X-Error-Kind: precondition (422 ou 503) → PreconditionNonRemplie (actionnable).
   Le mapping est **header-first, pas code-first** (#630) : une précondition métier
   peut être portée par un 422 (RSC non réconcilié) ou un 503 (flux R67 absent) — c'est
   `X-Error-Kind` qui discrimine, jamais le code HTTP seul.
"""

import json
import warnings

import httpx
import pytest
from electricore_client.exceptions import ContractVersionError, IngestionEnCours, PreconditionNonRemplie
from electricore_client.headers import EnTetesMeta
from electricore_client.transport import _BaseClient


def _client(handler) -> _BaseClient:
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return _BaseClient(url="http://testserver", api_key="secret-key", http_client=http)


# -- Famille 1 : 503 + X-Error-Kind: ingestion-lock → IngestionEnCours ----------


def test_raise_for_status_mappe_503_ingestion_lock_sur_ingestion_en_cours():
    """503 + X-Error-Kind: ingestion-lock → IngestionEnCours (#424)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, headers={"X-Error-Kind": "ingestion-lock"})

    client = _client(handler)
    response = client._http.get("http://testserver/x")
    with pytest.raises(IngestionEnCours):
        client._raise_for_status(response)


# -- Famille 2 : 503 sans header → HTTPStatusError (générique, pas IngestionEnCours) --


def test_raise_for_status_503_sans_header_nest_pas_ingestion_en_cours():
    """503 sans X-Error-Kind → HTTPStatusError, pas IngestionEnCours (#424).
    Un 503 générique (ex. proxy timeout) ne doit pas être maquillé en verrou."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    client = _client(handler)
    response = client._http.get("http://testserver/x")
    with pytest.raises(httpx.HTTPStatusError):
        client._raise_for_status(response)


# -- Famille 3 : 422 + X-Error-Kind: precondition → PreconditionNonRemplie ------


def test_raise_for_status_mappe_422_precondition_sur_precondition_non_remplie():
    """422 + X-Error-Kind: precondition → PreconditionNonRemplie avec le détail serveur (#424)."""
    detail_serveur = "Réconciliation RSC requise pour 2025-03-01 : 1 ligne(s) sans RSC."

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            422,
            headers={"X-Error-Kind": "precondition", "Content-Type": "application/json"},
            content=json.dumps({"detail": detail_serveur}).encode(),
        )

    client = _client(handler)
    response = client._http.get("http://testserver/x")
    with pytest.raises(PreconditionNonRemplie, match="RSC"):
        client._raise_for_status(response)


def test_raise_for_status_mappe_503_precondition_sur_precondition_non_remplie():
    """503 + X-Error-Kind: precondition → PreconditionNonRemplie aussi (#630) : le mapping
    suit le header, pas le code — precondition n'est plus 422-only (flux R67 absent,
    ADR-0048/#487, remonte en 503)."""
    detail_serveur = "Données R67 indisponibles : ... Déclencher une demande M023 ..."

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            headers={"X-Error-Kind": "precondition", "Content-Type": "application/json"},
            content=json.dumps({"detail": detail_serveur}).encode(),
        )

    client = _client(handler)
    response = client._http.get("http://testserver/x")
    with pytest.raises(PreconditionNonRemplie, match="M023"):
        client._raise_for_status(response)


def test_les_exceptions_sont_toutes_exposees_au_top_level():
    """Les 4 exceptions s'importent depuis `electricore_client` directement.

    Verrue relevée par l'intégration Odoo : `PreconditionNonRemplie` forçait
    `from electricore_client.exceptions import …` alors que ses trois sœurs
    étaient top-level."""
    import electricore_client

    for nom in (
        "ElectricoreClientError",
        "IngestionEnCours",
        "PreconditionNonRemplie",
        "ContractVersionError",
    ):
        assert nom in electricore_client.__all__
        assert issubclass(getattr(electricore_client, nom), Exception)


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
