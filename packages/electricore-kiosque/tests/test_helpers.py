"""Tests des helpers clé API → client `electricore-client` (#705).

Transport HTTP mocké (`httpx.MockTransport`) : aucun réseau, même patron que
`electricore_client/tests/test_transport.py`.
"""

from __future__ import annotations

import json

import httpx
import pytest
from electricore_kiosque.helpers import CleApiRefusee, construire_client, recuperer_meta_periodes


@pytest.fixture(autouse=True)
def _api_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """`KIOSQUE__API_URL` requise (fail-fast, pas de défaut) — posée pour les tests
    qui ne portent pas spécifiquement sur cette configuration."""
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.test.invalide")


_LIGNE = {
    "ref_situation_contractuelle": "RSC123",
    "pdl": "PDL456",
    "mois_annee": "2026-05",
    "debut": "2026-05-01",
    "fin": "2026-06-01",
    "nb_jours": 31,
    "turpe_fixe_eur": 12.3,
    "source_hash": "abc123",
}


def _client(handler, *, cle: str = "une-cle") -> object:
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return construire_client(cle, http_client=http)


def test_construire_client_positionne_url_et_cle(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.exemple.fr")
    client = construire_client("ma-cle")
    assert client.url == "https://kiosque.exemple.fr"
    assert client.api_key == "ma-cle"


def test_recuperer_meta_periodes_retourne_les_lignes() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"X-Contract-Version": "3"},
            content=(json.dumps(_LIGNE) + "\n").encode(),
        )

    client = _client(handler)
    lignes = recuperer_meta_periodes(client)

    assert len(lignes) == 1
    assert lignes[0]["ref_situation_contractuelle"] == "RSC123"
    assert lignes[0]["turpe_fixe_eur"] == 12.3
    assert "releves_utilises" not in lignes[0]


def test_recuperer_meta_periodes_cle_refusee() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401)

    client = _client(handler)
    with pytest.raises(CleApiRefusee, match="admin"):
        recuperer_meta_periodes(client)


def test_recuperer_meta_periodes_propage_les_autres_erreurs() -> None:
    """Une erreur qui n'est pas une clé refusée reste une erreur HTTP normale."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = _client(handler)
    with pytest.raises(httpx.HTTPStatusError):
        recuperer_meta_periodes(client)
