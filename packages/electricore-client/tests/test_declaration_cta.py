"""Tests du client `declaration_cta_xlsx` (octets du XLSX servi par l'API, #651).

Polars-free : le document est rendu tel quel (le même que livre le bot). On vérifie
le chemin, le paramètre, et le mapping de la précondition « trimestre non terminé ».
"""

from __future__ import annotations

import httpx
import pytest
from electricore_client import ElectricoreClient
from electricore_client.exceptions import PreconditionNonRemplie


def _client(handler) -> ElectricoreClient:
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return ElectricoreClient(url="http://testserver", api_key="key", http_client=http)


def test_declaration_cta_xlsx_rend_les_octets():
    captures: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captures.append(request.url)
        return httpx.Response(200, content=b"PK\x03\x04xlsx")

    assert _client(handler).declaration_cta_xlsx("2026-T3") == b"PK\x03\x04xlsx"
    assert captures[0].path == "/taxes/cta/declaration.xlsx"
    assert captures[0].params.get("trimestre") == "2026-T3"


def test_declaration_cta_xlsx_trimestre_non_termine():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            422,
            json={"detail": "Le trimestre 2026-T4 n'est pas terminé"},
            headers={"X-Error-Kind": "precondition"},
        )

    with pytest.raises(PreconditionNonRemplie, match="2026-T4"):
        _client(handler).declaration_cta_xlsx("2026-T4")
