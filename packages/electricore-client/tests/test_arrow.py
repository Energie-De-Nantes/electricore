"""Tests du client Arrow (`electricore_client.arrow`, extra `[arrow]`).

Le code Arrow dépend de polars : ces tests sont **skippés** si polars n'est pas
installé (le job `test-client` polars-free de la CI les saute). Quand polars est
là, on vérifie le round-trip Arrow IPC via MockTransport + le passage des filtres.
Le message d'install clair (polars absent) est testé séparément par monkeypatch.
"""

from __future__ import annotations

import builtins
import importlib
import io

import httpx
import pytest

# Le client Arrow dépend de polars : sans lui, tout ce fichier est sauté.
pl = pytest.importorskip("polars")

from electricore_client.arrow import ElectricoreArrowClient  # noqa: E402


def _arrow_response(df) -> bytes:
    buf = io.BytesIO()
    df.write_ipc_stream(buf)
    return buf.getvalue()


def _client(handler) -> ElectricoreArrowClient:
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return ElectricoreArrowClient(url="http://testserver", api_key="key", http_client=http)


def test_flux_round_trip_arrow():
    """`flux(table)` round-trip un DataFrame servi en Arrow IPC."""
    attendu = pl.DataFrame({"pdl": ["12345678901234"], "evenement_declencheur": ["MES"]})

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("x-api-key") == "key"
        return httpx.Response(200, content=_arrow_response(attendu))

    df = _client(handler).flux("c15")
    assert df.to_dicts() == attendu.to_dicts()


def test_releves_propage_les_filtres():
    """`releves(prm=, source=, debut=, fin=)` propage les filtres en query string."""
    captures: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captures.append(request.url)
        return httpx.Response(200, content=_arrow_response(pl.DataFrame({"pdl": ["p"]})))

    _client(handler).releves(prm="p", source="flux_R151", debut="2025-01-01", fin="2025-03-31")
    params = captures[0].params
    assert params.get("prm") == "p"
    assert params.get("source") == "flux_R151"
    assert params.get("debut") == "2025-01-01"
    assert params.get("fin") == "2025-03-31"


def test_message_clair_si_polars_absent(monkeypatch):
    """Si polars est introuvable à l'appel, un `ModuleNotFoundError` explicite (extra `arrow`)."""
    import electricore_client.arrow as arrow_mod

    vrai_import = builtins.__import__

    def faux_import(name, *args, **kwargs):
        if name == "polars":
            raise ModuleNotFoundError("No module named 'polars'")
        return vrai_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", faux_import)
    client = ElectricoreArrowClient(url="http://x", api_key="k")
    with pytest.raises(ModuleNotFoundError, match="arrow"):
        client._get_arrow("/flux/c15.arrow", {})

    # Restaure pour les imports ultérieurs.
    importlib.reload(arrow_mod)


def test_declaration_cta_propage_le_trimestre():
    """`declaration_cta(trimestre)` lit `/taxes/cta/declaration.arrow` (#651)."""
    attendu = pl.DataFrame({"pdl": ["p"], "cta_eur": [1.5], "empreinte_trimestre": ["0123456789abcdef"]})
    captures: list[httpx.URL] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captures.append(request.url)
        return httpx.Response(200, content=_arrow_response(attendu))

    assert _client(handler).declaration_cta("2026-T3").equals(attendu)
    assert captures[0].path == "/taxes/cta/declaration.arrow"
    assert captures[0].params.get("trimestre") == "2026-T3"
