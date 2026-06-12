"""Tests du client HTTP du bot (`electricore/bot/client.py`).

Boucle complète contre l'app FastAPI réelle via `httpx.ASGITransport` : le bot
reste pur client HTTP (cf. tests/architecture/test_bot_topology.py), on vérifie
ici qu'il obtient bien ses livrables par l'API et non par import direct (#150).
"""

import asyncio
from contextlib import contextmanager

import httpx
import polars as pl
import pytest

from electricore.api.config import settings
from electricore.api.main import app
from electricore.api.security import get_current_api_key
from electricore.bot.client import APIError, ElectriCoreClient
from electricore.integrations.odoo import ResultatVerification


@pytest.fixture(autouse=True)
def _mock_odoo_configured(monkeypatch):
    """Force `settings.is_odoo_configured` à True (sinon les endpoints renvoient 501 en CI)."""
    monkeypatch.setattr(type(settings), "get_odoo_config", lambda self: {})


def test_get_check_odoo_xlsx_obtient_le_detail_via_l_api(monkeypatch):
    """Le client bot récupère le XLSX du check Odoo par l'endpoint dédié."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"

    @contextmanager
    def _fake_reader(config):
        yield None

    resultat = ResultatVerification(
        rsc_manquante=pl.DataFrame({"sale_order_id": [1], "name": ["S00042"], "x_pdl": ["14000000000000"]}),
        cfne_manquante=pl.DataFrame(schema={"sale_order_id": pl.Int64, "name": pl.Utf8, "x_pdl": pl.Utf8}),
        invoicing_state_counts=pl.DataFrame({"state": ["up_to_date"], "n": [12]}),
        factures_draft=pl.DataFrame(schema={"account_move_id": pl.Int64, "name": pl.Utf8}),
        lisses_quantite_1=pl.DataFrame(
            schema={"sale_order_id": pl.Int64, "name": pl.Utf8, "categ_names": pl.List(pl.Utf8)}
        ),
    )
    monkeypatch.setattr(type(settings), "get_odoo_config", lambda self: {"url": "https://odoo.example"})
    monkeypatch.setattr("electricore.api.services.check_facturation_service.OdooReader", _fake_reader)
    monkeypatch.setattr("electricore.api.services.check_facturation_service.verifier", lambda odoo: resultat)
    bot_client = ElectriCoreClient(transport=httpx.ASGITransport(app=app))
    try:
        xlsx_bytes = asyncio.run(bot_client.get_check_odoo_xlsx())
    finally:
        app.dependency_overrides.clear()

    assert xlsx_bytes[:4] == b"PK\x03\x04", "XLSX = container ZIP, doit commencer par PK\\x03\\x04"


def test_erreur_http_expose_le_detail_api():
    """En cas d'erreur HTTP, l'opérateur lit le `detail` renvoyé par l'API —
    pas la ligne de statut httpx brute (incident du 503 cryptique, #171)."""

    def _repond_503(request):
        return httpx.Response(503, json={"detail": "Ingestion en cours — réessaie dans quelques minutes."})

    bot_client = ElectriCoreClient(transport=httpx.MockTransport(_repond_503))

    with pytest.raises(APIError) as excinfo:
        asyncio.run(bot_client.get_table_info("c15"))

    assert "Ingestion en cours" in str(excinfo.value)
    assert excinfo.value.status_code == 503


def test_erreur_http_sans_detail_json_garde_la_ligne_de_statut():
    """Corps non-JSON (proxy, page HTML…) → repli sur le message httpx,
    qui identifie au moins le statut et l'URL."""

    def _repond_502(request):
        return httpx.Response(502, text="<html>Bad Gateway</html>")

    bot_client = ElectriCoreClient(transport=httpx.MockTransport(_repond_502))

    with pytest.raises(APIError) as excinfo:
        asyncio.run(bot_client.get_table_info("c15"))

    assert "502" in str(excinfo.value)


def test_verrou_duckdb_affiche_ingestion_en_cours_dans_le_chat(monkeypatch):
    """Scénario de l'incident #171, de bout en bout : base verrouillée par
    l'ingestion → l'opérateur lit « Ingestion en cours », pas un 503 cryptique."""
    from electricore.bot.livraison import envoyer_document
    from electricore.core.loaders.duckdb import DuckDBLockError
    from tests.unit.telegram_fakes import FakeBot

    def _verrou(*args, **kwargs):
        raise DuckDBLockError("IO Error: Conflicting lock is held")

    monkeypatch.setattr("electricore.api.routers.flux._load_flux_df", _verrou)
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    bot_client = ElectriCoreClient(transport=httpx.ASGITransport(app=app))
    bot = FakeBot()
    try:
        livre = asyncio.run(
            envoyer_document(
                bot,
                1,
                100,
                attente="Export r151",
                fetch=lambda: bot_client.get_xlsx("r151"),
                filename="r151.xlsx",
                caption="r151",
                libelle_erreur="Erreur export",
            )
        )
    finally:
        app.dependency_overrides.clear()

    assert livre is False
    _, _, texte, _ = bot.edits[-1]
    assert "Ingestion en cours" in texte
    assert "503 Service Unavailable" not in texte
