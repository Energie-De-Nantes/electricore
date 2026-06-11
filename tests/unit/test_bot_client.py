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
from electricore.bot.client import ElectriCoreClient
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
