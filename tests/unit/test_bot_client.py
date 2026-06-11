"""Tests du client HTTP du bot (`electricore/bot/client.py`).

Boucle complète contre l'app FastAPI réelle via `httpx.ASGITransport` : le bot
reste pur client HTTP (cf. tests/architecture/test_bot_topology.py), on vérifie
ici qu'il obtient bien ses livrables par l'API et non par import direct (#150).
"""

import asyncio

import httpx
import pytest

from electricore.api.config import settings
from electricore.api.main import app
from electricore.api.security import get_current_api_key
from electricore.bot.client import ElectriCoreClient


@pytest.fixture(autouse=True)
def _mock_odoo_configured(monkeypatch):
    """Force `settings.is_odoo_configured` à True (sinon les endpoints renvoient 501 en CI)."""
    monkeypatch.setattr(type(settings), "get_odoo_config", lambda self: {})


def test_get_check_odoo_xlsx_obtient_le_detail_via_l_api(monkeypatch):
    """Le client bot récupère le XLSX du check Odoo par l'endpoint dédié."""
    app.dependency_overrides[get_current_api_key] = lambda: "test-key"
    monkeypatch.setattr(
        "electricore.api.routers.facturation.verifier_odoo",
        lambda: {
            "rsc_manquante": [{"id": 1, "name": "S00042", "url": "https://odoo.example/web#id=1"}],
            "cfne_manquante": [],
            "invoicing_state_counts": {"up_to_date": 12},
            "factures_draft": [],
            "lisses_quantite_1": [],
        },
    )
    bot_client = ElectriCoreClient(transport=httpx.ASGITransport(app=app))
    try:
        xlsx_bytes = asyncio.run(bot_client.get_check_odoo_xlsx())
    finally:
        app.dependency_overrides.clear()

    assert xlsx_bytes[:4] == b"PK\x03\x04", "XLSX = container ZIP, doit commencer par PK\\x03\\x04"
