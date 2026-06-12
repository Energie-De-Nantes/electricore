"""Tests de la dégradation sans ERP connecté (#159, P2.5, ADR-0016/0022).

Sur une instance sans ERP, le menu natif et l'aide masquent les domaines
ERP-dépendants (/taxes, /facturation) ; tapés quand même, ils répondent un
message explicite. /ingestion, /flux, /perimetre fonctionnent intégralement.
"""

import asyncio
from types import SimpleNamespace

import pytest

from electricore.api.config import settings
from electricore.bot.app import publier_menu
from electricore.bot.handlers import facturation as handlers_facturation
from electricore.bot.handlers import start as handlers_start
from electricore.bot.handlers import taxes as handlers_taxes
from tests.unit.telegram_fakes import contexte, update_callback, update_commande


@pytest.fixture(autouse=True)
def _autorise(monkeypatch):
    monkeypatch.setattr(type(settings), "get_telegram_allowed_users", lambda self: {42})


@pytest.fixture
def sans_erp(monkeypatch):
    monkeypatch.setattr(type(settings), "is_odoo_configured", property(lambda self: False))


@pytest.fixture
def avec_erp(monkeypatch):
    monkeypatch.setattr(type(settings), "is_odoo_configured", property(lambda self: True))


def _commandes_publiees() -> list[str]:
    publiees = []

    class FakeBot:
        async def set_my_commands(self, commands):
            publiees.extend(c.command for c in commands)

    asyncio.run(publier_menu(SimpleNamespace(bot=FakeBot())))
    return publiees


def test_le_menu_sans_erp_masque_taxes_et_facturation(sans_erp):
    publiees = _commandes_publiees()
    assert "taxes" not in publiees
    assert "facturation" not in publiees
    assert {"start", "ingestion", "flux", "perimetre"} <= set(publiees)


def test_le_menu_avec_erp_liste_tout(avec_erp):
    publiees = _commandes_publiees()
    assert {"taxes", "facturation"} <= set(publiees)


def test_l_aide_sans_erp_ne_liste_pas_les_domaines_erp(sans_erp):
    update = update_commande()
    asyncio.run(handlers_start.cmd_start(update, context=None))

    ((aide, _),) = update.effective_message.replies
    assert "/taxes" not in aide
    assert "/facturation" not in aide
    assert "/ingestion" in aide


def test_taxes_sans_erp_repond_un_message_explicite(sans_erp):
    update = update_commande()

    asyncio.run(handlers_taxes.cmd_taxes(update, contexte()))

    ((texte, kwargs),) = update.effective_message.replies
    assert "ERP" in texte
    assert "reply_markup" not in kwargs, "pas de clavier sur une instance sans ERP"


def test_callback_facturation_sans_erp_repond_un_message_explicite(sans_erp):
    update = update_callback("facturation:check")

    asyncio.run(handlers_facturation.on_callback(update, contexte()))

    assert any("ERP" in texte for texte, _ in update.callback_query.message.replies)
