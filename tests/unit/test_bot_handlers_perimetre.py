"""Tests du domaine `/perimetre` (`electricore/bot/handlers/perimetre.py`) — #154.

Entrées (PMES, MES, CFNE) et sorties (RES, CFNS) du périmètre, vues métier du
C15 — vocabulaire du glossaire core. Remplace /entrees et /sorties v1.
"""

import asyncio

import pytest

from electricore.api.config import settings
from electricore.bot.handlers import perimetre as handlers_perimetre
from tests.unit.telegram_fakes import (
    FakeBot,
    callbacks_du_markup,
    contexte,
    update_callback,
    update_commande,
)


class FakeClient:
    def __init__(self):
        self.exports: list[str] = []

    async def get_entrees_xlsx(self) -> bytes:
        self.exports.append("entrees")
        return b"PK\x03\x04entrees"

    async def get_sorties_xlsx(self) -> bytes:
        self.exports.append("sorties")
        return b"PK\x03\x04sorties"


@pytest.fixture(autouse=True)
def _autorise(monkeypatch):
    monkeypatch.setattr(type(settings), "get_telegram_allowed_users", lambda self: {42})


@pytest.fixture
def client(monkeypatch) -> FakeClient:
    fake = FakeClient()
    monkeypatch.setattr(handlers_perimetre, "ElectriCoreClient", lambda: fake)
    return fake


def test_perimetre_sans_argument_ouvre_le_clavier(client):
    update = update_commande()

    asyncio.run(handlers_perimetre.cmd_perimetre(update, contexte()))

    ((_, kwargs),) = update.effective_message.replies
    assert {"perimetre:entrees", "perimetre:sorties"} <= callbacks_du_markup(kwargs)
    assert client.exports == []


def test_callback_entrees_envoie_le_document(client):
    update = update_callback("perimetre:entrees")
    bot = FakeBot()

    asyncio.run(handlers_perimetre.on_callback(update, contexte(bot=bot)))

    assert client.exports == ["entrees"]
    ((_, kwargs),) = bot.documents
    assert kwargs["filename"] == "entrees_c15.xlsx"
    assert "PMES" in kwargs["caption"]


def test_callback_sorties_envoie_le_document(client):
    update = update_callback("perimetre:sorties")
    bot = FakeBot()

    asyncio.run(handlers_perimetre.on_callback(update, contexte(bot=bot)))

    assert client.exports == ["sorties"]
    ((_, kwargs),) = bot.documents
    assert kwargs["filename"] == "sorties_c15.xlsx"


@pytest.mark.parametrize("raccourci,export", [("entrees", "entrees"), ("sorties", "sorties")])
def test_raccourcis_texte(client, raccourci, export):
    update = update_commande()
    bot = FakeBot()

    asyncio.run(handlers_perimetre.cmd_perimetre(update, contexte(args=[raccourci], bot=bot)))

    assert client.exports == [export]


def test_raccourci_inconnu_affiche_l_usage(client):
    update = update_commande()

    asyncio.run(handlers_perimetre.cmd_perimetre(update, contexte(args=["bogus"])))

    ((texte, _),) = update.effective_message.replies
    assert "entrees" in texte and "sorties" in texte
    assert client.exports == []
