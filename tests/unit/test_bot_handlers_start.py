"""Tests de `/start` et `/help` (`electricore/bot/handlers/start.py`).

L'aide annonce l'instance servie (convention `@<slug>_electricore_bot`,
ADR-0022) et liste la surface de commandes — même source de vérité que le
menu natif (#151).
"""

import asyncio
from types import SimpleNamespace

import pytest

from electricore.api.config import settings
from electricore.bot.handlers.start import COMMANDES, cmd_start


class FakeMessage:
    def __init__(self):
        self.html_replies: list[str] = []

    async def reply_html(self, text: str, **kwargs):
        self.html_replies.append(text)

    async def reply_text(self, text: str, **kwargs):  # refus allowlist
        self.html_replies.append(text)


def fake_update(user_id: int = 42) -> SimpleNamespace:
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=user_id),
        effective_message=FakeMessage(),
    )


@pytest.fixture(autouse=True)
def _instance_edn(monkeypatch):
    monkeypatch.setattr(type(settings), "get_telegram_allowed_users", lambda self: {42})
    monkeypatch.setattr(settings, "instance_slug", "edn")


def test_start_annonce_l_instance(monkeypatch):
    update = fake_update()
    asyncio.run(cmd_start(update, context=None))

    (aide,) = update.effective_message.html_replies
    assert "edn" in aide, "l'aide doit annoncer l'instance servie"


def test_start_liste_la_surface_de_commandes():
    update = fake_update()
    asyncio.run(cmd_start(update, context=None))

    (aide,) = update.effective_message.html_replies
    for commande, _description in COMMANDES:
        assert f"/{commande}" in aide


def test_start_sans_slug_reste_generique(monkeypatch):
    monkeypatch.setattr(settings, "instance_slug", "")
    update = fake_update()
    asyncio.run(cmd_start(update, context=None))

    (aide,) = update.effective_message.html_replies
    assert "ElectriCore" in aide
