"""Tests du contrôle d'accès du bot (`electricore/bot/auth.py`).

L'allowlist Telegram est appliquée par le décorateur `@require_allowed` sur
chaque handler (#151) — remplace les checks répétés en tête de handler.
"""

import asyncio
from types import SimpleNamespace

import pytest

from electricore.api.config import settings
from electricore.bot.auth import require_allowed


class FakeMessage:
    def __init__(self):
        self.replies: list[str] = []

    async def reply_text(self, text: str, **kwargs):
        self.replies.append(text)


def fake_update(user_id: int) -> SimpleNamespace:
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=user_id),
        effective_message=FakeMessage(),
    )


@pytest.fixture
def allowlist_42(monkeypatch):
    monkeypatch.setattr(type(settings), "get_telegram_allowed_users", lambda self: {42})


def _handler_traceur():
    appels = []

    @require_allowed
    async def handler(update, context):
        appels.append(update.effective_user.id)

    return handler, appels


def test_refuse_un_user_hors_allowlist(allowlist_42):
    handler, appels = _handler_traceur()
    update = fake_update(user_id=666)

    asyncio.run(handler(update, context=None))

    assert appels == [], "le handler ne doit pas être exécuté"
    assert update.effective_message.replies == ["⛔ Accès refusé."]


def test_execute_pour_un_user_autorise(allowlist_42):
    handler, appels = _handler_traceur()
    update = fake_update(user_id=42)

    asyncio.run(handler(update, context=None))

    assert appels == [42]
    assert update.effective_message.replies == []


def test_refuse_tout_le_monde_si_allowlist_vide(monkeypatch):
    monkeypatch.setattr(type(settings), "get_telegram_allowed_users", lambda self: set())
    handler, appels = _handler_traceur()
    update = fake_update(user_id=42)

    asyncio.run(handler(update, context=None))

    assert appels == []
    assert update.effective_message.replies == ["⛔ Accès refusé."]
