"""Le lifespan de l'API démarre le bot SANS `run_polling` : python-telegram-bot
n'appelle alors pas `post_init` (seul `run_polling`/`run_webhook` le fait). Le
lifespan doit l'appeler lui-même, sinon menu natif, surveillance des jobs et
check de péremption ne démarrent jamais (alertes muettes en prod, oct. 2026).
"""

import asyncio
from types import SimpleNamespace

import electricore.bot.app as bot_app
from electricore.api import main
from electricore.api.config import APISettings


def test_le_lifespan_appelle_post_init(monkeypatch):
    appels: list[str] = []

    async def noter(nom):
        appels.append(nom)

    fake = SimpleNamespace(
        initialize=lambda: noter("initialize"),
        start=lambda: noter("start"),
        stop=lambda: noter("stop"),
        shutdown=lambda: noter("shutdown"),
        post_init=lambda application: noter("post_init"),
        updater=SimpleNamespace(start_polling=lambda: noter("start_polling"), stop=lambda: noter("updater.stop")),
    )
    monkeypatch.setattr(APISettings, "telegram_bot_token", property(lambda self: "123:abc"))
    monkeypatch.setattr(bot_app, "build_application", lambda token: fake)

    async def cycle():
        async with main.lifespan(main.app):
            pass

    asyncio.run(cycle())

    assert "post_init" in appels
    assert appels.index("initialize") < appels.index("post_init") < appels.index("start_polling")
