"""Tests de l'assemblage du bot (`electricore/bot/app.py`).

`build_application` enregistre toute la surface (start/help + commandes v1
non migrées) et publie le menu natif au démarrage (#151, ADR-0022).
"""

import asyncio
from types import SimpleNamespace

import pytest

from electricore.api.config import settings
from electricore.bot.app import build_application, publier_menu
from electricore.bot.handlers.start import COMMANDES


def test_build_application_enregistre_toute_la_surface():
    tg_app = build_application("123:abc")

    enregistrees: set[str] = set()
    for handlers in tg_app.handlers.values():
        for handler in handlers:
            enregistrees |= set(getattr(handler, "commands", ()))

    attendues = {commande for commande, _ in COMMANDES}
    assert attendues <= enregistrees, f"commandes sans handler : {attendues - enregistrees}"


def test_status_v1_est_supprime_de_la_surface():
    """/status est absorbé par /etl statut (#152) — plus de handler ni d'entrée menu."""
    tg_app = build_application("123:abc")

    enregistrees: set[str] = set()
    for handlers in tg_app.handlers.values():
        for handler in handlers:
            enregistrees |= set(getattr(handler, "commands", ()))

    assert "status" not in enregistrees
    assert "status" not in {c for c, _ in COMMANDES}


def test_les_callbacks_etl_sont_routes():
    """Un CallbackQueryHandler couvre le pattern etl: (claviers du domaine)."""
    from telegram.ext import CallbackQueryHandler

    tg_app = build_application("123:abc")
    patterns = [
        h.pattern.pattern
        for handlers in tg_app.handlers.values()
        for h in handlers
        if isinstance(h, CallbackQueryHandler)
    ]
    assert any(p.startswith("^etl:") for p in patterns)


def test_le_menu_natif_est_branche_au_demarrage():
    tg_app = build_application("123:abc")
    assert tg_app.post_init is publier_menu


def test_publier_menu_pousse_la_surface_a_telegram():
    publiees = []

    class FakeBot:
        async def set_my_commands(self, commands):
            publiees.extend(commands)

    asyncio.run(publier_menu(SimpleNamespace(bot=FakeBot())))

    assert [(c.command, c.description) for c in publiees] == COMMANDES


# =============================================================================
# Contrat global : l'allowlist protège chaque handler de la surface
# =============================================================================


class FakeMessage:
    def __init__(self):
        self.replies: list[str] = []

    async def reply_text(self, text: str, **kwargs):
        self.replies.append(text)

    async def reply_html(self, text: str, **kwargs):
        self.replies.append(text)


def _tous_les_callbacks():
    tg_app = build_application("123:abc")
    for handlers in tg_app.handlers.values():
        for handler in handlers:
            for commande in getattr(handler, "commands", ()):
                yield pytest.param(handler.callback, id=commande)


@pytest.mark.parametrize("callback", _tous_les_callbacks())
def test_chaque_handler_refuse_un_user_hors_allowlist(monkeypatch, callback):
    """Aucun handler de la surface n'est joignable hors allowlist (#151)."""
    monkeypatch.setattr(type(settings), "get_telegram_allowed_users", lambda self: {42})
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=666),
        effective_message=FakeMessage(),
    )

    asyncio.run(callback(update, context=None))

    assert update.effective_message.replies == ["⛔ Accès refusé."]
