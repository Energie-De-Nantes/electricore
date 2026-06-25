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
    """/status est absorbé par /ingestion statut (#152) — plus de handler ni d'entrée menu."""
    tg_app = build_application("123:abc")

    enregistrees: set[str] = set()
    for handlers in tg_app.handlers.values():
        for handler in handlers:
            enregistrees |= set(getattr(handler, "commands", ()))

    assert "status" not in enregistrees
    assert "status" not in {c for c, _ in COMMANDES}


def test_les_commandes_v1_absorbees_sont_hors_surface():
    """Les commandes v1 sont toutes absorbées par leurs domaines (#152–#156)."""
    tg_app = build_application("123:abc")

    enregistrees: set[str] = set()
    for handlers in tg_app.handlers.values():
        for handler in handlers:
            enregistrees |= set(getattr(handler, "commands", ()))

    absorbees = {"stats", "export", "entrees", "sorties", "check"}
    assert absorbees & enregistrees == set()
    assert absorbees & {c for c, _ in COMMANDES} == set()


def test_les_callbacks_des_domaines_sont_routes():
    """Un CallbackQueryHandler couvre chaque domaine à clavier."""
    from telegram.ext import CallbackQueryHandler

    tg_app = build_application("123:abc")
    patterns = [
        h.pattern for handlers in tg_app.handlers.values() for h in handlers if isinstance(h, CallbackQueryHandler)
    ]
    assert any(p.match("ingestion:menu") for p in patterns)
    assert not any(p.match("etl:menu") for p in patterns), "alias /etl retiré (#193) : etl:* ne route plus"
    assert any(p.match("flux:menu") for p in patterns)
    assert any(p.match("perimetre:menu") for p in patterns)
    assert any(p.match("taxes:menu") for p in patterns)
    assert any(p.match("facturation:menu") for p in patterns)


def test_le_demarrage_est_branche_en_post_init():
    from electricore.bot.app import demarrer

    tg_app = build_application("123:abc")
    assert tg_app.post_init is demarrer


def _taches_creees_par_demarrer(monkeypatch, chat_id: str) -> int:
    """Lance `demarrer` avec la config donnée, compte puis annule les tâches de fond."""
    from electricore.bot import tasks
    from electricore.bot.app import demarrer

    # `demarrer` ne tourne qu'après le démarrage du bot, donc avec un token présent
    # (le domaine bot l'exige) : on le pose pour un test hermétique, sans .env de dépôt.
    monkeypatch.setenv("BOT__TOKEN", "123:abc")
    monkeypatch.setenv("BOT__NOTIFY_CHAT_ID", chat_id)
    monkeypatch.setattr(type(settings), "is_odoo_configured", property(lambda self: True))

    class FakeBot:
        async def set_my_commands(self, commands):
            pass

    async def scenario() -> int:
        avant = set(tasks._background_tasks)
        await demarrer(SimpleNamespace(bot=FakeBot()))
        nouvelles = set(tasks._background_tasks) - avant
        for t in nouvelles:
            t.cancel()
        return len(nouvelles)

    return asyncio.run(scenario())


def test_demarrer_sans_chat_de_notification_ne_surveille_pas(monkeypatch):
    assert _taches_creees_par_demarrer(monkeypatch, chat_id="") == 0


def test_demarrer_avec_chat_de_notification_lance_les_surveillances(monkeypatch):
    # deux boucles : jobs d'ingestion (#157) + péremption des taux régulés (#186)
    assert _taches_creees_par_demarrer(monkeypatch, chat_id="-100123") == 2


def test_publier_menu_pousse_la_surface_a_telegram(monkeypatch):
    monkeypatch.setattr(type(settings), "is_odoo_configured", property(lambda self: True))
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
