"""Tests du module Livraison (`electricore/bot/livraison.py`) — #174.

Le squelette « ⏳ → action → ❌/📥 » est testé ici une seule fois ; les tests
des handlers ne couvrent plus que leur contenu métier.
"""

import asyncio

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from electricore.bot.livraison import envoyer_document, etape
from tests.unit.telegram_fakes import FakeBot

CLAVIER = InlineKeyboardMarkup([[InlineKeyboardButton("← Retour", callback_data="x:menu")]])


# =============================================================================
# etape — suivi de progression + filet d'erreur
# =============================================================================


def test_etape_edite_l_attente_puis_retourne_le_resultat():
    bot = FakeBot()

    async def action():
        return 42

    resultat = asyncio.run(etape(bot, 1, 100, attente="Calcul en cours", action=action))

    assert resultat == 42
    assert len(bot.edits) == 1
    _, _, texte, _ = bot.edits[0]
    assert texte == "⏳ Calcul en cours…"


def test_etape_signale_l_echec_et_retourne_none():
    bot = FakeBot()

    async def action():
        raise RuntimeError("boom")

    resultat = asyncio.run(etape(bot, 1, 100, attente="Calcul", action=action, libelle_erreur="Erreur Accise"))

    assert resultat is None
    _, _, texte, kwargs = bot.edits[-1]
    assert texte == "❌ Erreur Accise : <code>boom</code>"
    assert "reply_markup" not in kwargs


def test_etape_pose_le_clavier_d_erreur_quand_fourni():
    bot = FakeBot()

    async def action():
        raise RuntimeError("boom")

    asyncio.run(etape(bot, 1, 100, attente="x", action=action, clavier_erreur=CLAVIER))

    _, _, _, kwargs = bot.edits[-1]
    assert kwargs["reply_markup"] is CLAVIER


# =============================================================================
# envoyer_document — étape + document + confirmation
# =============================================================================


def test_envoyer_document_livre_puis_confirme():
    bot = FakeBot()

    async def fetch():
        return b"PK\x03\x04fake"

    livre = asyncio.run(
        envoyer_document(bot, 1, 100, attente="Génération", fetch=fetch, filename="accise.xlsx", caption="Accise")
    )

    assert livre is True
    [(chat_id, kwargs)] = bot.documents
    assert chat_id == 1
    assert kwargs["filename"] == "accise.xlsx"
    assert kwargs["caption"] == "Accise"
    assert kwargs["document"].read() == b"PK\x03\x04fake"
    _, _, texte, _ = bot.edits[-1]
    assert texte == "📥 <code>accise.xlsx</code> envoyé."


def test_envoyer_document_echec_du_fetch_aucun_document():
    bot = FakeBot()

    async def fetch():
        raise RuntimeError("API 503")

    livre = asyncio.run(
        envoyer_document(bot, 1, 100, attente="Génération", fetch=fetch, filename="f.xlsx", caption="c")
    )

    assert livre is False
    assert bot.documents == []
    _, _, texte, _ = bot.edits[-1]
    assert texte == "❌ Erreur : <code>API 503</code>"


def test_envoyer_document_echec_d_envoi_ne_laisse_pas_le_sablier_fige():
    """Filet de l'incident « ⏳ figé » : même un échec APRÈS le fetch édite le suivi."""
    bot = FakeBot()

    async def fetch():
        return b"ok"

    async def send_document_qui_explose(chat_id, **kwargs):
        raise RuntimeError("Request Entity Too Large")

    bot.send_document = send_document_qui_explose

    livre = asyncio.run(
        envoyer_document(bot, 1, 100, attente="Génération", fetch=fetch, filename="f.xlsx", caption="c")
    )

    assert livre is False
    _, _, texte, _ = bot.edits[-1]
    assert texte.startswith("❌"), "le message de suivi doit signaler l'échec, pas rester sur ⏳"


def test_envoyer_document_clavier_de_confirmation():
    bot = FakeBot()

    async def fetch():
        return b"ok"

    asyncio.run(
        envoyer_document(
            bot, 1, 100, attente="x", fetch=fetch, filename="f.xlsx", caption="c", clavier_confirmation=CLAVIER
        )
    )

    _, _, texte, kwargs = bot.edits[-1]
    assert texte == "📥 <code>f.xlsx</code> envoyé."
    assert kwargs["reply_markup"] is CLAVIER
