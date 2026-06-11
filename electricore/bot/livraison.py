"""Livraison : remise d'un livrable dans le chat (#174, voir CONTEXT.md).

Le squelette « éditer ⏳ → exécuter → éditer ❌ en cas d'échec → envoyer le
document → confirmer 📥 » vivait en quatre copies dans les handlers ; le filet
d'erreur posé après l'incident du ⏳ figé (72a1ed2) ne couvrait qu'une copie.
Deux primitives composables le portent désormais pour tous les domaines :

- `etape()` : suivi de progression + filet d'erreur — retourne le résultat de
  l'action, ou `None` si l'échec a déjà été signalé dans le chat ;
- `envoyer_document()` : étape + `send_document` + confirmation 📥, l'envoi
  lui-même restant sous le filet.
"""

import io
import logging
from collections.abc import Awaitable, Callable

from telegram import InlineKeyboardMarkup

from electricore.bot.format import escape

logger = logging.getLogger(__name__)


async def _signaler_echec(
    bot,
    chat_id: int,
    message_id: int,
    libelle_erreur: str,
    e: Exception,
    clavier_erreur: InlineKeyboardMarkup | None,
) -> None:
    kwargs = {"reply_markup": clavier_erreur} if clavier_erreur else {}
    await bot.edit_message_text(
        f"❌ {libelle_erreur} : <code>{escape(e)}</code>",
        chat_id=chat_id,
        message_id=message_id,
        parse_mode="HTML",
        **kwargs,
    )


async def etape[T](
    bot,
    chat_id: int,
    message_id: int,
    *,
    attente: str,
    action: Callable[[], Awaitable[T]],
    libelle_erreur: str = "Erreur",
    clavier_erreur: InlineKeyboardMarkup | None = None,
) -> T | None:
    """Exécute une action en éditant le message de suivi : ⏳ avant, ❌ si échec.

    Retourne `None` quand l'échec est déjà signalé dans le chat — le caller
    s'arrête là (`if resultat is None: return`), il n'a rien à rapporter.
    `attente` et `libelle_erreur` sont des fragments HTML déjà échappés.
    """
    await bot.edit_message_text(f"⏳ {attente}…", chat_id=chat_id, message_id=message_id, parse_mode="HTML")
    try:
        return await action()
    except Exception as e:
        logger.exception("Échec d'étape : %s", libelle_erreur)
        await _signaler_echec(bot, chat_id, message_id, libelle_erreur, e, clavier_erreur)
        return None


async def envoyer_document(
    bot,
    chat_id: int,
    message_id: int,
    *,
    attente: str,
    fetch: Callable[[], Awaitable[bytes]],
    filename: str,
    caption: str,
    libelle_erreur: str = "Erreur",
    clavier_erreur: InlineKeyboardMarkup | None = None,
    clavier_confirmation: InlineKeyboardMarkup | None = None,
) -> bool:
    """Livre un document : étape suivie, envoi, confirmation 📥.

    Retourne `True` si le document a été livré. L'envoi et la confirmation
    restent sous le filet — aucun chemin ne laisse le ⏳ figé.
    """
    contenu = await etape(
        bot,
        chat_id,
        message_id,
        attente=attente,
        action=fetch,
        libelle_erreur=libelle_erreur,
        clavier_erreur=clavier_erreur,
    )
    if contenu is None:
        return False
    try:
        await bot.send_document(chat_id=chat_id, document=io.BytesIO(contenu), filename=filename, caption=caption)
        kwargs = {"reply_markup": clavier_confirmation} if clavier_confirmation else {}
        await bot.edit_message_text(
            f"📥 <code>{escape(filename)}</code> envoyé.",
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
            **kwargs,
        )
    except Exception as e:
        logger.exception("Échec de livraison : %s", filename)
        await _signaler_echec(bot, chat_id, message_id, libelle_erreur, e, clavier_erreur)
        return False
    return True
