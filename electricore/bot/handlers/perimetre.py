"""Domaine `/perimetre` : entrées et sorties du périmètre (#154, ADR-0022).

Vues métier du flux C15 — entrées (PMES, MES, CFNE) et sorties (RES, CFNS) —
vocabulaire du glossaire core. Remplace les commandes v1 /entrees et /sorties.
"""

import io

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from electricore.bot.auth import require_allowed
from electricore.bot.client import ElectriCoreClient
from electricore.bot.format import escape

_TITRE_MENU = "<b>Périmètre</b> — exports C15 :"
_USAGE = "Usage : /perimetre — ou /perimetre entrees, /perimetre sorties"

_EXPORTS = {
    "entrees": ("entrees_c15.xlsx", "Entrées C15 (PMES, MES, CFNE)"),
    "sorties": ("sorties_c15.xlsx", "Sorties C15 (RES, CFNS)"),
}


def clavier_principal() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📥 Entrées", callback_data="perimetre:entrees"),
                InlineKeyboardButton("📤 Sorties", callback_data="perimetre:sorties"),
            ]
        ]
    )


async def _envoyer_export(bot, chat_id: int, message_id: int, sens: str) -> None:
    filename, caption = _EXPORTS[sens]
    await bot.edit_message_text(
        f"⏳ Génération de l'export {sens}…", chat_id=chat_id, message_id=message_id, parse_mode="HTML"
    )
    client = ElectriCoreClient()
    try:
        if sens == "entrees":
            xlsx_bytes = await client.get_entrees_xlsx()
        else:
            xlsx_bytes = await client.get_sorties_xlsx()
    except Exception as e:
        await bot.edit_message_text(
            f"❌ Erreur : <code>{escape(e)}</code>", chat_id=chat_id, message_id=message_id, parse_mode="HTML"
        )
        return
    await bot.send_document(chat_id=chat_id, document=io.BytesIO(xlsx_bytes), filename=filename, caption=caption)
    await bot.edit_message_text(
        f"📥 <code>{filename}</code> envoyé.", chat_id=chat_id, message_id=message_id, parse_mode="HTML"
    )


@require_allowed
async def cmd_perimetre(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.effective_message.reply_html(_TITRE_MENU, reply_markup=clavier_principal())
        return

    sens = context.args[0].lower()
    if sens not in _EXPORTS:
        await update.effective_message.reply_text(_USAGE)
        return
    msg = await update.effective_message.reply_html(f"⏳ Génération de l'export {sens}…")
    await _envoyer_export(context.bot, msg.chat_id, msg.message_id, sens)


@require_allowed
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    sens = query.data.removeprefix("perimetre:")
    if sens in _EXPORTS:
        await _envoyer_export(context.bot, query.message.chat_id, query.message.message_id, sens)
