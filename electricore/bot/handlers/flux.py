"""Domaine `/flux` : tables brutes Enedis — stats et exports (#153, ADR-0022).

Sans argument, `/flux` liste les tables disponibles, chacune avec ses actions
Stats / Export. Raccourcis : `/flux stats <table>`, `/flux export <table>`.
Remplace les commandes v1 `/stats`, `/export` et l'ancien `/flux` plat.
"""

import io

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from electricore.bot.auth import require_allowed
from electricore.bot.client import ElectriCoreClient
from electricore.bot.format import escape

_TITRE_MENU = "<b>Tables flux Enedis</b> — stats ou export :"


def clavier_tables(tables: list[str]) -> InlineKeyboardMarkup:
    lignes = [
        [
            InlineKeyboardButton(f"📊 {t}", callback_data=f"flux:stats:{t}"),
            InlineKeyboardButton("📥 xlsx", callback_data=f"flux:export:{t}"),
        ]
        for t in tables
    ]
    return InlineKeyboardMarkup(lignes)


def clavier_retour() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("← Retour", callback_data="flux:menu")]])


async def _texte_stats(client: ElectriCoreClient, table: str) -> str:
    try:
        info = await client.get_table_info(table)
    except Exception as e:
        return f"❌ Erreur pour <code>{escape(table)}</code> : <code>{escape(e)}</code>"
    count = info.get("count", "?")
    cols = len(info.get("columns", []))
    lignes = f"{count:,}" if isinstance(count, int) else str(count)
    return f"📊 <b>flux_{escape(table)}</b>\n  • Lignes : <code>{lignes}</code>\n  • Colonnes : <code>{cols}</code>"


async def _envoyer_export(bot, chat_id: int, message_id: int, client: ElectriCoreClient, table: str) -> None:
    """Génère l'export, l'envoie en document et confirme sur le message du clavier."""
    await bot.edit_message_text(
        f"⏳ Génération de l'export <code>{escape(table)}</code>…",
        chat_id=chat_id,
        message_id=message_id,
        parse_mode="HTML",
    )
    try:
        xlsx_bytes = await client.get_xlsx(table)
    except Exception as e:
        await bot.edit_message_text(
            f"❌ Erreur pour <code>{escape(table)}</code> : <code>{escape(e)}</code>",
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
            reply_markup=clavier_retour(),
        )
        return
    await bot.send_document(
        chat_id=chat_id,
        document=io.BytesIO(xlsx_bytes),
        filename=f"flux_{table}.xlsx",
        caption=f"Export flux_{table}",
    )
    await bot.edit_message_text(
        f"📥 <code>flux_{escape(table)}.xlsx</code> envoyé.",
        chat_id=chat_id,
        message_id=message_id,
        parse_mode="HTML",
        reply_markup=clavier_retour(),
    )


_USAGE = "Usage : /flux — ou /flux stats <table>, /flux export <table>"


@require_allowed
async def cmd_flux(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    client = ElectriCoreClient()
    if not context.args:
        try:
            tables = await client.list_tables()
        except Exception as e:
            await update.effective_message.reply_html(f"❌ Erreur : <code>{escape(e)}</code>")
            return
        await update.effective_message.reply_html(_TITRE_MENU, reply_markup=clavier_tables(tables))
        return

    action = context.args[0].lower()
    if action == "stats" and len(context.args) > 1:
        await update.effective_message.reply_html(await _texte_stats(client, context.args[1]))
        return
    if action == "export" and len(context.args) > 1:
        table = context.args[1]
        msg = await update.effective_message.reply_html(f"⏳ Génération de l'export <code>{escape(table)}</code>…")
        await _envoyer_export(context.bot, msg.chat_id, msg.message_id, client, table)
        return
    await update.effective_message.reply_text(_USAGE)


@require_allowed
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id, message_id = query.message.chat_id, query.message.message_id
    data = query.data
    client = ElectriCoreClient()

    if data == "flux:menu":
        try:
            tables = await client.list_tables()
        except Exception as e:
            await context.bot.edit_message_text(
                f"❌ Erreur : <code>{escape(e)}</code>", chat_id=chat_id, message_id=message_id, parse_mode="HTML"
            )
            return
        await context.bot.edit_message_text(
            _TITRE_MENU,
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
            reply_markup=clavier_tables(tables),
        )
    elif data.startswith("flux:stats:"):
        await context.bot.edit_message_text(
            await _texte_stats(client, data.removeprefix("flux:stats:")),
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
            reply_markup=clavier_retour(),
        )
    elif data.startswith("flux:export:"):
        await _envoyer_export(context.bot, chat_id, message_id, client, data.removeprefix("flux:export:"))
