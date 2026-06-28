"""Domaine `/flux` : tables brutes Enedis — stats et exports (#153, ADR-0022).

Sans argument, `/flux` liste les tables disponibles, chacune avec ses actions
Stats / Export. Raccourcis : `/flux stats <table>`, `/flux export <table>`.
Remplace les commandes v1 `/stats`, `/export` et l'ancien `/flux` plat.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from electricore.bot.auth import require_allowed
from electricore.bot.client import ElectriCoreClient
from electricore.bot.format import escape
from electricore.bot.livraison import envoyer_document

_TITRE_MENU = "<b>Tables flux Enedis</b> — stats ou export :"

# Descriptions courtes des tables (UI). Table absente du dict : listée sans description.
DESCRIPTIONS = {
    "c15": "Événements contractuels (périmètre)",
    "f12_detail": "Facturation distributeur (F12)",
    "f15_detail": "Factures détaillées (F15)",
    "r15": "Relevés quotidiens + événements",
    "r15_acc": "Relevés autoconsommation (R15)",
    "r151": "Relevés périodiques",
    "r64": "Relevés courbe (JSON)",
    "r67": "Mesures facturantes (énergie par période)",
}


def _texte_menu(tables: list[str]) -> str:
    lignes = [_TITRE_MENU, ""]
    for t in tables:
        description = DESCRIPTIONS.get(t)
        suffixe = f" — {escape(description)}" if description else ""
        lignes.append(f"• <b>{escape(t)}</b>{suffixe}")
    return "\n".join(lignes)


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
    texte = f"📊 <b>flux_{escape(table)}</b>\n  • Lignes : <code>{lignes}</code>\n  • Colonnes : <code>{cols}</code>"
    if info.get("derniere_date"):
        texte += f"\n  • Dernière donnée : <code>{escape(info['derniere_date'])}</code>"
    return texte


async def _envoyer_export(bot, chat_id: int, message_id: int, client: ElectriCoreClient, table: str) -> None:
    """Génère l'export, l'envoie en document et confirme sur le message du clavier."""
    await envoyer_document(
        bot,
        chat_id,
        message_id,
        attente=f"Génération de l'export <code>{escape(table)}</code>",
        fetch=lambda: client.get_xlsx(table),
        filename=f"flux_{table}.xlsx",
        caption=f"Export flux_{table}",
        libelle_erreur=f"Erreur pour <code>{escape(table)}</code>",
        clavier_erreur=clavier_retour(),
        clavier_confirmation=clavier_retour(),
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
        await update.effective_message.reply_html(_texte_menu(tables), reply_markup=clavier_tables(tables))
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
            _texte_menu(tables),
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
