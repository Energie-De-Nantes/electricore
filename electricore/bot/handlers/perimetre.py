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

_TITRE_MENU = "<b>Périmètre</b> — exports C15 + affaires SGE en cours :"
_USAGE = "Usage : /perimetre — ou /perimetre entrees, /perimetre sorties, /perimetre pdls, /perimetre affaires"

_EXPORTS = {
    "entrees": ("entrees_c15.xlsx", "Entrées C15 (PMES, MES, CFNE)"),
    "sorties": ("sorties_c15.xlsx", "Sorties C15 (RES, CFNS)"),
    "pdls": ("perimetre_pdls.csv", "PDL du périmètre actif — pour une demande M023 (relevés quotidiens R64)"),
}

_TELEGRAM_MSG_MAX = 4096  # limite dure de l'API Telegram par message


def clavier_principal() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📥 Entrées", callback_data="perimetre:entrees"),
                InlineKeyboardButton("📤 Sorties", callback_data="perimetre:sorties"),
            ],
            [InlineKeyboardButton("🔢 PDL périmètre (M023)", callback_data="perimetre:pdls")],
            [InlineKeyboardButton("🗂 Affaires en cours", callback_data="perimetre:affaires")],
        ]
    )


def _formater_affaires(affaires: list[dict]) -> str:
    """Rend la vue cockpit des affaires non soldées (texte HTML Telegram)."""
    if not affaires:
        return "✅ Aucune affaire en cours."
    entete = f"<b>Affaires en cours</b> ({len(affaires)}) :"
    lignes = [entete]
    # La liste arrive triée par ancienneté décroissante : on garde les plus anciennes
    # (= les plus actionnables) et on tronque la queue si on dépasse la limite Telegram —
    # sinon edit_message_text échoue (>4096 car.) et le bot reste muet (cf. facturation.py).
    budget = _TELEGRAM_MSG_MAX - 100
    affichees = 0
    for a in affaires:
        prestation = a.get("prestation_libelle") or a.get("prestation") or "?"
        etat = a.get("dernier_etat_libelle") or a.get("dernier_etat") or "?"
        ligne = (
            f"• <code>{escape(a.get('pdl'))}</code> — {escape(prestation)} · "
            f"{escape(etat)} · {a.get('anciennete_jours')} j"
        )
        if sum(len(li) + 1 for li in lignes) + len(ligne) > budget:
            break
        lignes.append(ligne)
        affichees += 1
    if affichees < len(affaires):
        lignes.append(f"\n<i>… et {len(affaires) - affichees} autres (les plus anciennes affichées)</i>")
    return "\n".join(lignes)


async def _envoyer_affaires(bot, chat_id: int, message_id: int) -> None:
    """Vue texte (pas d'export) des affaires SGE en cours, hors AME."""
    client = ElectriCoreClient()
    try:
        affaires = await client.get_affaires_ouvertes()
    except Exception as e:
        await bot.edit_message_text(
            f"❌ Erreur : <code>{escape(e)}</code>", chat_id=chat_id, message_id=message_id, parse_mode="HTML"
        )
        return
    await bot.edit_message_text(_formater_affaires(affaires), chat_id=chat_id, message_id=message_id, parse_mode="HTML")


async def _envoyer_export(bot, chat_id: int, message_id: int, sens: str) -> None:
    filename, caption = _EXPORTS[sens]
    await bot.edit_message_text(
        f"⏳ Génération de l'export {sens}…", chat_id=chat_id, message_id=message_id, parse_mode="HTML"
    )
    client = ElectriCoreClient()
    fetch = {
        "entrees": client.get_entrees_xlsx,
        "sorties": client.get_sorties_xlsx,
        "pdls": client.get_perimetre_pdls_csv,
    }[sens]
    try:
        contenu = await fetch()
    except Exception as e:
        await bot.edit_message_text(
            f"❌ Erreur : <code>{escape(e)}</code>", chat_id=chat_id, message_id=message_id, parse_mode="HTML"
        )
        return
    await bot.send_document(chat_id=chat_id, document=io.BytesIO(contenu), filename=filename, caption=caption)
    await bot.edit_message_text(
        f"📥 <code>{filename}</code> envoyé.", chat_id=chat_id, message_id=message_id, parse_mode="HTML"
    )


@require_allowed
async def cmd_perimetre(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.effective_message.reply_html(_TITRE_MENU, reply_markup=clavier_principal())
        return

    sens = context.args[0].lower()
    if sens == "affaires":
        msg = await update.effective_message.reply_html("⏳ Affaires en cours…")
        await _envoyer_affaires(context.bot, msg.chat_id, msg.message_id)
        return
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
    if sens == "affaires":
        await _envoyer_affaires(context.bot, query.message.chat_id, query.message.message_id)
    elif sens in _EXPORTS:
        await _envoyer_export(context.bot, query.message.chat_id, query.message.message_id, sens)
