"""Domaine `/taxes` : Accise TICFE et CTA (#155, ADR-0022).

Navigation par boutons (type de taxe → trimestre, dérivé de la date courante)
ou raccourcis texte v1 inchangés : `/taxes accise [trimestre]`, `/taxes cta
[trimestre]`.
"""

from datetime import date

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from electricore.bot.auth import require_allowed, require_odoo
from electricore.bot.client import ElectriCoreClient
from electricore.bot.format import escape
from electricore.bot.livraison import envoyer_document

_TITRE_MENU = "<b>Taxes</b> — choisis un calcul :"
_USAGE = "Usage :\n  /taxes accise [trimestre]  (ex: /taxes accise 2025-T1)\n  /taxes cta [trimestre]"

_LIBELLES = {"accise": "Accise TICFE", "cta": "CTA"}


def derniers_trimestres(aujourd_hui: date, n: int = 4) -> list[str]:
    """Les n derniers trimestres révolus, du plus récent au plus ancien."""
    annee, trimestre = aujourd_hui.year, (aujourd_hui.month - 1) // 3 + 1
    resultat = []
    for _ in range(n):
        trimestre -= 1
        if trimestre == 0:
            annee, trimestre = annee - 1, 4
        resultat.append(f"{annee}-T{trimestre}")
    return resultat


def clavier_principal() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🧾 Accise", callback_data="taxes:choix:accise"),
                InlineKeyboardButton("📊 CTA", callback_data="taxes:choix:cta"),
            ]
        ]
    )


def clavier_trimestres(taxe: str) -> InlineKeyboardMarkup:
    trimestres = derniers_trimestres(date.today())
    boutons = [InlineKeyboardButton(t, callback_data=f"taxes:run:{taxe}:{t}") for t in trimestres]
    lignes = [boutons[i : i + 2] for i in range(0, len(boutons), 2)]
    lignes.append([InlineKeyboardButton("📅 Toutes périodes", callback_data=f"taxes:run:{taxe}:all")])
    lignes.append([InlineKeyboardButton("← Retour", callback_data="taxes:menu")])
    return InlineKeyboardMarkup(lignes)


async def _envoyer_rapport(bot, chat_id: int, message_id: int, taxe: str, trimestre: str | None) -> None:
    libelle = _LIBELLES[taxe]
    periode = trimestre or "toutes périodes"
    client = ElectriCoreClient()
    fetch = (
        (lambda: client.get_accise_xlsx(trimestre)) if taxe == "accise" else (lambda: client.get_cta_xlsx(trimestre))
    )
    suffixe = f"_{trimestre}" if trimestre else ""
    await envoyer_document(
        bot,
        chat_id,
        message_id,
        attente=f"Calcul {libelle} — {escape(periode)}",
        fetch=fetch,
        filename=f"{taxe}{suffixe}.xlsx",
        caption=f"{libelle} — {periode}",
        libelle_erreur=f"Erreur {libelle}",
    )


@require_allowed
@require_odoo
async def cmd_taxes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.effective_message.reply_html(_TITRE_MENU, reply_markup=clavier_principal())
        return

    taxe = context.args[0].lower()
    if taxe not in _LIBELLES:
        await update.effective_message.reply_text(_USAGE)
        return
    trimestre = context.args[1] if len(context.args) > 1 else None
    msg = await update.effective_message.reply_html(f"⏳ Calcul {_LIBELLES[taxe]}…")
    await _envoyer_rapport(context.bot, msg.chat_id, msg.message_id, taxe, trimestre)


@require_allowed
@require_odoo
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id, message_id = query.message.chat_id, query.message.message_id
    data = query.data

    if data == "taxes:menu":
        await context.bot.edit_message_text(
            _TITRE_MENU, chat_id=chat_id, message_id=message_id, parse_mode="HTML", reply_markup=clavier_principal()
        )
    elif data.startswith("taxes:choix:"):
        taxe = data.removeprefix("taxes:choix:")
        await context.bot.edit_message_text(
            f"<b>{_LIBELLES[taxe]}</b> — choisis la période :",
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
            reply_markup=clavier_trimestres(taxe),
        )
    elif data.startswith("taxes:run:"):
        taxe, _, periode = data.removeprefix("taxes:run:").partition(":")
        trimestre = None if periode == "all" else periode
        await _envoyer_rapport(context.bot, chat_id, message_id, taxe, trimestre)
