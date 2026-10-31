"""Domaine `/taxes` : Accise TICFE et CTA (#155, ADR-0022).

Navigation par boutons (type de taxe → trimestre, dérivé de la date courante)
ou raccourcis texte v1 inchangés : `/taxes accise [trimestre]`, `/taxes cta
[trimestre]`.

La CTA est la déclaration parc entier sans Odoo (#746, ADR-0058) : le domaine
reste servi sur une instance sans ERP, seule l'accise (legacy, Odoo) y est
masquée et refusée.
"""

from datetime import date

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from electricore.api.config import settings
from electricore.bot.auth import require_allowed
from electricore.bot.client import ElectriCoreClient
from electricore.bot.format import escape
from electricore.bot.livraison import envoyer_document

_TITRE_MENU = "<b>Taxes</b> — choisis un calcul :"
_USAGE = (
    "Usage :\n  /taxes accise [trimestre]  (ex: /taxes accise 2025-T1)\n  /taxes cta [trimestre]\n  /taxes millesimes"
)

_LIBELLES = {"accise": "Accise TICFE", "cta": "CTA"}

_ACCISE_SANS_ERP = "⚠️ Instance sans ERP connecté — l'accise n'est pas disponible ici (la CTA, si)."


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
    taxes = [InlineKeyboardButton("📊 CTA", callback_data="taxes:choix:cta")]
    if settings.is_odoo_configured:
        taxes.insert(0, InlineKeyboardButton("🧾 Accise", callback_data="taxes:choix:accise"))
    return InlineKeyboardMarkup([taxes, [InlineKeyboardButton("📜 Millésimes", callback_data="taxes:millesimes")]])


async def _refuser_accise_sans_erp(update: Update, taxe: str) -> bool:
    """Refuse l'accise (legacy, lue dans Odoo) sur une instance sans ERP (#159)."""
    if taxe == "accise" and not settings.is_odoo_configured:
        await update.effective_message.reply_text(_ACCISE_SANS_ERP)
        return True
    return False


def formater_millesimes(millesimes: list[dict]) -> str:
    """Texte HTML des millésimes — ce que cette instance « sait » de la réglementation (ADR-0024)."""
    lignes = ["<b>Millésimes des taux régulés</b>"]
    for m in millesimes:
        if m["valeur"] is not None:
            entete = (
                f"• <b>{escape(m['taxe'])}</b> — {m['valeur']:g} {escape(m['unite'])} depuis le {m['date_vigueur']}"
            )
        else:
            entete = f"• <b>{escape(m['taxe'])}</b> — grille du {m['date_vigueur']}"
        lignes.append(f"{entete}\n  réf. {escape(m['reference'])}")
    return "\n".join(lignes)


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
async def cmd_taxes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.effective_message.reply_html(_TITRE_MENU, reply_markup=clavier_principal())
        return

    taxe = context.args[0].lower()
    if taxe == "millesimes":
        texte = formater_millesimes(await ElectriCoreClient().get_millesimes())
        await update.effective_message.reply_html(texte, disable_web_page_preview=True)
        return
    if taxe not in _LIBELLES:
        await update.effective_message.reply_text(_USAGE)
        return
    if await _refuser_accise_sans_erp(update, taxe):
        return
    trimestre = context.args[1] if len(context.args) > 1 else None
    msg = await update.effective_message.reply_html(f"⏳ Calcul {_LIBELLES[taxe]}…")
    await _envoyer_rapport(context.bot, msg.chat_id, msg.message_id, taxe, trimestre)


@require_allowed
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id, message_id = query.message.chat_id, query.message.message_id
    data = query.data

    if data == "taxes:menu":
        await context.bot.edit_message_text(
            _TITRE_MENU, chat_id=chat_id, message_id=message_id, parse_mode="HTML", reply_markup=clavier_principal()
        )
    elif data == "taxes:millesimes":
        texte = formater_millesimes(await ElectriCoreClient().get_millesimes())
        await context.bot.edit_message_text(
            texte,
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("← Retour", callback_data="taxes:menu")]]),
        )
    elif data.startswith("taxes:choix:"):
        taxe = data.removeprefix("taxes:choix:")
        if await _refuser_accise_sans_erp(update, taxe):
            return
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
        if await _refuser_accise_sans_erp(update, taxe):
            return
        await _envoyer_rapport(context.bot, chat_id, message_id, taxe, trimestre)
