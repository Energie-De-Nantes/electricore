"""Domaine `/facturation` : documents de campagne et contrôles pré-facturation (#156, ADR-0022).

Documents : choix du mois par boutons (derniers mois révolus + dernier
disponible). Check Odoo : résumé HTML + XLSX de détail via l'API (#150) quand
les anomalies dépassent la limite d'affichage. Absorbe /facturation et /check v1.
"""

import io
from datetime import date

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from electricore.bot.auth import require_allowed
from electricore.bot.client import ElectriCoreClient
from electricore.bot.format import escape

_TITRE_MENU = "<b>Facturation</b> — choisis une action :"
_USAGE = "Usage :\n  /facturation documents [YYYY-MM-DD]\n  /facturation check"


def derniers_mois(aujourd_hui: date, n: int = 4) -> list[str]:
    """Les n derniers mois révolus au format YYYY-MM-01, du plus récent au plus ancien."""
    annee, mois = aujourd_hui.year, aujourd_hui.month
    resultat = []
    for _ in range(n):
        mois -= 1
        if mois == 0:
            annee, mois = annee - 1, 12
        resultat.append(f"{annee}-{mois:02}-01")
    return resultat


def clavier_principal() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📑 Documents", callback_data="facturation:documents"),
                InlineKeyboardButton("🔍 Check Odoo", callback_data="facturation:check"),
            ]
        ]
    )


def clavier_mois() -> InlineKeyboardMarkup:
    boutons = [
        InlineKeyboardButton(m[:7], callback_data=f"facturation:run:documents:{m}") for m in derniers_mois(date.today())
    ]
    lignes = [boutons[i : i + 2] for i in range(0, len(boutons), 2)]
    lignes.append([InlineKeyboardButton("📅 Dernier mois dispo", callback_data="facturation:run:documents:last")])
    lignes.append([InlineKeyboardButton("← Retour", callback_data="facturation:menu")])
    return InlineKeyboardMarkup(lignes)


_CHECK_LIMIT = 20  # Au-delà : renvoi en XLSX joint


def _format_check_odoo(result: dict) -> tuple[str, bool]:
    """Formatte le résultat en HTML Telegram. Retourne (msg, xlsx_needed)."""
    lines = ["🔍 <b>Vérification Odoo</b>", ""]
    xlsx_needed = False

    def _bloc(titre: str, items: list[dict], label_fn, ok_msg: str) -> None:
        nonlocal xlsx_needed
        n = len(items)
        if n == 0:
            lines.append(f"✅ {ok_msg}")
            return
        lines.append(f"❌ <b>{n}</b> {titre}")
        for r in items[:_CHECK_LIMIT]:
            lines.append(f'  • <a href="{escape(r["url"])}">{escape(label_fn(r))}</a>')
        if n > _CHECK_LIMIT:
            lines.append(f"  <i>… et {n - _CHECK_LIMIT} autres → voir XLSX joint</i>")
            xlsx_needed = True

    _bloc(
        "sale.order sans <code>x_ref_situation_contractuelle</code>",
        result["rsc_manquante"],
        lambda r: r["name"],
        "Tous les sale.order ont une RSC",
    )
    lines.append("")
    _bloc(
        "sale.order sans <code>x_date_cfne</code>",
        result["cfne_manquante"],
        lambda r: r["name"],
        "Tous les sale.order ont une date CFNE",
    )
    lines.append("")

    counts = result["invoicing_state_counts"]
    lines.append("📊 <b>Répartition x_invoicing_state</b>")
    if counts:
        for state, n in counts.items():
            lines.append(f"  • <code>{escape(state)}</code> : {n}")
    else:
        lines.append("  <i>(aucune commande)</i>")
    lines.append("")

    _bloc(
        "factures encore en draft",
        result["factures_draft"],
        lambda r: f"{r['sale_order_name']}{r['invoice_name']}",
        "Aucune facture en draft",
    )
    lines.append("")

    _bloc(
        "contrats lissés avec ligne énergie à qty=1",
        result.get("lisses_quantite_1", []),
        lambda r: f"{r['sale_order_name']} ({', '.join(r['categ_names'])})",
        "Aucun contrat lissé avec qty=1 sur Base/HP/HC",
    )

    all_ok = not any(
        [
            result["rsc_manquante"],
            result["cfne_manquante"],
            result["factures_draft"],
            result.get("lisses_quantite_1", []),
        ]
    )
    if all_ok:
        lines.append("")
        lines.append("🟢 <b>OK pour lancer le cycle de facturation</b>")

    return "\n".join(lines), xlsx_needed


async def _envoyer_documents(bot, chat_id: int, message_id: int, mois: str | None) -> None:
    periode = mois[:7] if mois else "dernier mois disponible"
    await bot.edit_message_text(
        f"⏳ Génération des documents facturation — {escape(periode)}…",
        chat_id=chat_id,
        message_id=message_id,
        parse_mode="HTML",
    )
    client = ElectriCoreClient()
    try:
        xlsx_bytes = await client.get_facturation_documents_xlsx(mois)
    except Exception as e:
        await bot.edit_message_text(
            f"❌ Erreur : <code>{escape(e)}</code>", chat_id=chat_id, message_id=message_id, parse_mode="HTML"
        )
        return
    suffixe = f"_{mois[:7]}" if mois else ""
    filename = f"facturation{suffixe}.xlsx"
    await bot.send_document(
        chat_id=chat_id,
        document=io.BytesIO(xlsx_bytes),
        filename=filename,
        caption=f"Documents facturation Odoo ↔ Enedis — {periode} (6 onglets)",
    )
    await bot.edit_message_text(
        f"📥 <code>{filename}</code> envoyé.", chat_id=chat_id, message_id=message_id, parse_mode="HTML"
    )


async def _executer_check(bot, chat_id: int, message_id: int) -> None:
    await bot.edit_message_text(
        "⏳ Vérifications côté Odoo…", chat_id=chat_id, message_id=message_id, parse_mode="HTML"
    )
    client = ElectriCoreClient()
    try:
        result = await client.check_facturation_odoo()
    except Exception as e:
        await bot.edit_message_text(
            f"❌ Erreur : <code>{escape(e)}</code>", chat_id=chat_id, message_id=message_id, parse_mode="HTML"
        )
        return

    msg, xlsx_needed = _format_check_odoo(result)
    await bot.edit_message_text(
        msg, chat_id=chat_id, message_id=message_id, parse_mode="HTML", disable_web_page_preview=True
    )
    if xlsx_needed:
        try:
            xlsx_bytes = await client.get_check_odoo_xlsx()
        except Exception as e:
            await bot.send_message(chat_id=chat_id, text=f"❌ Erreur export détail : {e}")
            return
        await bot.send_document(
            chat_id=chat_id,
            document=io.BytesIO(xlsx_bytes),
            filename="check_odoo.xlsx",
            caption="Détail complet des cas problématiques",
        )


@require_allowed
async def cmd_facturation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.effective_message.reply_html(_TITRE_MENU, reply_markup=clavier_principal())
        return

    action = context.args[0].lower()
    if action == "documents":
        mois = context.args[1] if len(context.args) > 1 else None
        msg = await update.effective_message.reply_html("⏳ Documents facturation…")
        await _envoyer_documents(context.bot, msg.chat_id, msg.message_id, mois)
        return
    if action == "check":
        msg = await update.effective_message.reply_html("⏳ Vérifications côté Odoo…")
        await _executer_check(context.bot, msg.chat_id, msg.message_id)
        return
    await update.effective_message.reply_text(_USAGE)


@require_allowed
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id, message_id = query.message.chat_id, query.message.message_id
    data = query.data

    if data == "facturation:menu":
        await context.bot.edit_message_text(
            _TITRE_MENU, chat_id=chat_id, message_id=message_id, parse_mode="HTML", reply_markup=clavier_principal()
        )
    elif data == "facturation:documents":
        await context.bot.edit_message_text(
            "<b>Documents facturation</b> — choisis le mois :",
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
            reply_markup=clavier_mois(),
        )
    elif data == "facturation:check":
        await _executer_check(context.bot, chat_id, message_id)
    elif data.startswith("facturation:run:documents:"):
        periode = data.removeprefix("facturation:run:documents:")
        mois = None if periode == "last" else periode
        await _envoyer_documents(context.bot, chat_id, message_id, mois)
