"""
Handlers v1 du bot Telegram — en cours de migration par domaine (ADR-0022).

L'assemblage (application, surface, menu natif) vit dans [app.py](app.py) ;
chaque tranche #152–#156 migre un domaine vers `handlers/` et retire les
commandes correspondantes d'ici.
"""

import io
import logging

from telegram import Update
from telegram.ext import ContextTypes

from electricore.bot.auth import require_allowed
from electricore.bot.client import ElectriCoreClient
from electricore.bot.format import escape

logger = logging.getLogger(__name__)


@require_allowed
async def cmd_facturation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    mois = context.args[0] if context.args else None
    periode = f" — {mois}" if mois else " — dernier mois disponible"
    await update.effective_message.reply_text(f"⏳ Génération des documents facturation{periode}…")

    client = ElectriCoreClient()
    try:
        xlsx_bytes = await client.get_facturation_documents_xlsx(mois)
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Erreur : {e}")
        return

    suffix = f"_{mois[:7]}" if mois else ""
    await update.effective_message.reply_document(
        document=io.BytesIO(xlsx_bytes),
        filename=f"facturation{suffix}.xlsx",
        caption=f"Documents facturation Odoo ↔ Enedis{periode} (6 onglets)",
    )


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


@require_allowed
async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    usage = (
        "Usage :\n  /check odoo  — Vérifications pré-facturation côté Odoo\n  (sources `enedis` et `croise` à venir)"
    )

    if not context.args:
        await update.effective_message.reply_text(usage)
        return

    source = context.args[0].lower()

    if source == "odoo":
        await update.effective_message.reply_text("⏳ Vérifications côté Odoo…")
        client = ElectriCoreClient()
        try:
            result = await client.check_facturation_odoo()
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Erreur : {e}")
            return

        msg, xlsx_needed = _format_check_odoo(result)
        await update.effective_message.reply_html(msg, disable_web_page_preview=True)

        if xlsx_needed:
            try:
                xlsx_bytes = await client.get_check_odoo_xlsx()
            except Exception as e:
                await update.effective_message.reply_text(f"❌ Erreur export détail : {e}")
                return
            await update.effective_message.reply_document(
                document=io.BytesIO(xlsx_bytes),
                filename="check_odoo.xlsx",
                caption="Détail complet des cas problématiques",
            )
    else:
        await update.effective_message.reply_text(f"❌ Source inconnue : `{source}`\n\n{usage}")
