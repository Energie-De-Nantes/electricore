"""
Bot Telegram pour ElectriCore.
Permet de lancer l'ETL, consulter les stats et exporter des données via Telegram.
"""

import asyncio
import io
import logging

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from electricore.api.config import settings
from electricore.bot.client import ElectriCoreClient

logger = logging.getLogger(__name__)

_background_tasks: set[asyncio.Task] = set()


def _create_task(coro) -> asyncio.Task:
    """Crée une tâche en la gardant en référence pour pouvoir l'annuler au shutdown."""
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


_HELP = r"""
*ElectriCore Bot* — Commandes disponibles :

/start — Ce message d'aide
/etl \[mode\] — Lancer l'ingestion ETL \(défaut : all\)
  Modes : `test`, `r151`, `all`, `reset`
/status — Statut des 5 derniers jobs ETL
/stats \[table\] — Stats d'une table flux
/export \[table\] — Exporter une table en fichier Excel
/entrees — Exporter les entrées C15 \(PMES, MES, CFNE\)
/sorties — Exporter les sorties C15 \(RES, CFNS\)
/flux — Lister les tables disponibles à l'export
/taxes accise \[trimestre\] — Exporter le calcul Accise TICFE en Excel
/taxes cta \[trimestre\] — Exporter le calcul CTA en Excel
/facturation \[YYYY\-MM\-DD\] — Exporter la réconciliation facturation Odoo ↔ Enedis
/check odoo — Vérifications pré\-facturation côté Odoo \(RSC, CFNE, draft, états\)
"""

_STATUS_EMOJI = {
    "running": "⏳",
    "completed": "✅",
    "failed": "❌",
}


def _is_allowed(update: Update) -> bool:
    allowed = settings.get_telegram_allowed_users()
    if not allowed:
        return False
    return update.effective_user.id in allowed


async def _deny(update: Update) -> None:
    await update.effective_message.reply_text("⛔ Accès refusé.")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_allowed(update):
        await _deny(update)
        return
    await update.effective_message.reply_markdown_v2(_HELP)


async def cmd_etl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_allowed(update):
        await _deny(update)
        return

    mode = context.args[0] if context.args else "all"
    valid_modes = {"test", "r151", "all", "reset"}
    if mode not in valid_modes:
        await update.effective_message.reply_text(f"Mode invalide : `{mode}`. Choix : {', '.join(valid_modes)}")
        return

    client = ElectriCoreClient()
    try:
        job = await client.run_etl(mode)
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Erreur au lancement : {e}")
        return

    job_id = job["id"]
    chat_id = update.effective_message.chat_id
    await update.effective_message.reply_text(f"⏳ Pipeline `{mode}` lancé (job `{job_id[:8]}…`). Je te notifie à la fin.")

    async def _notify():
        for _ in range(360):  # max 30 minutes
            await asyncio.sleep(5)
            try:
                j = await client.get_job(job_id)
            except Exception:
                continue
            if j["status"] != "running":
                break

        emoji = _STATUS_EMOJI.get(j["status"], "❓")
        msg = f"{emoji} Pipeline `{mode}` terminé — statut : *{j['status']}*"
        if j.get("error"):
            msg += f"\n\n`{j['error'][:500]}`"
        elif j.get("output"):
            msg += f"\n\n```\n{j['output'][:800]}\n```"
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")

    _create_task(_notify())


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_allowed(update):
        await _deny(update)
        return

    client = ElectriCoreClient()
    try:
        jobs = await client.get_jobs(limit=5)
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Erreur : {e}")
        return

    if not jobs:
        await update.effective_message.reply_text("Aucun job ETL enregistré.")
        return

    lines = ["*Derniers jobs ETL :*\n"]
    for j in jobs:
        emoji = _STATUS_EMOJI.get(j["status"], "❓")
        started = j["started_at"][:16].replace("T", " ")
        lines.append(f"{emoji} `{j['id'][:8]}…` — *{j['mode']}* — {started}")

    await update.effective_message.reply_markdown("\n".join(lines))


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_allowed(update):
        await _deny(update)
        return

    client = ElectriCoreClient()

    if not context.args:
        try:
            tables = await client.list_tables()
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Erreur : {e}")
            return
        await update.effective_message.reply_text(
            "Tables disponibles :\n" + "\n".join(f"  • {t}" for t in tables)
            + "\n\nUtilise /stats <table> pour les détails."
        )
        return

    table = context.args[0]
    try:
        info = await client.get_table_info(table)
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Erreur pour `{table}` : {e}")
        return

    count = info.get("count", "?")
    cols = len(info.get("columns", []))
    await update.effective_message.reply_markdown(
        f"📊 *flux_{table}*\n"
        f"  • Lignes : `{count:,}`\n"
        f"  • Colonnes : `{cols}`"
    )


async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_allowed(update):
        await _deny(update)
        return

    if not context.args:
        await update.effective_message.reply_text("Usage : /export <table>  (ex: /export r151)")
        return

    table = context.args[0]
    await update.effective_message.reply_text(f"⏳ Génération de l'export `{table}`…")

    client = ElectriCoreClient()
    try:
        xlsx_bytes = await client.get_xlsx(table)
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Erreur pour `{table}` : {e}")
        return

    await update.effective_message.reply_document(
        document=io.BytesIO(xlsx_bytes),
        filename=f"flux_{table}.xlsx",
        caption=f"Export flux_{table}",
    )


async def cmd_flux(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_allowed(update):
        await _deny(update)
        return
    client = ElectriCoreClient()
    try:
        tables = await client.list_tables()
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Erreur : {e}")
        return
    lines = ["📋 Tables disponibles à l'export :\n"]
    for t in tables:
        lines.append(f"  • /export {t}")
    lines += ["\nExports métier :", "  • /entrees — Entrées C15", "  • /sorties — Sorties C15"]
    await update.effective_message.reply_text("\n".join(lines))


async def cmd_entrees(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_allowed(update):
        await _deny(update)
        return
    await update.effective_message.reply_text("⏳ Génération de l'export entrées C15…")
    client = ElectriCoreClient()
    try:
        xlsx_bytes = await client.get_entrees_xlsx()
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Erreur : {e}")
        return
    await update.effective_message.reply_document(
        document=io.BytesIO(xlsx_bytes),
        filename="entrees_c15.xlsx",
        caption="Entrées C15 (PMES, MES, CFNE)",
    )


async def cmd_sorties(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_allowed(update):
        await _deny(update)
        return
    await update.effective_message.reply_text("⏳ Génération de l'export sorties C15…")
    client = ElectriCoreClient()
    try:
        xlsx_bytes = await client.get_sorties_xlsx()
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Erreur : {e}")
        return
    await update.effective_message.reply_document(
        document=io.BytesIO(xlsx_bytes),
        filename="sorties_c15.xlsx",
        caption="Sorties C15 (RES, CFNS)",
    )


async def cmd_taxes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_allowed(update):
        await _deny(update)
        return

    usage = (
        "Usage :\n"
        "  /taxes accise [trimestre]  — Accise TICFE (ex: /taxes accise 2025-T1)\n"
        "  /taxes cta [trimestre]     — CTA (ex: /taxes cta 2025-T1)"
    )

    if not context.args:
        await update.effective_message.reply_text(usage)
        return

    sous_commande = context.args[0].lower()

    if sous_commande == "accise":
        trimestre = context.args[1] if len(context.args) > 1 else None
        periode = f" — {trimestre}" if trimestre else " — toutes périodes"
        await update.effective_message.reply_text(f"⏳ Calcul Accise TICFE{periode}…")
        client = ElectriCoreClient()
        try:
            xlsx_bytes = await client.get_accise_xlsx(trimestre)
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Erreur Accise : {e}")
            return
        suffix = f"_{trimestre}" if trimestre else ""
        await update.effective_message.reply_document(
            document=io.BytesIO(xlsx_bytes),
            filename=f"accise{suffix}.xlsx",
            caption=f"Accise TICFE{periode}",
        )

    elif sous_commande == "cta":
        trimestre = context.args[1] if len(context.args) > 1 else None
        periode = f" — {trimestre}" if trimestre else " — toutes périodes"
        await update.effective_message.reply_text(f"⏳ Calcul CTA{periode}…")
        client = ElectriCoreClient()
        try:
            xlsx_bytes = await client.get_cta_xlsx(trimestre)
        except Exception as e:
            await update.effective_message.reply_text(f"❌ Erreur CTA : {e}")
            return
        suffix = f"_{trimestre}" if trimestre else ""
        await update.effective_message.reply_document(
            document=io.BytesIO(xlsx_bytes),
            filename=f"cta{suffix}.xlsx",
            caption=f"CTA{periode}",
        )

    else:
        await update.effective_message.reply_text(f"❌ Sous-commande inconnue : `{sous_commande}`\n\n{usage}")


async def cmd_facturation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_allowed(update):
        await _deny(update)
        return

    mois = context.args[0] if context.args else None
    periode = f" — {mois}" if mois else " — dernier mois disponible"
    await update.effective_message.reply_text(f"⏳ Génération des documents facturation{periode}…")

    client = ElectriCoreClient()
    try:
        zip_bytes = await client.get_facturation_documents(mois)
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Erreur : {e}")
        return

    suffix = f"_{mois[:7]}" if mois else ""
    await update.effective_message.reply_document(
        document=io.BytesIO(zip_bytes),
        filename=f"facturation{suffix}.zip",
        caption=f"Documents facturation Odoo ↔ Enedis{periode} (6 CSV)",
    )


_CHECK_LIMIT = 20  # Au-delà : renvoi en XLSX joint


def _md_escape(text: str) -> str:
    """Échappe les caractères Markdown V1 (mode 'Markdown', pas V2)."""
    return text.replace("_", r"\_").replace("*", r"\*").replace("`", r"\`").replace("[", r"\[")


def _format_check_odoo(result: dict) -> tuple[str, bool]:
    """Formatte le résultat en Markdown Telegram. Retourne (msg, xlsx_needed)."""
    lines = ["🔍 *Vérification Odoo*", ""]
    xlsx_needed = False

    def _bloc(titre: str, items: list[dict], label_fn, ok_msg: str) -> None:
        nonlocal xlsx_needed
        n = len(items)
        if n == 0:
            lines.append(f"✅ {ok_msg}")
            return
        lines.append(f"❌ *{n}* {titre}")
        for r in items[:_CHECK_LIMIT]:
            label = _md_escape(label_fn(r))
            lines.append(f"  • [{label}]({r['url']})")
        if n > _CHECK_LIMIT:
            lines.append(f"  _… et {n - _CHECK_LIMIT} autres → voir XLSX joint_")
            xlsx_needed = True

    _bloc("sale.order sans `x_ref_situation_contractuelle`",
          result["rsc_manquante"], lambda r: r["name"],
          "Tous les sale.order ont une RSC")
    lines.append("")
    _bloc("sale.order sans `x_date_cfne`",
          result["cfne_manquante"], lambda r: r["name"],
          "Tous les sale.order ont une date CFNE")
    lines.append("")

    counts = result["invoicing_state_counts"]
    lines.append("📊 *Répartition x_invoicing_state*")
    if counts:
        for state, n in counts.items():
            lines.append(f"  • `{state}` : {n}")
    else:
        lines.append("  _(aucune commande)_")
    lines.append("")

    _bloc("factures encore en draft",
          result["factures_draft"],
          lambda r: f"{r['sale_order_name']}{r['invoice_name']}",
          "Aucune facture en draft")
    lines.append("")

    _bloc("contrats lissés avec ligne énergie à qty=1",
          result.get("lisses_quantite_1", []),
          lambda r: f"{r['sale_order_name']} ({', '.join(r['categ_names'])})",
          "Aucun contrat lissé avec qty=1 sur Base/HP/HC")

    all_ok = not any([
        result["rsc_manquante"],
        result["cfne_manquante"],
        result["factures_draft"],
        result.get("lisses_quantite_1", []),
    ])
    if all_ok:
        lines.append("")
        lines.append("🟢 *OK pour lancer le cycle de facturation*")

    return "\n".join(lines), xlsx_needed


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_allowed(update):
        await _deny(update)
        return

    usage = (
        "Usage :\n"
        "  /check odoo  — Vérifications pré-facturation côté Odoo\n"
        "  (sources `enedis` et `croise` à venir)"
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
        await update.effective_message.reply_markdown(msg, disable_web_page_preview=True)

        if xlsx_needed:
            from electricore.api.services.check_facturation_service import generer_check_odoo_xlsx
            xlsx_bytes = await asyncio.get_event_loop().run_in_executor(
                None, generer_check_odoo_xlsx, result
            )
            await update.effective_message.reply_document(
                document=io.BytesIO(xlsx_bytes),
                filename="check_odoo.xlsx",
                caption="Détail complet des cas problématiques",
            )
    else:
        await update.effective_message.reply_text(f"❌ Source inconnue : `{source}`\n\n{usage}")


def build_application(token: str) -> Application:
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("etl", cmd_etl))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("export", cmd_export))
    app.add_handler(CommandHandler("flux", cmd_flux))
    app.add_handler(CommandHandler("entrees", cmd_entrees))
    app.add_handler(CommandHandler("sorties", cmd_sorties))
    app.add_handler(CommandHandler("taxes", cmd_taxes))
    app.add_handler(CommandHandler("facturation", cmd_facturation))
    app.add_handler(CommandHandler("check", cmd_check))
    return app
