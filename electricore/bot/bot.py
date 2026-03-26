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

_HELP = r"""
*ElectriCore Bot* — Commandes disponibles :

/start — Ce message d'aide
/etl \[mode\] — Lancer l'ingestion ETL \(défaut : all\)
  Modes : `test`, `r151`, `all`, `reset`
/status — Statut des 5 derniers jobs ETL
/stats \[table\] — Stats d'une table flux
/export \[table\] — Exporter une table en fichier Excel
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
    await update.effective_message.reply_text(f"⏳ Pipeline `{mode}` lancé (job `{job_id[:8]}…`). Je te notifie à la fin.")

    # Poll jusqu'à completion
    for _ in range(360):  # max 30 minutes (360 × 5s)
        await asyncio.sleep(5)
        try:
            job = await client.get_job(job_id)
        except Exception:
            continue
        if job["status"] != "running":
            break

    emoji = _STATUS_EMOJI.get(job["status"], "❓")
    msg = f"{emoji} Pipeline `{mode}` terminé — statut : *{job['status']}*"
    if job.get("error"):
        msg += f"\n\n`{job['error'][:500]}`"
    elif job.get("output"):
        msg += f"\n\n```\n{job['output'][:800]}\n```"
    await update.effective_message.reply_markdown(msg)


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


def build_application(token: str) -> Application:
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("etl", cmd_etl))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("export", cmd_export))
    return app
