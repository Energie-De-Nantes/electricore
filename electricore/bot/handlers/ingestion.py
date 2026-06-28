"""Domaine `/ingestion` : ingestion — clavier inline, raccourcis, suivi de job (#152, ADR-0022).

Sans argument, `/ingestion` ouvre le clavier du domaine ; avec arguments, raccourci
power-user (`/ingestion rebuild`, `/ingestion r151 c15`…). `resync` exige une confirmation
à deux taps. Le suivi de job édite le message de lancement au lieu d'en envoyer
un nouveau.
"""

import asyncio

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from electricore.bot.auth import require_allowed
from electricore.bot.client import ElectriCoreClient
from electricore.bot.format import escape
from electricore.bot.tasks import create_task

POLL_INTERVAL = 5  # secondes entre deux interrogations du job
POLL_MAX = 360  # ≈ 30 minutes de suivi au maximum

# Clés de flux proposées dans le sous-menu (miroir de FLUX_CONNUS côté API).
FLUX = ("c15", "f12", "f15", "r15", "r151", "r64", "r67")

_STATUS_EMOJI = {
    "running": "⏳",
    "completed": "✅",
    "failed": "❌",
}

_TITRE_MENU = "<b>Ingestion</b> — choisis une action :"


def clavier_principal() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("▶️ All", callback_data="ingestion:run:all"),
                InlineKeyboardButton("🧪 Test", callback_data="ingestion:run:test"),
            ],
            [
                InlineKeyboardButton("🔨 Rebuild", callback_data="ingestion:run:rebuild"),
                InlineKeyboardButton("📡 Flux…", callback_data="ingestion:flux"),
            ],
            [
                InlineKeyboardButton("♻️ Resync", callback_data="ingestion:resync"),
                InlineKeyboardButton("📊 Statut", callback_data="ingestion:statut"),
            ],
        ]
    )


def clavier_flux() -> InlineKeyboardMarkup:
    boutons = [InlineKeyboardButton(f, callback_data=f"ingestion:run:{f}") for f in FLUX]
    lignes = [boutons[i : i + 3] for i in range(0, len(boutons), 3)]
    lignes.append([InlineKeyboardButton("← Retour", callback_data="ingestion:menu")])
    return InlineKeyboardMarkup(lignes)


def clavier_confirmation_resync() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Confirmer", callback_data="ingestion:resync:go"),
                InlineKeyboardButton("✖️ Annuler", callback_data="ingestion:menu"),
            ]
        ]
    )


_AVERTISSEMENT_RESYNC = (
    "⚠️ <b>Resync</b> purge l'état incrémental dlt et <b>re-télécharge tout le SFTP</b> (~10 min).\n\nConfirmer ?"
)


def _texte_suivi(mode: str, job_id: str, statut: str) -> str:
    emoji = _STATUS_EMOJI.get(statut, "❓")
    return f"{emoji} Pipeline <code>{escape(mode)}</code> — job <code>{job_id[:8]}</code> — <b>{escape(statut)}</b>"


async def _suivre(bot, chat_id: int, message_id: int, client: ElectriCoreClient, job_id: str, mode: str) -> None:
    """Interroge le job jusqu'au statut final et édite le message de lancement."""
    job: dict = {}
    statut = "running"
    for _ in range(POLL_MAX):
        await asyncio.sleep(POLL_INTERVAL)
        try:
            job = await client.get_job(job_id)
        except Exception:
            continue
        if job["status"] != "running":
            statut = job["status"]
            break

    texte = _texte_suivi(mode, job_id, statut)
    if job.get("error"):
        texte += f"\n\n<code>{escape(job['error'][:500])}</code>"
    elif job.get("output"):
        texte += f"\n\n<pre>{escape(job['output'][:800])}</pre>"
    await bot.edit_message_text(texte, chat_id=chat_id, message_id=message_id, parse_mode="HTML")


async def _demarrer_et_suivre(bot, chat_id: int, message_id: int, mode: str) -> None:
    """Lance le pipeline puis suit le job en éditant toujours le même message."""
    client = ElectriCoreClient()
    try:
        job = await client.run_ingestion(mode)
    except Exception as e:
        await bot.edit_message_text(
            f"❌ Erreur au lancement : <code>{escape(e)}</code>",
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
        )
        return

    job_id = job["id"]
    await bot.edit_message_text(
        _texte_suivi(mode, job_id, "running"), chat_id=chat_id, message_id=message_id, parse_mode="HTML"
    )
    create_task(_suivre(bot, chat_id, message_id, client, job_id, mode))


def clavier_retour() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("← Retour", callback_data="ingestion:menu")]])


async def _texte_statut() -> str:
    client = ElectriCoreClient()
    try:
        jobs = await client.get_jobs(limit=5)
    except Exception as e:
        return f"❌ Erreur : <code>{escape(e)}</code>"
    if not jobs:
        return "Aucun job d'ingestion enregistré."
    lignes = ["<b>Derniers jobs d'ingestion :</b>", ""]
    for j in jobs:
        emoji = _STATUS_EMOJI.get(j["status"], "❓")
        debut = j["started_at"][:16].replace("T", " ")
        lignes.append(f"{emoji} <code>{j['id'][:8]}</code> — <b>{escape(j['mode'])}</b> — {debut}")
    return "\n".join(lignes)


@require_allowed
async def cmd_ingestion(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.effective_message.reply_html(_TITRE_MENU, reply_markup=clavier_principal())
        return

    mode = " ".join(context.args).lower()
    if mode in ("statut", "status"):
        await update.effective_message.reply_html(await _texte_statut())
        return
    if mode == "reset":
        await update.effective_message.reply_html(
            "Le mode <code>reset</code> est retiré — utilise <b>resync</b> (purge + re-téléchargement complet)."
        )
        return
    if mode == "resync":
        await update.effective_message.reply_html(_AVERTISSEMENT_RESYNC, reply_markup=clavier_confirmation_resync())
        return

    msg = await update.effective_message.reply_html(f"⏳ Lancement de <code>{escape(mode)}</code>…")
    await _demarrer_et_suivre(context.bot, msg.chat_id, msg.message_id, mode)


@require_allowed
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    chat_id, message_id = query.message.chat_id, query.message.message_id
    data = query.data

    if data == "ingestion:menu":
        await context.bot.edit_message_text(
            _TITRE_MENU, chat_id=chat_id, message_id=message_id, parse_mode="HTML", reply_markup=clavier_principal()
        )
    elif data == "ingestion:flux":
        await context.bot.edit_message_text(
            "<b>Ingestion ciblée</b> — choisis un flux :",
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
            reply_markup=clavier_flux(),
        )
    elif data == "ingestion:statut":
        await context.bot.edit_message_text(
            await _texte_statut(),
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
            reply_markup=clavier_retour(),
        )
    elif data == "ingestion:resync":
        await context.bot.edit_message_text(
            _AVERTISSEMENT_RESYNC,
            chat_id=chat_id,
            message_id=message_id,
            parse_mode="HTML",
            reply_markup=clavier_confirmation_resync(),
        )
    elif data == "ingestion:resync:go":
        await _demarrer_et_suivre(context.bot, chat_id, message_id, "resync")
    elif data.startswith("ingestion:run:"):
        await _demarrer_et_suivre(context.bot, chat_id, message_id, data.removeprefix("ingestion:run:"))
