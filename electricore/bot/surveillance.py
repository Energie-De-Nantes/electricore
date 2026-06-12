"""Surveillance proactive des jobs d'ingestion (#157, ADR-0022).

Le bot reste pur client HTTP : c'est lui qui interroge périodiquement
`/ingestion/jobs` et pousse une alerte dans le chat de notification
(`TELEGRAM_NOTIFY_CHAT_ID`) quand un job passe à `failed` — y compris les
jobs lancés par le scheduler, pas seulement ceux lancés depuis Telegram.

Au démarrage, l'historique est marqué « vu » sans alerte ; seuls les échecs
postérieurs alertent, sans doublon.
"""

import asyncio
import logging

from electricore.bot.client import ElectriCoreClient
from electricore.bot.format import escape

logger = logging.getLogger(__name__)

SURVEILLANCE_INTERVAL = 60  # secondes entre deux interrogations


async def initialiser_vus(client: ElectriCoreClient) -> set[str]:
    """Snapshot au démarrage : les jobs déjà terminés ne seront jamais alertés."""
    try:
        jobs = await client.get_jobs(limit=50)
    except Exception:
        return set()
    return {j["id"] for j in jobs if j["status"] != "running"}


async def verifier_jobs(bot, client: ElectriCoreClient, vus: set[str], chat_id: str) -> None:
    """Une itération de surveillance : alerte les jobs passés à `failed` jamais signalés."""
    try:
        jobs = await client.get_jobs(limit=20)
    except Exception:
        return  # API indisponible : on retentera à la prochaine itération

    for job in jobs:
        if job["id"] in vus or job["status"] == "running":
            continue
        vus.add(job["id"])
        if job["status"] != "failed":
            continue
        texte = f"🚨 Job d'ingestion <code>{escape(job['mode'])}</code> en échec — <code>{job['id'][:8]}</code>"
        if job.get("error"):
            texte += f"\n\n<code>{escape(job['error'][:500])}</code>"
        try:
            await bot.send_message(chat_id=chat_id, text=texte, parse_mode="HTML")
        except Exception:
            logger.exception("Échec d'envoi de l'alerte ingestion")
            vus.discard(job["id"])  # retentera à la prochaine itération


async def boucle_surveillance(bot, chat_id: str) -> None:
    """Boucle infinie de surveillance (annulée au shutdown via tasks)."""
    client = ElectriCoreClient()
    vus = await initialiser_vus(client)
    logger.info("Surveillance ingestion active (chat %s)", chat_id)
    while True:
        await asyncio.sleep(SURVEILLANCE_INTERVAL)
        await verifier_jobs(bot, client, vus, chat_id)
