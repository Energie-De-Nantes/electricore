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
PEREMPTION_INTERVAL = 24 * 3600  # un check de péremption par jour suffit (rythmes annuels)


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


# =============================================================================
# Péremption des taux régulés (#186, ADR-0024)
# =============================================================================


async def verifier_peremption(bot, client: ElectriCoreClient, vus: set, chat_id: str) -> None:
    """Une itération du check : alerte les taux présumés périmés, une fois par jalon manqué.

    Le bot reste pur client HTTP : le check est calculé par l'API
    (`GET /taxes/peremption`, fonction core pure côté serveur). Warning
    uniquement, jamais d'auto-correction (ADR-0024).
    """
    try:
        avertissements = await client.get_peremption()
    except Exception:
        return  # API indisponible : on retentera au prochain passage

    for a in avertissements:
        cle = (a["taxe"], a["attendu_depuis"])
        if cle in vus:
            continue
        texte = f"⚠️ Taux régulé présumé périmé : {escape(a['message'])}"
        try:
            await bot.send_message(chat_id=chat_id, text=texte, parse_mode="HTML")
            vus.add(cle)
        except Exception:
            logger.exception("Échec d'envoi de l'alerte péremption")  # retentera au prochain passage


async def boucle_peremption(bot, chat_id: str) -> None:
    """Boucle quotidienne du check de péremption (annulée au shutdown via tasks)."""
    client = ElectriCoreClient()
    vus: set = set()
    logger.info("Surveillance péremption des taux régulés active (chat %s)", chat_id)
    await verifier_peremption(bot, client, vus, chat_id)  # check immédiat au démarrage
    while True:
        await asyncio.sleep(PEREMPTION_INTERVAL)
        await verifier_peremption(bot, client, vus, chat_id)
