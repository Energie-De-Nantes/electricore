"""Tests de la surveillance des jobs d'ingestion (`electricore/bot/surveillance.py`) — #157.

Le bot interroge périodiquement les jobs via l'API (pur client HTTP) et pousse
une alerte dans le chat de notification quand un job passe à `failed` — y
compris ceux lancés par le scheduler. Pas de doublon, pas d'alerte sur
l'historique antérieur au démarrage.
"""

import asyncio

from electricore.bot import surveillance
from tests.unit.telegram_fakes import FakeBot


class FakeClient:
    def __init__(self, jobs):
        self.jobs = jobs

    async def get_jobs(self, limit: int = 5) -> list[dict]:
        return self.jobs


def _job(job_id: str, status: str, mode: str = "all", error: str | None = None) -> dict:
    return {"id": job_id, "status": status, "mode": mode, "error": error, "started_at": "2026-06-12T03:00:00"}


def test_un_nouveau_job_failed_declenche_une_alerte():
    bot = FakeBot()
    client = FakeClient([_job("aaaa", "failed", mode="all", error="boom")])
    vus: set[str] = set()

    asyncio.run(surveillance.verifier_jobs(bot, client, vus, chat_id="-100123"))

    ((chat_id, texte),) = bot.messages
    assert chat_id == "-100123"
    assert "🚨" in texte and "all" in texte and "boom" in texte


def test_pas_de_doublon_d_alerte_pour_un_meme_job():
    bot = FakeBot()
    client = FakeClient([_job("aaaa", "failed")])
    vus: set[str] = set()

    asyncio.run(surveillance.verifier_jobs(bot, client, vus, chat_id="-100123"))
    asyncio.run(surveillance.verifier_jobs(bot, client, vus, chat_id="-100123"))

    assert len(bot.messages) == 1


def test_les_jobs_completed_ou_running_ne_declenchent_rien():
    bot = FakeBot()
    client = FakeClient([_job("bbbb", "completed"), _job("cccc", "running")])

    asyncio.run(surveillance.verifier_jobs(bot, client, set(), chat_id="-100123"))

    assert bot.messages == []


def test_un_job_running_est_realerte_quand_il_echoue_plus_tard():
    bot = FakeBot()
    client = FakeClient([_job("dddd", "running")])
    vus: set[str] = set()

    asyncio.run(surveillance.verifier_jobs(bot, client, vus, chat_id="-1"))
    client.jobs = [_job("dddd", "failed")]
    asyncio.run(surveillance.verifier_jobs(bot, client, vus, chat_id="-1"))

    assert len(bot.messages) == 1


def test_le_snapshot_initial_n_alerte_pas_l_historique():
    client = FakeClient([_job("vieux", "failed"), _job("encours", "running")])

    vus = asyncio.run(surveillance.initialiser_vus(client))

    assert "vieux" in vus, "échec antérieur au démarrage : jamais alerté"
    assert "encours" not in vus, "un job en cours au démarrage reste surveillé"


def test_une_erreur_api_n_interrompt_pas_la_surveillance():
    class ClientCasse:
        async def get_jobs(self, limit: int = 5):
            raise ConnectionError("api down")

    bot = FakeBot()

    asyncio.run(surveillance.verifier_jobs(bot, ClientCasse(), set(), chat_id="-1"))

    assert bot.messages == []


# =============================================================================
# Péremption des taux régulés (#186, ADR-0024)
# =============================================================================

_AVERT_TURPE = {
    "taxe": "TURPE",
    "attendu_depuis": "2026-08-01",
    "dernier_start": "2025-08-01",
    "consigne": "vérifier la délibération CRE (indexation annuelle au 1ᵉʳ août)",
    "message": (
        "aucune ligne TURPE entrée en vigueur depuis le 2026-08-01 "
        "(dernière connue : 2025-08-01) — vérifier la délibération CRE (indexation annuelle au 1ᵉʳ août)"
    ),
}


class FakeClientPeremption:
    def __init__(self, avertissements):
        self.avertissements = avertissements

    async def get_peremption(self) -> list[dict]:
        return self.avertissements


def test_un_taux_perime_declenche_une_alerte():
    bot = FakeBot()
    client = FakeClientPeremption([_AVERT_TURPE])

    asyncio.run(surveillance.verifier_peremption(bot, client, set(), chat_id="-100123"))

    ((chat_id, texte),) = bot.messages
    assert chat_id == "-100123"
    assert "⚠️" in texte and "TURPE" in texte
    assert "2026-08-01" in texte and "délibération CRE" in texte


def test_pas_de_doublon_pour_le_meme_jalon():
    bot = FakeBot()
    client = FakeClientPeremption([_AVERT_TURPE])
    vus: set = set()

    asyncio.run(surveillance.verifier_peremption(bot, client, vus, chat_id="-1"))
    asyncio.run(surveillance.verifier_peremption(bot, client, vus, chat_id="-1"))

    assert len(bot.messages) == 1


def test_un_nouveau_jalon_realerte():
    """L'année suivante, le même taux toujours périmé est re-signalé (jalon différent)."""
    bot = FakeBot()
    client = FakeClientPeremption([_AVERT_TURPE])
    vus: set = set()

    asyncio.run(surveillance.verifier_peremption(bot, client, vus, chat_id="-1"))
    client.avertissements = [{**_AVERT_TURPE, "attendu_depuis": "2027-08-01"}]
    asyncio.run(surveillance.verifier_peremption(bot, client, vus, chat_id="-1"))

    assert len(bot.messages) == 2


def test_taux_a_jour_aucune_alerte():
    bot = FakeBot()

    asyncio.run(surveillance.verifier_peremption(bot, FakeClientPeremption([]), set(), chat_id="-1"))

    assert bot.messages == []


def test_une_erreur_api_n_interrompt_pas_le_check_de_peremption():
    class ClientCasse:
        async def get_peremption(self):
            raise ConnectionError("api down")

    bot = FakeBot()

    asyncio.run(surveillance.verifier_peremption(bot, ClientCasse(), set(), chat_id="-1"))

    assert bot.messages == []


def test_echec_d_envoi_retentera_au_prochain_passage():
    class BotCasse(FakeBot):
        def __init__(self):
            super().__init__()
            self.casse = True

        async def send_message(self, chat_id, text: str, **kwargs):
            if self.casse:
                raise ConnectionError("telegram down")
            await super().send_message(chat_id, text, **kwargs)

    bot = BotCasse()
    client = FakeClientPeremption([_AVERT_TURPE])
    vus: set = set()

    asyncio.run(surveillance.verifier_peremption(bot, client, vus, chat_id="-1"))
    assert bot.messages == [] and vus == set(), "échec d'envoi : rien n'est marqué vu"

    bot.casse = False
    asyncio.run(surveillance.verifier_peremption(bot, client, vus, chat_id="-1"))
    assert len(bot.messages) == 1
