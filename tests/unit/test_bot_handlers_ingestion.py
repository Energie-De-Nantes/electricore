"""Tests du domaine `/ingestion` (`electricore/bot/handlers/ingestion.py`) — #152.

Clavier inline, confirmation deux-taps pour resync, lancement et suivi de job
par édition du même message, raccourcis texte alignés sur les modes réels de
l'API. Le client HTTP est doublé : on teste le comportement Telegram.
"""

import asyncio

import pytest

from electricore.api.config import settings
from electricore.bot.handlers import ingestion as handlers_ingestion
from tests.unit.telegram_fakes import (
    FakeBot,
    callbacks_du_markup,
    contexte,
    update_callback,
    update_commande,
)

_FLUX_CONNUS = ["affaires", "c12", "c15", "f12", "f15", "r15", "r151", "r64", "r67"]


class FakeClient:
    def __init__(self, statut_final="completed", jobs=None, flux=None):
        self.lances: list[str] = []
        self.statut_final = statut_final
        self.jobs = jobs if jobs is not None else []
        self.flux = flux if flux is not None else list(_FLUX_CONNUS)

    async def run_ingestion(self, mode: str) -> dict:
        self.lances.append(mode)
        return {"id": "abcdef12-0000", "status": "running", "mode": mode}

    async def get_job(self, job_id: str) -> dict:
        return {"id": job_id, "status": self.statut_final, "mode": "all", "error": None, "output": "ok"}

    async def get_jobs(self, limit: int = 5) -> list[dict]:
        return self.jobs

    async def get_flux_connus(self) -> list[str]:
        return self.flux


@pytest.fixture(autouse=True)
def _autorise(monkeypatch):
    monkeypatch.setattr(type(settings), "get_telegram_allowed_users", lambda self: {42})


@pytest.fixture
def client(monkeypatch) -> FakeClient:
    fake = FakeClient()
    monkeypatch.setattr(handlers_ingestion, "ElectriCoreClient", lambda: fake)
    monkeypatch.setattr(handlers_ingestion, "POLL_INTERVAL", 0)
    return fake


# =============================================================================
# Cycle 2 — /ingestion sans argument : clavier principal
# =============================================================================


def test_callback_refuse_un_user_hors_allowlist(client):
    """Les claviers aussi sont protégés — pas seulement les commandes."""
    update = update_callback("ingestion:run:all", user_id=666)

    asyncio.run(handlers_ingestion.on_callback(update, contexte()))

    assert client.lances == []
    assert any("⛔" in texte for texte, _ in update.callback_query.message.replies)


def test_ingestion_sans_argument_ouvre_le_clavier_principal(client):
    update = update_commande()
    asyncio.run(handlers_ingestion.cmd_ingestion(update, contexte()))

    ((_, kwargs),) = update.effective_message.replies
    assert {
        "ingestion:run:all",
        "ingestion:run:test",
        "ingestion:run:rebuild",
        "ingestion:flux",
        "ingestion:resync",
        "ingestion:statut",
    } <= callbacks_du_markup(kwargs)
    assert client.lances == [], "ouvrir le menu ne lance rien"


# =============================================================================
# Cycle 3 — lancement + suivi par édition du même message
# =============================================================================


def _scenario_callback(update, bot):
    """Exécute le callback puis attend les tâches de suivi en arrière-plan."""
    from electricore.bot import tasks

    async def _run():
        await handlers_ingestion.on_callback(update, contexte(bot=bot))
        await asyncio.gather(*tasks._background_tasks)

    asyncio.run(_run())


def test_callback_run_lance_et_suit_par_edition_du_meme_message(client):
    update = update_callback("ingestion:run:all")
    bot = FakeBot()

    _scenario_callback(update, bot)

    assert client.lances == ["all"]
    assert update.callback_query.answered
    assert {(c, m) for c, m, _, _ in bot.edits} == {(1, 100)}, "toutes les éditions portent sur le même message"
    assert "completed" in bot.edits[-1][2]


def test_callback_run_echec_affiche_failed_sur_le_meme_message(monkeypatch):
    fake = FakeClient(statut_final="failed")
    monkeypatch.setattr(handlers_ingestion, "ElectriCoreClient", lambda: fake)
    monkeypatch.setattr(handlers_ingestion, "POLL_INTERVAL", 0)
    update = update_callback("ingestion:run:test")
    bot = FakeBot()

    _scenario_callback(update, bot)

    assert "failed" in bot.edits[-1][2]
    assert bot.edits[-1][1] == 100


def test_callback_flux_ouvre_le_sous_menu_des_flux(client):
    update = update_callback("ingestion:flux")
    bot = FakeBot()

    _scenario_callback(update, bot)

    assert client.lances == []
    (_, _, _, kwargs) = bot.edits[-1]
    callbacks = {btn.callback_data for row in kwargs["reply_markup"].inline_keyboard for btn in row}
    # Le sous-menu dérive de GET /ingestion/flux (#535) — y compris c12/affaires,
    # sans liste en dur côté bot.
    assert {f"ingestion:run:{f}" for f in client.flux} <= callbacks
    assert "ingestion:run:c12" in callbacks
    assert "ingestion:run:affaires" in callbacks
    assert "ingestion:menu" in callbacks, "le sous-menu offre un retour"


def test_callback_flux_degrade_proprement_si_l_api_est_injoignable(monkeypatch):
    """Pattern `etape()`/`flux.py` : une erreur API affiche un message, pas une exception."""

    class ClientEnPanne(FakeClient):
        async def get_flux_connus(self) -> list[str]:
            raise RuntimeError("API injoignable")

    fake = ClientEnPanne()
    monkeypatch.setattr(handlers_ingestion, "ElectriCoreClient", lambda: fake)
    update = update_callback("ingestion:flux")
    bot = FakeBot()

    _scenario_callback(update, bot)

    assert "❌" in bot.edits[-1][2]


def test_callback_menu_revient_au_clavier_principal(client):
    update = update_callback("ingestion:menu")
    bot = FakeBot()

    _scenario_callback(update, bot)

    (_, _, _, kwargs) = bot.edits[-1]
    callbacks = {btn.callback_data for row in kwargs["reply_markup"].inline_keyboard for btn in row}
    assert "ingestion:resync" in callbacks


# =============================================================================
# Cycle 4 — resync : confirmation à deux taps
# =============================================================================


def test_resync_demande_confirmation_sans_rien_lancer(client):
    update = update_callback("ingestion:resync")
    bot = FakeBot()

    _scenario_callback(update, bot)

    assert client.lances == [], "premier tap : aucun lancement"
    (_, _, texte, kwargs) = bot.edits[-1]
    assert "re-télécharge" in texte
    callbacks = {btn.callback_data for row in kwargs["reply_markup"].inline_keyboard for btn in row}
    assert "ingestion:resync:go" in callbacks, "bouton Confirmer"
    assert "ingestion:menu" in callbacks, "bouton Annuler (retour menu)"


def test_resync_confirme_lance_le_pipeline(client):
    update = update_callback("ingestion:resync:go")
    bot = FakeBot()

    _scenario_callback(update, bot)

    assert client.lances == ["resync"]
    assert "completed" in bot.edits[-1][2]


# =============================================================================
# Cycle 5 — raccourcis texte
# =============================================================================


def _scenario_commande(update, ctx):
    from electricore.bot import tasks

    async def _run():
        await handlers_ingestion.cmd_ingestion(update, ctx)
        await asyncio.gather(*tasks._background_tasks)

    asyncio.run(_run())


def test_raccourci_rebuild_lance_directement(client):
    update = update_commande()
    bot = FakeBot()

    _scenario_commande(update, contexte(args=["rebuild"], bot=bot))

    assert client.lances == ["rebuild"]
    assert "completed" in bot.edits[-1][2]
    assert bot.edits[-1][1] == 101, "le suivi édite le message de lancement"


def test_raccourci_liste_de_flux(client):
    update = update_commande()
    bot = FakeBot()

    _scenario_commande(update, contexte(args=["r151", "c15"], bot=bot))

    assert client.lances == ["r151 c15"]


def test_raccourci_resync_exige_aussi_la_confirmation(client):
    update = update_commande()

    _scenario_commande(update, contexte(args=["resync"]))

    assert client.lances == [], "pas de lancement direct, même en raccourci"
    ((texte, kwargs),) = update.effective_message.replies
    assert "re-télécharge" in texte
    assert "ingestion:resync:go" in callbacks_du_markup(kwargs)


def test_raccourci_reset_est_retire(client):
    update = update_commande()

    _scenario_commande(update, contexte(args=["reset"]))

    assert client.lances == []
    ((texte, _),) = update.effective_message.replies
    assert "resync" in texte, "le message oriente vers resync"


# =============================================================================
# Cycle 6 — statut des derniers jobs (remplace /status)
# =============================================================================


_JOBS = [
    {"id": "aaaa1111-0000", "mode": "all", "status": "completed", "started_at": "2026-06-11T03:00:00"},
    {"id": "bbbb2222-0000", "mode": "r151 c15", "status": "failed", "started_at": "2026-06-10T03:00:00"},
]


def test_callback_statut_liste_les_jobs_avec_retour(monkeypatch):
    fake = FakeClient(jobs=_JOBS)
    monkeypatch.setattr(handlers_ingestion, "ElectriCoreClient", lambda: fake)
    update = update_callback("ingestion:statut")
    bot = FakeBot()

    _scenario_callback(update, bot)

    (_, _, texte, kwargs) = bot.edits[-1]
    assert "aaaa1111" in texte and "r151 c15" in texte
    assert "✅" in texte and "❌" in texte
    assert "ingestion:menu" in {btn.callback_data for row in kwargs["reply_markup"].inline_keyboard for btn in row}


def test_raccourci_statut_liste_les_jobs(monkeypatch):
    fake = FakeClient(jobs=_JOBS)
    monkeypatch.setattr(handlers_ingestion, "ElectriCoreClient", lambda: fake)
    update = update_commande()

    _scenario_commande(update, contexte(args=["statut"]))

    ((texte, _),) = update.effective_message.replies
    assert "aaaa1111" in texte
    assert fake.lances == []


def test_statut_sans_aucun_job(monkeypatch):
    fake = FakeClient(jobs=[])
    monkeypatch.setattr(handlers_ingestion, "ElectriCoreClient", lambda: fake)
    update = update_commande()

    _scenario_commande(update, contexte(args=["statut"]))

    ((texte, _),) = update.effective_message.replies
    assert "Aucun job" in texte
