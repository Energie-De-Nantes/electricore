"""Tests du domaine `/etl` (`electricore/bot/handlers/etl.py`) — #152.

Clavier inline, confirmation deux-taps pour resync, lancement et suivi de job
par édition du même message, raccourcis texte alignés sur les modes réels de
l'API. Le client HTTP est doublé : on teste le comportement Telegram.
"""

import asyncio
from types import SimpleNamespace

import pytest

from electricore.api.config import settings
from electricore.bot.handlers import etl as handlers_etl

# =============================================================================
# Fakes Telegram & client
# =============================================================================


class FakeMessage:
    def __init__(self, chat_id=1, message_id=100):
        self.chat_id = chat_id
        self.message_id = message_id
        self.replies: list[tuple[str, dict]] = []

    async def reply_html(self, text: str, **kwargs):
        self.replies.append((text, kwargs))
        return SimpleNamespace(chat_id=self.chat_id, message_id=self.message_id + 1)

    async def reply_text(self, text: str, **kwargs):
        self.replies.append((text, kwargs))


class FakeBot:
    def __init__(self):
        self.edits: list[tuple[int, int, str, dict]] = []

    async def edit_message_text(self, text: str, chat_id: int, message_id: int, **kwargs):
        self.edits.append((chat_id, message_id, text, kwargs))


class FakeQuery:
    def __init__(self, data: str, chat_id=1, message_id=100):
        self.data = data
        self.message = FakeMessage(chat_id=chat_id, message_id=message_id)
        self.answered = False
        self.edits: list[tuple[str, dict]] = []

    async def answer(self):
        self.answered = True

    async def edit_message_text(self, text: str, **kwargs):
        self.edits.append((text, kwargs))


class FakeClient:
    def __init__(self, statut_final="completed", jobs=None):
        self.lances: list[str] = []
        self.statut_final = statut_final
        self.jobs = jobs if jobs is not None else []

    async def run_etl(self, mode: str) -> dict:
        self.lances.append(mode)
        return {"id": "abcdef12-0000", "status": "running", "mode": mode}

    async def get_job(self, job_id: str) -> dict:
        return {"id": job_id, "status": self.statut_final, "mode": "all", "error": None, "output": "ok"}

    async def get_jobs(self, limit: int = 5) -> list[dict]:
        return self.jobs


def update_commande(user_id=42) -> SimpleNamespace:
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=user_id),
        effective_message=FakeMessage(),
        callback_query=None,
    )


def update_callback(data: str, user_id=42) -> SimpleNamespace:
    query = FakeQuery(data)
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=user_id),
        effective_message=query.message,
        callback_query=query,
    )


def contexte(args=None, bot=None) -> SimpleNamespace:
    return SimpleNamespace(args=args or [], bot=bot or FakeBot())


@pytest.fixture(autouse=True)
def _autorise(monkeypatch):
    monkeypatch.setattr(type(settings), "get_telegram_allowed_users", lambda self: {42})


@pytest.fixture
def client(monkeypatch) -> FakeClient:
    fake = FakeClient()
    monkeypatch.setattr(handlers_etl, "ElectriCoreClient", lambda: fake)
    monkeypatch.setattr(handlers_etl, "POLL_INTERVAL", 0)
    return fake


def _callbacks_du_markup(kwargs: dict) -> set[str]:
    markup = kwargs["reply_markup"]
    return {btn.callback_data for row in markup.inline_keyboard for btn in row}


# =============================================================================
# Cycle 2 — /etl sans argument : clavier principal
# =============================================================================


def test_callback_refuse_un_user_hors_allowlist(client):
    """Les claviers aussi sont protégés — pas seulement les commandes."""
    update = update_callback("etl:run:all", user_id=666)

    asyncio.run(handlers_etl.on_callback(update, contexte()))

    assert client.lances == []
    assert any("⛔" in texte for texte, _ in update.callback_query.message.replies)


def test_etl_sans_argument_ouvre_le_clavier_principal(client):
    update = update_commande()
    asyncio.run(handlers_etl.cmd_etl(update, contexte()))

    ((_, kwargs),) = update.effective_message.replies
    assert {
        "etl:run:all",
        "etl:run:test",
        "etl:run:rebuild",
        "etl:flux",
        "etl:resync",
        "etl:statut",
    } <= _callbacks_du_markup(kwargs)
    assert client.lances == [], "ouvrir le menu ne lance rien"


# =============================================================================
# Cycle 3 — lancement + suivi par édition du même message
# =============================================================================


def _scenario_callback(update, bot):
    """Exécute le callback puis attend les tâches de suivi en arrière-plan."""
    from electricore.bot import tasks

    async def _run():
        await handlers_etl.on_callback(update, contexte(bot=bot))
        await asyncio.gather(*tasks._background_tasks)

    asyncio.run(_run())


def test_callback_run_lance_et_suit_par_edition_du_meme_message(client):
    update = update_callback("etl:run:all")
    bot = FakeBot()

    _scenario_callback(update, bot)

    assert client.lances == ["all"]
    assert update.callback_query.answered
    assert {(c, m) for c, m, _, _ in bot.edits} == {(1, 100)}, "toutes les éditions portent sur le même message"
    assert "completed" in bot.edits[-1][2]


def test_callback_run_echec_affiche_failed_sur_le_meme_message(monkeypatch):
    fake = FakeClient(statut_final="failed")
    monkeypatch.setattr(handlers_etl, "ElectriCoreClient", lambda: fake)
    monkeypatch.setattr(handlers_etl, "POLL_INTERVAL", 0)
    update = update_callback("etl:run:test")
    bot = FakeBot()

    _scenario_callback(update, bot)

    assert "failed" in bot.edits[-1][2]
    assert bot.edits[-1][1] == 100


def test_callback_flux_ouvre_le_sous_menu_des_flux(client):
    update = update_callback("etl:flux")
    bot = FakeBot()

    _scenario_callback(update, bot)

    assert client.lances == []
    (_, _, _, kwargs) = bot.edits[-1]
    callbacks = {btn.callback_data for row in kwargs["reply_markup"].inline_keyboard for btn in row}
    assert {f"etl:run:{f}" for f in handlers_etl.FLUX} <= callbacks
    assert "etl:menu" in callbacks, "le sous-menu offre un retour"


def test_callback_menu_revient_au_clavier_principal(client):
    update = update_callback("etl:menu")
    bot = FakeBot()

    _scenario_callback(update, bot)

    (_, _, _, kwargs) = bot.edits[-1]
    callbacks = {btn.callback_data for row in kwargs["reply_markup"].inline_keyboard for btn in row}
    assert "etl:resync" in callbacks


# =============================================================================
# Cycle 4 — resync : confirmation à deux taps
# =============================================================================


def test_resync_demande_confirmation_sans_rien_lancer(client):
    update = update_callback("etl:resync")
    bot = FakeBot()

    _scenario_callback(update, bot)

    assert client.lances == [], "premier tap : aucun lancement"
    (_, _, texte, kwargs) = bot.edits[-1]
    assert "re-télécharge" in texte
    callbacks = {btn.callback_data for row in kwargs["reply_markup"].inline_keyboard for btn in row}
    assert "etl:resync:go" in callbacks, "bouton Confirmer"
    assert "etl:menu" in callbacks, "bouton Annuler (retour menu)"


def test_resync_confirme_lance_le_pipeline(client):
    update = update_callback("etl:resync:go")
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
        await handlers_etl.cmd_etl(update, ctx)
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
    assert "etl:resync:go" in _callbacks_du_markup(kwargs)


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
    monkeypatch.setattr(handlers_etl, "ElectriCoreClient", lambda: fake)
    update = update_callback("etl:statut")
    bot = FakeBot()

    _scenario_callback(update, bot)

    (_, _, texte, kwargs) = bot.edits[-1]
    assert "aaaa1111" in texte and "r151 c15" in texte
    assert "✅" in texte and "❌" in texte
    assert "etl:menu" in {btn.callback_data for row in kwargs["reply_markup"].inline_keyboard for btn in row}


def test_raccourci_statut_liste_les_jobs(monkeypatch):
    fake = FakeClient(jobs=_JOBS)
    monkeypatch.setattr(handlers_etl, "ElectriCoreClient", lambda: fake)
    update = update_commande()

    _scenario_commande(update, contexte(args=["statut"]))

    ((texte, _),) = update.effective_message.replies
    assert "aaaa1111" in texte
    assert fake.lances == []


def test_statut_sans_aucun_job(monkeypatch):
    fake = FakeClient(jobs=[])
    monkeypatch.setattr(handlers_etl, "ElectriCoreClient", lambda: fake)
    update = update_commande()

    _scenario_commande(update, contexte(args=["statut"]))

    ((texte, _),) = update.effective_message.replies
    assert "Aucun job" in texte
