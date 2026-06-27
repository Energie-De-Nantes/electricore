"""Tests du domaine `/provision` (`electricore/bot/handlers/provision.py`) — #487.

Estimation de provision d'un lissé par PDL : texte (provision base/HP/HC + couverture +
qualité + signal), pas d'export. Commande ERP-agnostique (pas de garde no-ERP).
"""

import asyncio

import pytest

from electricore.api.config import settings
from electricore.bot.handlers import provision as handlers_provision
from tests.unit.telegram_fakes import FakeBot, contexte, update_commande


def _estimation(**overrides) -> dict:
    est = {
        "energie_base_kwh": 1200.0,
        "energie_hp_kwh": None,
        "energie_hc_kwh": None,
        "energie_base_mensuel_kwh": 100.0,
        "energie_hp_mensuel_kwh": None,
        "energie_hc_mensuel_kwh": None,
        "couverture_debut": "2025-06-01",
        "couverture_fin": "2026-06-01",
        "couverture_mois": 12.0,
        "couverture_suffisante": True,
        "profondeur_cadran": "base",
        "qualite": "réelle",
        "presence_regularisation": False,
        "signal_alertable": False,
    }
    est.update(overrides)
    return est


class FakeClient:
    def __init__(self, body: dict):
        self.appels: list[str] = []
        self._body = body

    async def get_provision_estimation(self, pdl: str) -> dict:
        self.appels.append(pdl)
        return self._body


@pytest.fixture(autouse=True)
def _autorise(monkeypatch):
    monkeypatch.setattr(type(settings), "get_telegram_allowed_users", lambda self: {42})


def _brancher(monkeypatch, body: dict) -> FakeClient:
    fake = FakeClient(body)
    monkeypatch.setattr(handlers_provision, "ElectriCoreClient", lambda: fake)
    return fake


def test_provision_sans_argument_affiche_usage(monkeypatch):
    client = _brancher(monkeypatch, {})
    update = update_commande()

    asyncio.run(handlers_provision.cmd_provision(update, contexte()))

    ((texte, _),) = update.effective_message.replies
    assert "Usage" in texte and "/provision" in texte
    assert client.appels == []


def test_provision_avec_pdl_affiche_provision_et_couverture(monkeypatch):
    body = {"pdl": "PDL1", "trouve": True, "estimation": _estimation()}
    client = _brancher(monkeypatch, body)
    update = update_commande()
    bot = FakeBot()

    asyncio.run(handlers_provision.cmd_provision(update, contexte(args=["PDL1"], bot=bot)))

    assert client.appels == ["PDL1"]
    _, _, texte, _ = bot.edits[-1]
    assert "100 kWh/mois" in texte  # provision base /12 plate
    assert "1200 kWh/an" in texte
    assert "12.0 mois" in texte  # couverture
    assert "réelle" in texte
    # Zéro € dans le rendu (kWh uniquement, ADR-0016/0027).
    assert "€" not in texte


def test_provision_signal_alertable_remonte(monkeypatch):
    body = {
        "pdl": "PDL2",
        "trouve": True,
        "estimation": _estimation(
            couverture_mois=6.0, couverture_suffisante=False, qualite="estimée", signal_alertable=True
        ),
    }
    _brancher(monkeypatch, body)
    update = update_commande()
    bot = FakeBot()

    asyncio.run(handlers_provision.cmd_provision(update, contexte(args=["PDL2"], bot=bot)))

    _, _, texte, _ = bot.edits[-1]
    assert "🔔" in texte  # signal alertable affiché
    assert "estimée" in texte


def test_provision_pdl_sans_r67(monkeypatch):
    body = {"pdl": "PDLX", "trouve": False, "estimation": None}
    _brancher(monkeypatch, body)
    update = update_commande()
    bot = FakeBot()

    asyncio.run(handlers_provision.cmd_provision(update, contexte(args=["PDLX"], bot=bot)))

    _, _, texte, _ = bot.edits[-1]
    assert "Aucune mesure R67" in texte or "impossible" in texte
