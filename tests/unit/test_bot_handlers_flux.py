"""Tests du domaine `/flux` (`electricore/bot/handlers/flux.py`) — #153.

Exploration et export des tables brutes Enedis : clavier des tables (stats +
export par table), raccourcis texte. Remplace les commandes v1 /stats, /export
et l'ancien /flux plat.
"""

import asyncio

import pytest

from electricore.api.config import settings
from electricore.bot.handlers import flux as handlers_flux
from tests.unit.telegram_fakes import (
    FakeBot,
    callbacks_du_markup,
    contexte,
    update_callback,
    update_commande,
)

TABLES = ["c15", "f15_detail", "r151"]


class FakeClient:
    def __init__(self, tables=None):
        self.tables = tables if tables is not None else list(TABLES)
        self.exports: list[str] = []

    async def list_tables(self) -> list[str]:
        return self.tables

    async def get_table_info(self, table: str) -> dict:
        return {"table": f"flux_{table}", "count": 1234, "columns": ["a", "b", "c"], "derniere_date": "2026-06-01"}

    async def get_xlsx(self, table: str) -> bytes:
        self.exports.append(table)
        return b"PK\x03\x04fake-xlsx"


@pytest.fixture(autouse=True)
def _autorise(monkeypatch):
    monkeypatch.setattr(type(settings), "get_telegram_allowed_users", lambda self: {42})


@pytest.fixture
def client(monkeypatch) -> FakeClient:
    fake = FakeClient()
    monkeypatch.setattr(handlers_flux, "ElectriCoreClient", lambda: fake)
    return fake


# =============================================================================
# Cycle 1 — /flux sans argument : clavier des tables
# =============================================================================


def test_flux_sans_argument_liste_les_tables_avec_stats_et_export(client):
    update = update_commande()

    asyncio.run(handlers_flux.cmd_flux(update, contexte()))

    ((_, kwargs),) = update.effective_message.replies
    callbacks = callbacks_du_markup(kwargs)
    for table in TABLES:
        assert f"flux:stats:{table}" in callbacks
        assert f"flux:export:{table}" in callbacks
    assert client.exports == []


def test_le_menu_decrit_chaque_table_connue(monkeypatch):
    fake = FakeClient(tables=["c15", "r151", "table_inconnue"])
    monkeypatch.setattr(handlers_flux, "ElectriCoreClient", lambda: fake)
    update = update_commande()

    asyncio.run(handlers_flux.cmd_flux(update, contexte()))

    ((texte, _),) = update.effective_message.replies
    assert "Événements contractuels" in texte, "description du c15"
    assert "Relevés périodiques" in texte, "description du r151"
    assert "table_inconnue" in texte, "une table sans description reste listée"


def test_le_menu_decrit_c12_et_affaires(monkeypatch):
    """#537 : c12 et affaires étaient listés sans description (dérive silencieuse)."""
    fake = FakeClient(tables=["c12", "affaires"])
    monkeypatch.setattr(handlers_flux, "ElectriCoreClient", lambda: fake)
    update = update_commande()

    asyncio.run(handlers_flux.cmd_flux(update, contexte()))

    ((texte, _),) = update.effective_message.replies
    assert "C2-C4" in texte, "description contractuelle c12"
    assert "SGE" in texte, "description affaires"


# =============================================================================
# Cycles 2-3 — callbacks stats, export, retour
# =============================================================================


def test_callback_stats_edite_le_message_avec_les_stats(client):
    update = update_callback("flux:stats:c15")
    bot = FakeBot()

    asyncio.run(handlers_flux.on_callback(update, contexte(bot=bot)))

    (_, message_id, texte, kwargs) = bot.edits[-1]
    assert message_id == 100, "les stats éditent le message du clavier"
    assert "flux_c15" in texte
    assert "1,234" in texte, "nombre de lignes"
    assert "3" in texte, "nombre de colonnes"
    assert "2026-06-01" in texte, "fraîcheur : dernière date métier (#158)"
    assert "flux:menu" in callbacks_du_markup(kwargs), "retour à la liste"


def test_callback_menu_revient_a_la_liste_des_tables(client):
    update = update_callback("flux:menu")
    bot = FakeBot()

    asyncio.run(handlers_flux.on_callback(update, contexte(bot=bot)))

    (_, _, _, kwargs) = bot.edits[-1]
    assert "flux:stats:c15" in callbacks_du_markup(kwargs)


def test_callback_export_envoie_le_document_xlsx(client):
    update = update_callback("flux:export:r151")
    bot = FakeBot()

    asyncio.run(handlers_flux.on_callback(update, contexte(bot=bot)))

    assert client.exports == ["r151"]
    ((chat_id, kwargs),) = bot.documents
    assert chat_id == 1
    assert kwargs["filename"] == "flux_r151.xlsx"
    assert "envoyé" in bot.edits[-1][2], "le message du clavier confirme l'envoi"


# =============================================================================
# Cycle 4 — raccourcis texte
# =============================================================================


def test_raccourci_stats(client):
    update = update_commande()

    asyncio.run(handlers_flux.cmd_flux(update, contexte(args=["stats", "c15"])))

    ((texte, _),) = update.effective_message.replies
    assert "flux_c15" in texte and "1,234" in texte


def test_raccourci_export(client):
    update = update_commande()
    bot = FakeBot()

    asyncio.run(handlers_flux.cmd_flux(update, contexte(args=["export", "r151"], bot=bot)))

    assert client.exports == ["r151"]
    ((_, kwargs),) = bot.documents
    assert kwargs["filename"] == "flux_r151.xlsx"


def test_raccourci_inconnu_affiche_l_usage(client):
    update = update_commande()

    asyncio.run(handlers_flux.cmd_flux(update, contexte(args=["bogus"])))

    ((texte, _),) = update.effective_message.replies
    assert "stats" in texte and "export" in texte, "le message rappelle l'usage"
    assert client.exports == []
