"""Tests du domaine `/facturation` (`electricore/bot/handlers/facturation.py`) — #156.

Documents de campagne (choix du mois par boutons) et contrôles pré-facturation
(résumé + XLSX de détail via l'API, #150). Absorbe /facturation et /check v1.
"""

import asyncio
from datetime import date

import pytest

from electricore.api.config import settings
from electricore.bot.handlers import facturation as handlers_facturation
from tests.unit.telegram_fakes import (
    FakeBot,
    callbacks_du_markup,
    contexte,
    update_callback,
    update_commande,
)

_CHECK_VIDE = {
    "rsc_manquante": [],
    "cfne_manquante": [],
    "invoicing_state_counts": {"up_to_date": 7},
    "factures_draft": [],
    "lisses_quantite_1": [],
}


class FakeClient:
    def __init__(self, check=None):
        self.documents_appels: list[str | None] = []
        self.check = check if check is not None else dict(_CHECK_VIDE)
        self.check_xlsx_demande = False

    async def get_facturation_documents_xlsx(self, mois: str | None = None) -> bytes:
        self.documents_appels.append(mois)
        return b"PK\x03\x04docs"

    async def check_facturation_odoo(self) -> dict:
        return self.check

    async def get_check_odoo_xlsx(self) -> bytes:
        self.check_xlsx_demande = True
        return b"PK\x03\x04check"


@pytest.fixture(autouse=True)
def _autorise(monkeypatch):
    monkeypatch.setattr(type(settings), "get_telegram_allowed_users", lambda self: {42})


@pytest.fixture
def client(monkeypatch) -> FakeClient:
    fake = FakeClient()
    monkeypatch.setattr(handlers_facturation, "ElectriCoreClient", lambda: fake)
    return fake


def test_derniers_mois():
    assert handlers_facturation.derniers_mois(date(2026, 6, 11), n=3) == [
        "2026-05-01",
        "2026-04-01",
        "2026-03-01",
    ]


def test_facturation_sans_argument_propose_documents_et_check(client):
    update = update_commande()

    asyncio.run(handlers_facturation.cmd_facturation(update, contexte()))

    ((_, kwargs),) = update.effective_message.replies
    assert {"facturation:documents", "facturation:check"} <= callbacks_du_markup(kwargs)


def test_callback_documents_propose_les_mois(client):
    update = update_callback("facturation:documents")
    bot = FakeBot()

    asyncio.run(handlers_facturation.on_callback(update, contexte(bot=bot)))

    (_, _, _, kwargs) = bot.edits[-1]
    callbacks = callbacks_du_markup(kwargs)
    mois = handlers_facturation.derniers_mois(date.today())
    assert {f"facturation:run:documents:{m}" for m in mois} <= callbacks
    assert "facturation:run:documents:last" in callbacks, "dernier mois disponible"
    assert "facturation:menu" in callbacks


def test_callback_run_documents_mois_envoie_le_document(client):
    update = update_callback("facturation:run:documents:2026-05-01")
    bot = FakeBot()

    asyncio.run(handlers_facturation.on_callback(update, contexte(bot=bot)))

    assert client.documents_appels == ["2026-05-01"]
    ((_, kwargs),) = bot.documents
    assert kwargs["filename"] == "facturation_2026-05.xlsx"


def test_callback_run_documents_dernier_mois(client):
    update = update_callback("facturation:run:documents:last")
    bot = FakeBot()

    asyncio.run(handlers_facturation.on_callback(update, contexte(bot=bot)))

    assert client.documents_appels == [None]
    ((_, kwargs),) = bot.documents
    assert kwargs["filename"] == "facturation.xlsx"


def test_callback_check_tout_vert_edite_le_resume_sans_document(client):
    update = update_callback("facturation:check")
    bot = FakeBot()

    asyncio.run(handlers_facturation.on_callback(update, contexte(bot=bot)))

    (_, _, texte, _) = bot.edits[-1]
    assert "OK pour lancer le cycle de facturation" in texte
    assert bot.documents == []
    assert client.check_xlsx_demande is False


def test_callback_check_avec_depassement_joint_le_xlsx(monkeypatch):
    nombreuses = [{"name": f"S{i:05}", "url": f"https://odoo/{i}"} for i in range(25)]
    fake = FakeClient(check={**_CHECK_VIDE, "rsc_manquante": nombreuses})
    monkeypatch.setattr(handlers_facturation, "ElectriCoreClient", lambda: fake)
    update = update_callback("facturation:check")
    bot = FakeBot()

    asyncio.run(handlers_facturation.on_callback(update, contexte(bot=bot)))

    assert fake.check_xlsx_demande is True
    ((_, kwargs),) = bot.documents
    assert kwargs["filename"] == "check_odoo.xlsx"


def test_raccourci_documents_avec_mois(client):
    update = update_commande()
    bot = FakeBot()

    asyncio.run(handlers_facturation.cmd_facturation(update, contexte(args=["documents", "2026-04-01"], bot=bot)))

    assert client.documents_appels == ["2026-04-01"]


def test_raccourci_check(client):
    update = update_commande()
    bot = FakeBot()

    asyncio.run(handlers_facturation.cmd_facturation(update, contexte(args=["check"], bot=bot)))

    assert "up_to_date" in bot.edits[-1][2]


def test_raccourci_inconnu_affiche_l_usage(client):
    update = update_commande()

    asyncio.run(handlers_facturation.cmd_facturation(update, contexte(args=["2026-05-01"])))

    ((texte, _),) = update.effective_message.replies
    assert "documents" in texte and "check" in texte
    assert client.documents_appels == []
