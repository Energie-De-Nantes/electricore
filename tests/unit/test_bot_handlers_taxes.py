"""Tests du domaine `/taxes` (`electricore/bot/handlers/taxes.py`) — #155.

Accise TICFE et CTA : navigation par boutons (type → trimestre), trimestres
dérivés de la date courante, raccourcis texte inchangés. Remplace /taxes v1.
"""

import asyncio
from datetime import date

import httpx
import pytest

from electricore.api.config import settings
from electricore.bot.client import ElectriCoreClient
from electricore.bot.handlers import taxes as handlers_taxes
from tests.unit.telegram_fakes import (
    FakeBot,
    callbacks_du_markup,
    contexte,
    update_callback,
    update_commande,
)


class FakeClient:
    def __init__(self):
        self.appels: list[tuple[str, str | None]] = []

    async def get_accise_xlsx(self, trimestre: str | None = None) -> bytes:
        self.appels.append(("accise", trimestre))
        return b"PK\x03\x04accise"

    async def get_cta_xlsx(self, trimestre: str | None = None) -> bytes:
        self.appels.append(("cta", trimestre))
        return b"PK\x03\x04cta"


@pytest.fixture(autouse=True)
def _autorise(monkeypatch):
    monkeypatch.setattr(type(settings), "get_telegram_allowed_users", lambda self: {42})
    # Domaine ERP-dépendant : on teste son comportement, pas la garde no-ERP (#159).
    monkeypatch.setattr(type(settings), "is_odoo_configured", property(lambda self: True))


@pytest.fixture
def client(monkeypatch) -> FakeClient:
    fake = FakeClient()
    monkeypatch.setattr(handlers_taxes, "ElectriCoreClient", lambda: fake)
    return fake


# =============================================================================
# Trimestres dérivés de la date courante
# =============================================================================


def test_derniers_trimestres_en_milieu_d_annee():
    assert handlers_taxes.derniers_trimestres(date(2026, 6, 11), n=4) == [
        "2026-T1",
        "2025-T4",
        "2025-T3",
        "2025-T2",
    ]


def test_derniers_trimestres_en_janvier():
    assert handlers_taxes.derniers_trimestres(date(2026, 1, 15), n=4) == [
        "2025-T4",
        "2025-T3",
        "2025-T2",
        "2025-T1",
    ]


# =============================================================================
# Navigation par boutons
# =============================================================================


def test_taxes_sans_argument_propose_accise_et_cta(client):
    update = update_commande()

    asyncio.run(handlers_taxes.cmd_taxes(update, contexte()))

    ((_, kwargs),) = update.effective_message.replies
    assert {"taxes:choix:accise", "taxes:choix:cta"} <= callbacks_du_markup(kwargs)
    assert client.appels == []


def test_callback_choix_accise_propose_les_trimestres(client):
    update = update_callback("taxes:choix:accise")
    bot = FakeBot()

    asyncio.run(handlers_taxes.on_callback(update, contexte(bot=bot)))

    (_, _, _, kwargs) = bot.edits[-1]
    callbacks = callbacks_du_markup(kwargs)
    trimestres = handlers_taxes.derniers_trimestres(date.today())
    assert {f"taxes:run:accise:{t}" for t in trimestres} <= callbacks
    assert "taxes:run:accise:all" in callbacks, "option toutes périodes"
    assert "taxes:menu" in callbacks, "retour"


def test_callback_run_accise_trimestre_envoie_le_document(client):
    update = update_callback("taxes:run:accise:2025-T4")
    bot = FakeBot()

    asyncio.run(handlers_taxes.on_callback(update, contexte(bot=bot)))

    assert client.appels == [("accise", "2025-T4")]
    ((_, kwargs),) = bot.documents
    assert kwargs["filename"] == "accise_2025-T4.xlsx"


def test_callback_run_cta_toutes_periodes(client):
    update = update_callback("taxes:run:cta:all")
    bot = FakeBot()

    asyncio.run(handlers_taxes.on_callback(update, contexte(bot=bot)))

    assert client.appels == [("cta", None)]
    ((_, kwargs),) = bot.documents
    assert kwargs["filename"] == "cta.xlsx"


# =============================================================================
# Raccourcis texte (contrat v1 inchangé)
# =============================================================================


def test_raccourci_accise_avec_trimestre(client):
    update = update_commande()
    bot = FakeBot()

    asyncio.run(handlers_taxes.cmd_taxes(update, contexte(args=["accise", "2025-T1"], bot=bot)))

    assert client.appels == [("accise", "2025-T1")]


def test_raccourci_cta_sans_trimestre(client):
    update = update_commande()
    bot = FakeBot()

    asyncio.run(handlers_taxes.cmd_taxes(update, contexte(args=["cta"], bot=bot)))

    assert client.appels == [("cta", None)]


def test_raccourci_inconnu_affiche_l_usage(client):
    update = update_commande()

    asyncio.run(handlers_taxes.cmd_taxes(update, contexte(args=["bogus"])))

    ((texte, _),) = update.effective_message.replies
    assert "accise" in texte and "cta" in texte
    assert client.appels == []


# =============================================================================
# Régression incident 2026-06-12 — 503 cryptique pendant l'ingestion (#171)
# =============================================================================


def test_cta_pendant_l_ingestion_affiche_le_detail_api(monkeypatch):
    """Bout en bout côté bot : l'API répond 503 + detail (base verrouillée par
    l'ingestion), l'opérateur lit ce détail — pas la ligne de statut httpx."""
    detail = "Ingestion en cours — la base de données est en cours d'écriture. Réessaie dans quelques minutes."

    def _base_verrouillee(request):
        return httpx.Response(503, json={"detail": detail})

    monkeypatch.setattr(
        handlers_taxes,
        "ElectriCoreClient",
        lambda: ElectriCoreClient(transport=httpx.MockTransport(_base_verrouillee)),
    )
    update = update_callback("taxes:run:cta:2025-T4")
    bot = FakeBot()

    asyncio.run(handlers_taxes.on_callback(update, contexte(bot=bot)))

    assert bot.documents == [], "pas de document pendant le verrou"
    _, _, texte, _ = bot.edits[-1]
    assert "Ingestion en cours" in texte
    assert "Server error" not in texte, "la ligne de statut httpx ne doit plus fuiter dans le chat"
