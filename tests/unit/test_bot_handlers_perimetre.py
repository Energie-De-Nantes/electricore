"""Tests du domaine `/perimetre` (`electricore/bot/handlers/perimetre.py`) — #154.

Entrées (PMES, MES, CFNE) et sorties (RES, CFNS) du périmètre, vues métier du
C15 — vocabulaire du glossaire core. Remplace /entrees et /sorties v1.
"""

import asyncio

import pytest

from electricore.api.config import settings
from electricore.bot.handlers import perimetre as handlers_perimetre
from tests.unit.telegram_fakes import (
    FakeBot,
    callbacks_du_markup,
    contexte,
    update_callback,
    update_commande,
)


class FakeClient:
    def __init__(self):
        self.exports: list[str] = []
        self.affaires_appels = 0

    async def get_entrees_xlsx(self) -> bytes:
        self.exports.append("entrees")
        return b"PK\x03\x04entrees"

    async def get_sorties_xlsx(self) -> bytes:
        self.exports.append("sorties")
        return b"PK\x03\x04sorties"

    async def get_affaires_ouvertes(self) -> list[dict]:
        self.affaires_appels += 1
        return [
            {
                "pdl": "99000000000017",
                "prestation": "CFN",
                "prestation_libelle": "Changement de fournisseur",
                "dernier_etat": "INPL",
                "dernier_etat_libelle": "Intervention planifiée",
                "anciennete_jours": 5,
            }
        ]


@pytest.fixture(autouse=True)
def _autorise(monkeypatch):
    monkeypatch.setattr(type(settings), "get_telegram_allowed_users", lambda self: {42})


@pytest.fixture
def client(monkeypatch) -> FakeClient:
    fake = FakeClient()
    monkeypatch.setattr(handlers_perimetre, "ElectriCoreClient", lambda: fake)
    return fake


def test_perimetre_sans_argument_ouvre_le_clavier(client):
    update = update_commande()

    asyncio.run(handlers_perimetre.cmd_perimetre(update, contexte()))

    ((_, kwargs),) = update.effective_message.replies
    assert {"perimetre:entrees", "perimetre:sorties"} <= callbacks_du_markup(kwargs)
    assert client.exports == []


def test_callback_entrees_envoie_le_document(client):
    update = update_callback("perimetre:entrees")
    bot = FakeBot()

    asyncio.run(handlers_perimetre.on_callback(update, contexte(bot=bot)))

    assert client.exports == ["entrees"]
    ((_, kwargs),) = bot.documents
    assert kwargs["filename"] == "entrees_c15.xlsx"
    assert "PMES" in kwargs["caption"]


def test_callback_sorties_envoie_le_document(client):
    update = update_callback("perimetre:sorties")
    bot = FakeBot()

    asyncio.run(handlers_perimetre.on_callback(update, contexte(bot=bot)))

    assert client.exports == ["sorties"]
    ((_, kwargs),) = bot.documents
    assert kwargs["filename"] == "sorties_c15.xlsx"


@pytest.mark.parametrize("raccourci,export", [("entrees", "entrees"), ("sorties", "sorties")])
def test_raccourcis_texte(client, raccourci, export):
    update = update_commande()
    bot = FakeBot()

    asyncio.run(handlers_perimetre.cmd_perimetre(update, contexte(args=[raccourci], bot=bot)))

    assert client.exports == [export]


def test_raccourci_inconnu_affiche_l_usage(client):
    update = update_commande()

    asyncio.run(handlers_perimetre.cmd_perimetre(update, contexte(args=["bogus"])))

    ((texte, _),) = update.effective_message.replies
    assert "entrees" in texte and "sorties" in texte
    assert client.exports == []


# =============================================================================
# Affaires SGE en cours (cockpit read-only, #276)
# =============================================================================


def test_formater_affaires_liste_etat_et_anciennete():
    txt = handlers_perimetre._formater_affaires(
        [
            {
                "pdl": "99000000000017",
                "prestation": "CFN",
                "prestation_libelle": "Changement de fournisseur",
                "dernier_etat": "INPL",
                "dernier_etat_libelle": "Intervention planifiée",
                "anciennete_jours": 5,
            }
        ]
    )
    assert "Changement de fournisseur" in txt
    assert "Intervention planifiée" in txt
    assert "5" in txt


def test_formater_affaires_vide():
    txt = handlers_perimetre._formater_affaires([])
    assert "aucune" in txt.lower()


def test_menu_propose_les_affaires(client):
    update = update_commande()

    asyncio.run(handlers_perimetre.cmd_perimetre(update, contexte()))

    ((_, kwargs),) = update.effective_message.replies
    assert "perimetre:affaires" in callbacks_du_markup(kwargs)


def test_callback_affaires_affiche_la_liste(client):
    update = update_callback("perimetre:affaires")
    bot = FakeBot()

    asyncio.run(handlers_perimetre.on_callback(update, contexte(bot=bot)))

    assert client.affaires_appels == 1
    assert client.exports == []  # vue texte, pas d'export XLSX
    textes = " ".join(texte for _, _, texte, _ in bot.edits)
    assert "Intervention planifiée" in textes


def test_raccourci_texte_affaires(client):
    update = update_commande()
    bot = FakeBot()

    asyncio.run(handlers_perimetre.cmd_perimetre(update, contexte(args=["affaires"], bot=bot)))

    assert client.affaires_appels == 1
