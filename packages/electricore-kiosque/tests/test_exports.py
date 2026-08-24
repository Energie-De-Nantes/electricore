"""Tests du notebook exports : erreurs de config/clé affichées, pas de traceback (#719 revue).

`app.run(defs=...)` exécute le notebook marimo en process et permet d'injecter
une valeur pour la clé sans passer par une vraie interaction UI — même patron
que `test_accueil.py`.
"""

from __future__ import annotations

import pytest
from electricore_kiosque import exports, helpers


class _FakeCle:
    """Stand-in pour `mo.ui.text` : seule sa `.value` compte pour le notebook."""

    def __init__(self, value: str) -> None:
        self.value = value


def _rendu(cle: str) -> str:
    sorties, _ = exports.app.run(defs={"cle": _FakeCle(cle)})
    return sorties[-1].text


def test_exports_api_url_manquante_affiche_un_message_actionnable(monkeypatch: pytest.MonkeyPatch) -> None:
    """`config.ApiUrlManquante` (KIOSQUE__API_URL absente) est rattrapée, pas un traceback brut."""
    monkeypatch.delenv("KIOSQUE__API_URL", raising=False)

    assert "KIOSQUE__API_URL manquante" in _rendu("une-cle")


def test_exports_cle_refusee_affiche_toujours_un_message_actionnable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-régression : `CleApiRefusee` reste rattrapée après l'élargissement du except."""
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.test.invalide")

    def _lever(cle: str, **kwargs: object) -> list[dict]:
        raise helpers.CleApiRefusee()

    monkeypatch.setattr(helpers, "recuperer_meta_periodes", _lever)

    assert "Clé API refusée" in _rendu("une-cle")
