"""L'accueil reflète `KIOSQUE__TITRE` et exactement la sélection `KIOSQUE__APPS` (#704).

`app.run()` exécute le notebook marimo en process et rend ses sorties — pas de
navigateur, pas de serveur : juste le markdown produit par la cellule d'accueil.
"""

from __future__ import annotations

import pytest
from electricore_kiosque import accueil


def _rendu() -> str:
    sorties, _ = accueil.app.run()
    return sorties[0].text


def test_accueil_affiche_le_titre_et_les_seules_apps_actives(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KIOSQUE__TITRE", "Ma structure")
    monkeypatch.setenv("KIOSQUE__APPS", "exports")

    rendu = _rendu()

    assert "Ma structure" in rendu
    assert 'href="/exports"' in rendu
    assert "facturation" not in rendu  # app du catalogue non listée : aucun lien


def test_accueil_sans_app_active_le_dit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KIOSQUE__APPS", raising=False)
    monkeypatch.setenv("KIOSQUE__TITRE", "Ma structure")

    assert "Aucune app active" in _rendu()
