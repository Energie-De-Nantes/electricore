"""Tests de la lecture de config par variables d'environnement (par entité, ADR-0057)."""

from __future__ import annotations

import pytest
from electricore_kiosque.config import ApiUrlManquante, api_url, apps_actives, titre


def test_apps_actives_liste_les_noms_separes_par_virgule(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KIOSQUE__APPS", "exports, facturation ,rsc")
    assert apps_actives() == ["exports", "facturation", "rsc"]


def test_apps_actives_vide_par_defaut(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KIOSQUE__APPS", raising=False)
    assert apps_actives() == []


def test_titre_lit_kiosque_titre(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KIOSQUE__TITRE", "Ma structure")
    assert titre() == "Ma structure"


def test_titre_a_un_defaut(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KIOSQUE__TITRE", raising=False)
    assert titre() == "ElectriCore"


def test_api_url_lit_kiosque_api_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KIOSQUE__API_URL", "https://kiosque.exemple.fr")
    assert api_url() == "https://kiosque.exemple.fr"


def test_api_url_manquante_leve_une_erreur_actionnable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pas de défaut codé en dur : une box réelle vient du config.env du provider."""
    monkeypatch.delenv("KIOSQUE__API_URL", raising=False)
    with pytest.raises(ApiUrlManquante, match="KIOSQUE__API_URL"):
        api_url()
