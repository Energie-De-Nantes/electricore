"""Tests de la lecture de config par variables d'environnement (par entité, ADR-0057)."""

from __future__ import annotations

import pytest
from electricore_kiosque.config import apps_actives, titre


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
