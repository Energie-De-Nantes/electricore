"""Config Kiosque par variables d'environnement — une instance par entité (ADR-0057)."""

from __future__ import annotations

import os

_TITRE_DEFAUT = "ElectriCore"


def apps_actives() -> list[str]:
    """`KIOSQUE__APPS` : noms séparés par virgule, sélection à monter dans le catalogue."""
    brut = os.environ.get("KIOSQUE__APPS", "")
    return [nom.strip() for nom in brut.split(",") if nom.strip()]


def titre() -> str:
    """`KIOSQUE__TITRE` : nom de l'entité, affiché par l'accueil."""
    return os.environ.get("KIOSQUE__TITRE", _TITRE_DEFAUT)
