"""Config Kiosque par variables d'environnement — une instance par entité (ADR-0057)."""

from __future__ import annotations

import os

_TITRE_DEFAUT = "ElectriCore"
_API_URL_DEFAUT = "https://edn.electricore.fr"


def apps_actives() -> list[str]:
    """`KIOSQUE__APPS` : noms séparés par virgule, sélection à monter dans le catalogue."""
    brut = os.environ.get("KIOSQUE__APPS", "")
    return [nom.strip() for nom in brut.split(",") if nom.strip()]


def titre() -> str:
    """`KIOSQUE__TITRE` : nom de l'entité, affiché par l'accueil."""
    return os.environ.get("KIOSQUE__TITRE", _TITRE_DEFAUT)


def api_url() -> str:
    """`KIOSQUE__API_URL` : API `electricore` interrogée par les notebooks (#705).

    Défaut : l'API publique de la box de référence (même URL que l'onboarding
    notebook opérateur, `docs/operateur-notebook.md`).
    """
    return os.environ.get("KIOSQUE__API_URL", _API_URL_DEFAUT)
