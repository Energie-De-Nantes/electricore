"""Config Kiosque par variables d'environnement — une instance par entité (ADR-0057)."""

from __future__ import annotations

import os

_TITRE_DEFAUT = "ElectriCore"


class ApiUrlManquante(ValueError):
    """`KIOSQUE__API_URL` absente — fail-fast, message actionnable.

    L'URL d'une box réelle est de la config de déploiement (config.env du
    provider, `electricore-secrets`, ADR-0044) — jamais un défaut codé en dur ici.
    """


def apps_actives() -> list[str]:
    """`KIOSQUE__APPS` : noms séparés par virgule, sélection à monter dans le catalogue."""
    brut = os.environ.get("KIOSQUE__APPS", "")
    return [nom.strip() for nom in brut.split(",") if nom.strip()]


def titre() -> str:
    """`KIOSQUE__TITRE` : nom de l'entité, affiché par l'accueil."""
    return os.environ.get("KIOSQUE__TITRE", _TITRE_DEFAUT)


def api_url() -> str:
    """`KIOSQUE__API_URL` : API `electricore` interrogée par les notebooks (#705).

    Requise, sans défaut — l'URL d'une box réelle vit dans le config.env du
    provider (`electricore-secrets`, ADR-0044), pas dans ce code versionné.
    Lue paresseusement (à l'usage, pas à l'import) ; fail-fast si absente.
    """
    valeur = os.environ.get("KIOSQUE__API_URL")
    if not valeur:
        raise ApiUrlManquante("KIOSQUE__API_URL manquante — configure-la dans le config.env du provider.")
    return valeur
