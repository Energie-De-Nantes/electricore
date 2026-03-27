"""
Loader partagé pour le fichier .env.

Utilisé par l'API, les notebooks, et l'ETL pour charger les variables
d'environnement depuis le fichier .env à la racine du projet.
"""

import os
from pathlib import Path


def charger_env() -> None:
    """
    Charge le fichier .env dans os.environ.

    Cherche .env dans le répertoire courant puis à la racine du projet.
    Les variables déjà définies dans l'environnement ne sont pas écrasées
    (priorité aux variables système/déploiement sur le fichier .env).
    """
    candidates = [
        Path(".env"),
        Path(__file__).parents[2] / ".env",
    ]
    for candidate in candidates:
        if candidate.exists():
            with open(candidate) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, _, value = line.partition("=")
                        value = value.strip()
                        # Retirer les guillemets entourants (simples ou doubles)
                        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                            value = value[1:-1]
                        os.environ.setdefault(key.strip(), value)
            break
