"""
Helper partagé pour charger la configuration Odoo depuis les variables d'environnement.

Utilisé par l'API, les notebooks, et tout module nécessitant une connexion Odoo.
La source de vérité est le fichier `.env` à la racine du projet.

Supporte deux environnements (test / prod) via le sélecteur ODOO_ENV.
"""

import os

from electricore.config.env import charger_env


def charger_config_odoo(env: str | None = None) -> dict:
    """
    Charge la configuration Odoo pour l'environnement spécifié.

    Args:
        env: "test" ou "prod". Si None, lit la variable ODOO_ENV
            (défaut : "test").

    Returns:
        dict: Configuration avec les clés url, db, username, password.

    Raises:
        ValueError: Si des variables ODOO_<ENV>_* sont manquantes dans .env.

    Example:
        >>> from electricore.config import charger_config_odoo
        >>> config = charger_config_odoo()           # env depuis ODOO_ENV
        >>> config = charger_config_odoo("prod")     # forcer prod
        >>> with OdooReader(config=config) as odoo:
        ...     ...
    """
    charger_env()
    env = env or os.getenv("ODOO_ENV", "test")
    prefix = f"ODOO_{env.upper()}_"

    config = {
        "url":      os.getenv(f"{prefix}URL", ""),
        "db":       os.getenv(f"{prefix}DB", ""),
        "username": os.getenv(f"{prefix}USERNAME", ""),
        "password": os.getenv(f"{prefix}PASSWORD", ""),
    }

    manquantes = [k for k, v in config.items() if not v]
    if manquantes:
        vars_manquantes = [f"{prefix}{k.upper()}" for k in manquantes]
        raise ValueError(
            f"Configuration Odoo [{env}] incomplète. "
            f"Définissez {', '.join(vars_manquantes)} dans le fichier .env"
        )

    return config
