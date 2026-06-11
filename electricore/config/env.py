"""
Loader partagé pour le fichier .env et résolution de l'environnement.

Utilisé par l'API, les notebooks, et l'ETL pour charger les variables
d'environnement depuis le fichier .env à la racine du projet, et pour
résoudre le chemin de la base DuckDB de production (issue #146).
"""

import os
from collections.abc import Mapping
from pathlib import Path

# Base de prod locale, ancrée sur le package (jamais sur le CWD).
_DEFAUT_BASE_DUCKDB = Path(__file__).parents[1] / "etl" / "flux_enedis_pipeline.duckdb"


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


def chemin_base_duckdb(env: Mapping[str, str] | None = None) -> Path:
    """Résolution unique du chemin de la base DuckDB de production.

    `DUCKDB_PATH` sinon défaut absolu ancré sur le dépôt — indépendant du CWD.

    Args:
        env: mapping d'environnement explicite (tests, résolution hors process).
            `None` = environnement réel : `.env` chargé via `charger_env()`
            puis `os.environ`.
    """
    if env is None:
        charger_env()
        env = os.environ
    brut = env.get("DUCKDB_PATH")
    return Path(brut) if brut else _DEFAUT_BASE_DUCKDB
