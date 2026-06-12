"""
Configuration et gestion des connexions DuckDB.

Ce module fournit les primitives de configuration et de connexion
pour l'accès aux bases DuckDB dans un style fonctionnel.
"""

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import duckdb

from electricore.config import chemin_base_duckdb

logger = logging.getLogger(__name__)

# Politique de retry lorsque le writer d'ingestion détient le verrou exclusif.
# 3 essais × 1s couvrent les fenêtres typiques de checkpoint DLT.
_LOCK_RETRY_ATTEMPTS = 3
_LOCK_RETRY_BACKOFF_S = 1.0

# Mapping documentaire des tables DuckDB vers schémas métier (non utilisé en runtime).
_TABLE_MAPPINGS = {
    "historique": {
        "source_tables": ["flux_enedis.flux_c15"],
        "description": "Historique des événements contractuels avec relevés avant/après",
    },
    "releves": {
        "source_tables": ["flux_enedis.flux_r151", "flux_enedis.flux_r15"],
        "description": "Relevés de compteurs unifiés depuis R151 et R15",
    },
}


@dataclass(frozen=True, slots=True)
class DuckDBConfig:
    """Configuration pour les connexions DuckDB."""

    database_path: Path

    @classmethod
    def from_env(cls) -> "DuckDBConfig":
        return cls(chemin_base_duckdb())

    @classmethod
    def from_path(cls, path: str | Path | None) -> "DuckDBConfig":
        if path is None:
            return cls.from_env()
        return cls(Path(path))


class DuckDBLockError(duckdb.IOException):
    """Verrou exclusif persistant après épuisement des retries.

    Signale aux couches hautes (API) qu'un writer — typiquement l'ingestion
    ingestion — détient la base : situation banale et auto-résolutive, à présenter
    comme telle plutôt qu'en erreur générique (issue #171). Sous-classe
    `duckdb.IOException` pour ne pas casser les `except` existants.
    """


def _is_lock_error(exc: duckdb.IOException) -> bool:
    """Discrimine un verrou (récupérable) d'une erreur non récupérable.

    DuckDB n'expose qu'`IOException` ; on inspecte le message.
    """
    msg = str(exc).lower()
    if "does not exist" in msg or "no such file" in msg:
        return False
    return True


@contextmanager
def duckdb_readonly_conn(database_path: str | Path) -> Iterator[duckdb.DuckDBPyConnection]:
    """Ouvre une connexion DuckDB read-only avec retry sur lock exclusif.

    Le writer d'ingestion peut prendre un lock pendant un checkpoint DLT ou un
    `EXPORT DATABASE` ; on retente brièvement pour absorber ces pics sans
    casser les lectures API/notebooks. Pas de retry sur fichier introuvable.
    """
    for attempt in range(_LOCK_RETRY_ATTEMPTS):
        try:
            conn = duckdb.connect(str(database_path), read_only=True)
        except duckdb.IOException as exc:
            if not _is_lock_error(exc):
                raise
            if attempt == _LOCK_RETRY_ATTEMPTS - 1:
                raise DuckDBLockError(str(exc)) from exc
            logger.warning(
                "DuckDB verrouillé (tentative %d/%d), nouvelle tentative dans %.1fs",
                attempt + 1,
                _LOCK_RETRY_ATTEMPTS,
                _LOCK_RETRY_BACKOFF_S,
            )
            time.sleep(_LOCK_RETRY_BACKOFF_S)
            continue
        try:
            yield conn
        finally:
            conn.close()
        return
