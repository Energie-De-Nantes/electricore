"""
Configuration et gestion des connexions DuckDB.

Ce module fournit les primitives de configuration et de connexion
pour l'accès aux bases DuckDB dans un style fonctionnel.
"""

import logging
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

# Politique de retry lorsque le writer ETL détient le verrou exclusif.
# 3 essais × 1s couvrent les fenêtres typiques de checkpoint DLT.
_LOCK_RETRY_ATTEMPTS = 3
_LOCK_RETRY_BACKOFF_S = 1.0


class DuckDBConfig:
    """Configuration pour les connexions DuckDB."""

    def __init__(self, database_path: str | Path | None = None):
        """
        Initialise la configuration DuckDB.

        Args:
            database_path: Chemin vers la base DuckDB. Si None, utilise la config par défaut.
        """
        if database_path is None:
            self.database_path = Path(os.getenv("DUCKDB_PATH", "electricore/etl/flux_enedis_pipeline.duckdb"))
        else:
            self.database_path = Path(database_path)

        # Mapping des tables DuckDB vers schémas métier
        self.table_mappings = {
            "historique": {
                "source_tables": ["flux_enedis.flux_c15"],
                "description": "Historique des événements contractuels avec relevés avant/après",
            },
            "releves": {
                "source_tables": ["flux_enedis.flux_r151", "flux_enedis.flux_r15"],
                "description": "Relevés de compteurs unifiés depuis R151 et R15",
            },
        }


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

    Le writer ETL peut prendre un lock pendant un checkpoint DLT ou un
    `EXPORT DATABASE` ; on retente brièvement pour absorber ces pics sans
    casser les lectures API/notebooks. Pas de retry sur fichier introuvable.
    """
    for attempt in range(_LOCK_RETRY_ATTEMPTS):
        try:
            conn = duckdb.connect(str(database_path), read_only=True)
        except duckdb.IOException as exc:
            if not _is_lock_error(exc) or attempt == _LOCK_RETRY_ATTEMPTS - 1:
                raise
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
