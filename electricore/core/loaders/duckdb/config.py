"""
Configuration et gestion des connexions DuckDB.

Ce module fournit les primitives de configuration et de connexion
pour l'accès aux bases DuckDB dans un style fonctionnel.
"""

from pathlib import Path
from typing import Union
from contextlib import contextmanager

import duckdb


class DuckDBConfig:
    """Configuration pour les connexions DuckDB."""

    def __init__(self, database_path: Union[str, Path] = None):
        """
        Initialise la configuration DuckDB.

        Args:
            database_path: Chemin vers la base DuckDB. Si None, utilise la config par défaut.
        """
        if database_path is None:
            # Utiliser la base par défaut du projet
            self.database_path = Path("electricore/etl/flux_enedis_pipeline.duckdb")
        else:
            self.database_path = Path(database_path)

        # Mapping des tables DuckDB vers schémas métier
        self.table_mappings = {
            "historique_perimetre": {
                "source_tables": ["flux_enedis.flux_c15"],
                "description": "Historique des événements contractuels avec relevés avant/après"
            },
            "releves": {
                "source_tables": ["flux_enedis.flux_r151", "flux_enedis.flux_r15"],
                "description": "Relevés de compteurs unifiés depuis R151 et R15"
            }
        }


@contextmanager
def duckdb_connection(database_path: Union[str, Path]):
    """
    Context manager pour connexions DuckDB.

    Args:
        database_path: Chemin vers la base DuckDB

    Yields:
        duckdb.DuckDBPyConnection: Connexion active
    """
    conn = None
    try:
        conn = duckdb.connect(str(database_path), read_only=True)
        yield conn
    finally:
        if conn:
            conn.close()