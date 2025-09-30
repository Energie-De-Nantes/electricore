"""
Configuration globale pytest et fixtures partagées pour ElectriCore.

Ce module centralise les fixtures réutilisables à travers tous les tests,
incluant les connexions temporaires, les données de test minimales, et
les hooks pytest pour personnaliser le comportement des tests.
"""

import tempfile
from pathlib import Path
from typing import Generator
import pytest
import polars as pl
import duckdb
from datetime import datetime


# =========================================================================
# FIXTURES - DONNÉES DE TEST MINIMALES
# =========================================================================


@pytest.fixture
def sample_pdl_list() -> list[str]:
    """Liste de PDLs de test pour fixtures."""
    return ["PDL00001", "PDL00002", "PDL00003"]


@pytest.fixture
def sample_dates() -> dict[str, datetime]:
    """Dates communes pour tests."""
    return {
        "debut_2024": datetime(2024, 1, 1, 0, 0, 0),
        "mi_2024": datetime(2024, 6, 15, 12, 0, 0),
        "fin_2024": datetime(2024, 12, 31, 23, 59, 59),
        "debut_2025": datetime(2025, 1, 1, 0, 0, 0),
    }


@pytest.fixture
def minimal_historique_df() -> pl.DataFrame:
    """
    DataFrame historique minimal pour tests unitaires.

    Contient le strict minimum pour tester les pipelines périmètre/abonnements.
    """
    return pl.DataFrame({
        "pdl": ["PDL00001", "PDL00001", "PDL00002"],
        "Date_Evenement": [
            datetime(2024, 1, 1, 9, 0, 0),
            datetime(2024, 6, 1, 10, 0, 0),
            datetime(2024, 3, 15, 14, 30, 0),
        ],
        "Evenement_Declencheur": ["MES", "MCT", "MES"],
        "Etat_Contractuel": ["EN SERVICE", "EN SERVICE", "EN SERVICE"],
        "Puissance_Souscrite": [6.0, 9.0, 3.0],
        "Formule_Tarifaire_Acheminement": ["BTINFCUST", "BTINFCUST", "BTINFCU4"],
        "ref_situation_contractuelle": ["REF001", "REF001", "REF002"],
    })


@pytest.fixture
def minimal_releves_df() -> pl.DataFrame:
    """
    DataFrame relevés minimal pour tests unitaires.

    Contient quelques relevés BASE et HP/HC.
    """
    return pl.DataFrame({
        "pdl": ["PDL00001", "PDL00001", "PDL00002"],
        "date_releve": [
            datetime(2024, 2, 1, 0, 0, 0),
            datetime(2024, 7, 1, 0, 0, 0),
            datetime(2024, 4, 1, 0, 0, 0),
        ],
        "BASE": [1500.0, None, 800.0],
        "HP": [None, 2000.0, None],
        "HC": [None, 1200.0, None],
        "Id_Calendrier_Distributeur": ["DI000001", "DI000002", "DI000001"],
        "source": ["flux_R151", "flux_R151", "flux_R151"],
    })


# =========================================================================
# FIXTURES - CONNEXIONS ET RESSOURCES
# =========================================================================


@pytest.fixture(scope="session")
def temp_duckdb_path() -> Generator[Path, None, None]:
    """
    Crée une base DuckDB temporaire pour tests d'intégration.

    Scope: session - une seule DB pour toute la session de test.
    Cleanup automatique à la fin.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_electricore.duckdb"
        yield db_path
        # Cleanup automatique par le context manager


@pytest.fixture
def duckdb_test_connection(temp_duckdb_path: Path) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    Connexion DuckDB temporaire pour tests.

    Scope: function - nouvelle connexion propre pour chaque test.
    """
    conn = duckdb.connect(str(temp_duckdb_path))
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def duckdb_with_sample_data(
    duckdb_test_connection: duckdb.DuckDBPyConnection,
    minimal_historique_df: pl.DataFrame,
    minimal_releves_df: pl.DataFrame,
) -> duckdb.DuckDBPyConnection:
    """
    Connexion DuckDB pré-chargée avec données minimales.

    Utile pour tests d'intégration des loaders/query builders.
    """
    # Créer les tables avec données minimales
    duckdb_test_connection.execute(
        "CREATE TABLE flux_c15 AS SELECT * FROM minimal_historique_df"
    )
    duckdb_test_connection.execute(
        "CREATE TABLE flux_r151 AS SELECT * FROM minimal_releves_df"
    )

    return duckdb_test_connection


# =========================================================================
# HOOKS PYTEST - PERSONNALISATION COMPORTEMENT
# =========================================================================


def pytest_configure(config):
    """Hook appelé après parsing de la configuration."""
    # Ajouter des informations dans le terminal
    config.addinivalue_line(
        "markers", "wip: Tests en cours de développement (work in progress)"
    )


def pytest_collection_modifyitems(config, items):
    """
    Hook appelé après collection des tests.

    Permet d'ajouter automatiquement des markers selon des conventions.
    """
    for item in items:
        # Auto-marker les tests selon leur emplacement
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)

        # Auto-marker les tests DuckDB
        if "duckdb" in item.nodeid.lower():
            item.add_marker(pytest.mark.duckdb)

        # Auto-marker les tests Odoo
        if "odoo" in item.nodeid.lower():
            item.add_marker(pytest.mark.odoo)


def pytest_report_header(config):
    """Ajoute des informations custom dans le header du rapport."""
    return [
        "ElectriCore Test Suite",
        f"Test data path: {Path(__file__).parent / 'fixtures'}",
    ]


# =========================================================================
# FIXTURES - VALIDATION ET ASSERTIONS
# =========================================================================


@pytest.fixture
def assert_polars_schema_compatible():
    """
    Helper fixture pour assertions de compatibilité de schéma Polars.

    Usage:
        def test_foo(assert_polars_schema_compatible):
            result_df = pipeline()
            assert_polars_schema_compatible(result_df, expected_columns)
    """
    def _assert_schema(df: pl.DataFrame, expected_columns: list[str]):
        """Vérifie que le DataFrame contient les colonnes attendues."""
        actual_columns = set(df.columns)
        expected_set = set(expected_columns)

        missing = expected_set - actual_columns
        extra = actual_columns - expected_set

        assert not missing, f"Colonnes manquantes: {missing}"
        assert not extra, f"Colonnes inattendues: {extra}"

    return _assert_schema


@pytest.fixture
def assert_no_nulls_in_columns():
    """
    Helper fixture pour assertions de non-nullité.

    Usage:
        def test_foo(assert_no_nulls_in_columns):
            result_df = pipeline()
            assert_no_nulls_in_columns(result_df, ["pdl", "date_releve"])
    """
    def _assert_no_nulls(df: pl.DataFrame, columns: list[str]):
        """Vérifie qu'il n'y a pas de nulls dans les colonnes spécifiées."""
        for col in columns:
            null_count = df[col].null_count()
            assert null_count == 0, f"Colonne '{col}' contient {null_count} valeurs nulles"

    return _assert_no_nulls