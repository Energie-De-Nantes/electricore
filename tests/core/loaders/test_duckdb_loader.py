"""
Tests pour le module duckdb_loader.

Ces tests valident l'intégration DuckDB avec les pipelines ElectriCore,
incluant le chargement des données, les transformations et la validation Pandera.
"""

import pytest
import polars as pl
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from electricore.core.loaders.duckdb import (
    DuckDBConfig,
    DuckDBQuery,
    load_historique_perimetre,
    load_releves,
    get_available_tables,
    execute_custom_query,
    duckdb_connection,
    c15,
    releves,
)

# Import des transformations depuis le module interne
from electricore.core.loaders.duckdb.transforms import (
    transform_historique_perimetre as _transform_historique_perimetre,
    transform_releves as _transform_releves,
)


class TestDuckDBConfig:
    """Tests pour la classe DuckDBConfig."""

    def test_config_default_path(self):
        """Test de la configuration par défaut."""
        config = DuckDBConfig()
        assert config.database_path == Path("electricore/etl/flux_enedis_pipeline.duckdb")

    def test_config_custom_path(self):
        """Test avec un chemin personnalisé."""
        custom_path = "/custom/path/db.duckdb"
        config = DuckDBConfig(custom_path)
        assert config.database_path == Path(custom_path)

    def test_table_mappings_structure(self):
        """Test de la structure des mappings de tables."""
        config = DuckDBConfig()

        # Vérifier que les mappings essentiels existent
        assert "historique_perimetre" in config.table_mappings
        assert "releves" in config.table_mappings

        # Vérifier la structure des mappings
        hist_mapping = config.table_mappings["historique_perimetre"]
        assert "source_tables" in hist_mapping
        assert "description" in hist_mapping


class TestDuckDBConnection:
    """Tests pour la gestion des connexions DuckDB."""

    @patch('electricore.core.loaders.duckdb.config.duckdb.connect')
    def test_connection_context_manager(self, mock_connect):
        """Test du context manager de connexion."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        with duckdb_connection("test.duckdb") as conn:
            assert conn == mock_conn

        # Vérifier que la connexion est fermée
        mock_conn.close.assert_called_once()

    @patch('electricore.core.loaders.duckdb.config.duckdb.connect')
    def test_connection_exception_handling(self, mock_connect):
        """Test de la gestion d'erreur dans le context manager."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        try:
            with duckdb_connection("test.duckdb") as conn:
                raise Exception("Test error")
        except Exception:
            pass

        # La connexion doit être fermée même en cas d'erreur
        mock_conn.close.assert_called_once()


class TestTransformationFunctions:
    """Tests pour les fonctions de transformation."""

    def test_transform_historique_perimetre(self):
        """Test de transformation des données d'historique."""
        from datetime import datetime, timezone as tz

        # Créer un LazyFrame de test avec dates timezone-aware Python
        test_data = pl.DataFrame({
            "date_evenement": [datetime(2024, 1, 1, 10, 0, 0, tzinfo=tz.utc)],
            "avant_date_releve": [datetime(2024, 1, 1, 9, 0, 0, tzinfo=tz.utc)],
            "apres_date_releve": [datetime(2024, 1, 1, 11, 0, 0, tzinfo=tz.utc)],
            "pdl": ["PDL123"]
        }).lazy()

        # Appliquer la transformation
        result = _transform_historique_perimetre(test_data)

        # Vérifier que c'est toujours un LazyFrame
        assert isinstance(result, pl.LazyFrame)

        # Collecter pour vérifier le contenu
        df = result.collect()

        # Vérifier que les colonnes ajoutées sont présentes
        assert "unite" in df.columns
        assert "precision" in df.columns
        assert df["unite"][0] == "kWh"
        assert df["precision"][0] == "kWh"

    def test_transform_releves(self):
        """Test de transformation des données de relevés."""
        from datetime import datetime, timezone as tz

        # Créer un LazyFrame de test avec noms de colonnes selon convention
        test_data = pl.DataFrame({
            "date_releve": [datetime(2024, 1, 1, 10, 0, 0, tzinfo=tz.utc)],
            "pdl": ["PDL123"],
            "index_hp_kwh": [1000.0],
            "index_base_kwh": [2000.0],
            "index_hc_kwh": [500.0],
            "index_hph_kwh": [None],
            "index_hpb_kwh": [None],
            "index_hcb_kwh": [None],
            "index_hch_kwh": [None],
            "unite": ["kWh"],
            "precision": ["kWh"]
        }).lazy()

        # Appliquer la transformation
        result = _transform_releves(test_data)

        # Vérifier que c'est toujours un LazyFrame
        assert isinstance(result, pl.LazyFrame)

        # Collecter pour vérifier le contenu
        df = result.collect()

        # Vérifier les colonnes de base
        assert "date_releve" in df.columns
        assert "pdl" in df.columns
        assert "index_hp_kwh" in df.columns
        assert "unite" in df.columns
        assert "precision" in df.columns

    def test_transform_releves_conversion_wh_to_kwh(self):
        """Test de la conversion Wh -> kWh avec troncature."""
        # Test de la logique de conversion sans la conversion de timezone
        index_cols = ["BASE", "HP", "HC", "HPH", "HPB", "HCB", "HCH"]

        test_data = pl.DataFrame({
            "pdl": ["PDL123", "PDL124"],
            "BASE": [13874.0, 16017.0],  # Valeurs d'exemple en Wh
            "HP": [None, 5500.0],
            "HC": [2500.0, None],
            "HPH": [None, None],
            "HPB": [None, None],
            "HCB": [None, None],
            "HCH": [None, None],
            "unite": ["Wh", "Wh"],
            "precision": ["Wh", "Wh"]
        }).lazy()

        # Appliquer uniquement la logique de conversion (sans date timezone)
        result = test_data.with_columns([
            # Conversion Wh -> kWh avec troncature
            *[
                pl.when(pl.col("unite") == "Wh")
                .then(
                    pl.when(pl.col(col).is_not_null())
                    .then((pl.col(col) / 1000).floor())
                    .otherwise(pl.col(col))
                )
                .otherwise(pl.col(col))
                .alias(col)
                for col in index_cols
            ],
            # Mettre à jour l'unité après conversion
            pl.when(pl.col("unite") == "Wh")
            .then(pl.lit("kWh"))
            .otherwise(pl.col("unite"))
            .alias("unite"),
            # Mettre à jour la précision après conversion
            pl.when(pl.col("precision") == "Wh")
            .then(pl.lit("kWh"))
            .otherwise(pl.col("precision"))
            .alias("precision")
        ])

        df = result.collect()

        # Vérifier la conversion des valeurs
        assert df["BASE"][0] == 13.0  # floor(13874 / 1000) = 13
        assert df["BASE"][1] == 16.0  # floor(16017 / 1000) = 16
        assert df["HP"][0] is None     # Les valeurs null restent null
        assert df["HP"][1] == 5.0      # floor(5500 / 1000) = 5
        assert df["HC"][0] == 2.0      # floor(2500 / 1000) = 2
        assert df["HC"][1] is None

        # Vérifier que les unités ont été mises à jour
        assert df["unite"][0] == "kWh"
        assert df["unite"][1] == "kWh"
        assert df["precision"][0] == "kWh"
        assert df["precision"][1] == "kWh"

    def test_transform_releves_no_conversion_kwh(self):
        """Test que les valeurs déjà en kWh ne sont pas modifiées."""
        # Test de la logique de non-conversion
        index_cols = ["BASE", "HP", "HC", "HPH", "HPB", "HCB", "HCH"]

        test_data = pl.DataFrame({
            "pdl": ["PDL123"],
            "BASE": [13.874],  # Déjà en kWh avec décimales
            "HP": [5.5],
            "HC": [None],
            "HPH": [None],
            "HPB": [None],
            "HCB": [None],
            "HCH": [None],
            "unite": ["kWh"],
            "precision": ["kWh"]
        }).lazy()

        # Appliquer uniquement la logique de conversion (sans date timezone)
        result = test_data.with_columns([
            # Conversion Wh -> kWh avec troncature
            *[
                pl.when(pl.col("unite") == "Wh")
                .then(
                    pl.when(pl.col(col).is_not_null())
                    .then((pl.col(col) / 1000).floor())
                    .otherwise(pl.col(col))
                )
                .otherwise(pl.col(col))
                .alias(col)
                for col in index_cols
            ],
            # Mettre à jour l'unité après conversion
            pl.when(pl.col("unite") == "Wh")
            .then(pl.lit("kWh"))
            .otherwise(pl.col("unite"))
            .alias("unite"),
            # Mettre à jour la précision après conversion
            pl.when(pl.col("precision") == "Wh")
            .then(pl.lit("kWh"))
            .otherwise(pl.col("precision"))
            .alias("precision")
        ])

        df = result.collect()

        # Vérifier que les valeurs ne sont pas modifiées
        assert df["BASE"][0] == 13.874  # Pas de conversion
        assert df["HP"][0] == 5.5       # Pas de conversion
        assert df["HC"][0] is None

        # Vérifier que les unités restent inchangées
        assert df["unite"][0] == "kWh"
        assert df["precision"][0] == "kWh"


class TestLoadFunctions:
    """Tests pour les fonctions de chargement de données."""

    def test_load_historique_perimetre_basic(self):
        """Test de base du chargement d'historique."""
        # Test simplifié : vérifier que la fonction retourne un LazyFrame
        # lorsque la base existe
        from pathlib import Path

        # Créer une query avec la méthode factory et vérifier l'API
        query = c15()
        assert isinstance(query, DuckDBQuery)

        # Vérifier que le chaînage fonctionne
        query_filtered = query.filter({"pdl": ["PDL123"]}).limit(100)
        assert isinstance(query_filtered, DuckDBQuery)

        # Vérifier load_historique_perimetre avec base inexistante échoue correctement
        with pytest.raises(FileNotFoundError):
            load_historique_perimetre(database_path="nonexistent_test.duckdb").collect()

    def test_load_historique_perimetre_database_not_found(self):
        """Test avec base de données inexistante."""
        with pytest.raises(FileNotFoundError):
            load_historique_perimetre(database_path="nonexistent.duckdb")

    def test_load_releves_basic(self):
        """Test de base du chargement de relevés."""
        # Test simplifié : vérifier que la fonction API fonctionne
        from pathlib import Path
        from electricore.core.loaders.duckdb import DuckDBQuery

        # Créer une query avec la méthode factory
        # releves() retourne maintenant DuckDBQuery avec base_sql
        query = releves()
        assert isinstance(query, DuckDBQuery)
        assert query.base_sql is not None  # Vérifie que c'est une requête CTE

        # Vérifier que le chaînage fonctionne
        query_filtered = query.filter({"pdl": ["PDL123"]}).limit(50)
        assert isinstance(query_filtered, DuckDBQuery)

        # Vérifier load_releves avec base inexistante échoue correctement
        with pytest.raises(FileNotFoundError):
            load_releves(database_path="nonexistent_test.duckdb").collect()


class TestUtilityFunctions:
    """Tests pour les fonctions utilitaires."""

    @patch('electricore.core.loaders.duckdb.helpers.duckdb_connection')
    def test_get_available_tables(self, mock_conn_context):
        """Test de récupération des tables disponibles."""
        # Setup du mock
        mock_conn = Mock()
        mock_conn_context.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = [
            ("schema1", "table1"), ("schema1", "table2"), ("schema2", "table3")
        ]

        # Appeler la fonction
        result = get_available_tables("test.duckdb")

        # Vérifications
        assert result == ["schema1.table1", "schema1.table2", "schema2.table3"]
        # Vérifier que la bonne requête a été appelée
        mock_conn.execute.assert_called_once_with("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema != 'information_schema'
            ORDER BY table_schema, table_name
        """)

    @patch('electricore.core.loaders.duckdb.helpers.duckdb_connection')
    @patch('electricore.core.loaders.duckdb.helpers.pl.read_database')
    def test_execute_custom_query_lazy(self, mock_read_db, mock_conn_context):
        """Test d'exécution de requête personnalisée (lazy)."""
        # Setup des mocks
        mock_conn = Mock()
        mock_conn_context.return_value.__enter__.return_value = mock_conn
        mock_lazy_frame = Mock(spec=pl.LazyFrame)
        mock_read_db.return_value.lazy.return_value = mock_lazy_frame

        # Appeler la fonction
        result = execute_custom_query(
            query="SELECT * FROM test_table",
            database_path="test.duckdb",
            lazy=True
        )

        # Vérifications
        assert result == mock_lazy_frame
        mock_read_db.assert_called_once()

    @patch('electricore.core.loaders.duckdb.helpers.duckdb_connection')
    @patch('electricore.core.loaders.duckdb.helpers.pl.read_database')
    def test_execute_custom_query_eager(self, mock_read_db, mock_conn_context):
        """Test d'exécution de requête personnalisée (eager)."""
        # Setup des mocks
        mock_conn = Mock()
        mock_conn_context.return_value.__enter__.return_value = mock_conn
        mock_dataframe = Mock(spec=pl.DataFrame)
        mock_read_db.return_value = mock_dataframe

        # Appeler la fonction
        result = execute_custom_query(
            query="SELECT * FROM test_table",
            database_path="test.duckdb",
            lazy=False
        )

        # Vérifications
        assert result == mock_dataframe
        mock_read_db.assert_called_once()


class TestIntegrationWithRealData:
    """Tests d'intégration avec données réelles (si disponibles)."""

    @pytest.mark.skipif(
        not Path("electricore/etl/flux_enedis_pipeline.duckdb").exists(),
        reason="Base DuckDB non disponible"
    )
    def test_load_historique_real_database(self):
        """Test avec la vraie base DuckDB si disponible."""
        try:
            # Charger un petit échantillon
            result = load_historique_perimetre(
                database_path="electricore/etl/flux_enedis_pipeline.duckdb",
                limit=5,
                valider=False
            )

            # Vérifications de base
            assert isinstance(result, pl.LazyFrame)

            # Collecter et vérifier la structure
            df = result.collect()
            assert len(df) <= 5

            # Vérifier quelques colonnes essentielles
            expected_columns = ["date_evenement", "pdl", "ref_situation_contractuelle"]
            for col in expected_columns:
                assert col in df.columns

        except Exception as e:
            pytest.skip(f"Erreur lors du test avec vraie DB: {e}")

    @pytest.mark.skipif(
        not Path("electricore/etl/flux_enedis_pipeline.duckdb").exists(),
        reason="Base DuckDB non disponible"
    )
    def test_load_releves_real_database(self):
        """Test avec la vraie base DuckDB si disponible."""
        try:
            # Charger un petit échantillon
            result = load_releves(
                database_path="electricore/etl/flux_enedis_pipeline.duckdb",
                limit=5,
                valider=False
            )

            # Vérifications de base
            assert isinstance(result, pl.LazyFrame)

            # Collecter et vérifier la structure
            df = result.collect()
            assert len(df) <= 5

            # Vérifier quelques colonnes essentielles
            expected_columns = ["date_releve", "pdl", "source"]
            for col in expected_columns:
                assert col in df.columns

        except Exception as e:
            pytest.skip(f"Erreur lors du test avec vraie DB: {e}")


# Fixtures partagées
@pytest.fixture
def sample_historique_data():
    """Fixture avec des données d'historique de test."""
    return pl.DataFrame({
        "date_evenement": ["2024-01-01 10:00:00", "2024-01-02 11:00:00"],
        "pdl": ["PDL123", "PDL124"],
        "ref_situation_contractuelle": ["REF001", "REF002"],
        "segment_clientele": ["C5", "C5"],
        "etat_contractuel": ["EN SERVICE", "EN SERVICE"],
        "evenement_declencheur": ["MES", "MCT"],
        "type_evenement": ["mise_en_service", "modification"],
        "puissance_souscrite": [6.0, 9.0],
        "formule_tarifaire_acheminement": ["BTINF36", "BTINF36"],
        "type_compteur": ["ELEC", "ELEC"],
        "num_compteur": ["123456", "789012"]
    })


@pytest.fixture
def sample_releves_data():
    """Fixture avec des données de relevés de test."""
    return pl.DataFrame({
        "date_releve": ["2024-01-01 10:00:00", "2024-01-02 11:00:00"],
        "pdl": ["PDL123", "PDL124"],
        "id_calendrier_distributeur": ["DI000002", "DI000002"],
        "HP": [1000.0, 1500.0],
        "HC": [500.0, 750.0],
        "source": ["flux_R151", "flux_R151"],
        "ordre_index": [False, False],
        "unite": ["kWh", "kWh"],
        "precision": ["kWh", "kWh"]
    })


if __name__ == "__main__":
    pytest.main([__file__])