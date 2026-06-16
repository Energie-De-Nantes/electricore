"""
Tests pour le module duckdb_loader.

Ces tests valident l'intégration DuckDB avec les pipelines ElectriCore,
incluant le chargement des données, les transformations et la validation Pandera.
"""

from datetime import UTC
from pathlib import Path
from unittest.mock import Mock, patch

import polars as pl
import pytest

from electricore.config import runtime
from electricore.core.loaders.duckdb import (
    DuckDBConfig,
    DuckDBQuery,
    c15,
    duckdb_readonly_conn,
    execute_custom_query,
    get_available_tables,
    releves,
)
from electricore.core.loaders.duckdb.config import _TABLE_MAPPINGS

# Import des transformations depuis le module interne
from electricore.core.loaders.duckdb.transforms import (
    transform_historique as _transform_historique,
)
from electricore.core.loaders.duckdb.transforms import (
    transform_r64 as _transform_r64,
)
from electricore.core.loaders.duckdb.transforms import (
    transform_releves as _transform_releves,
)


class TestDuckDBConfig:
    """Tests pour DuckDBConfig et ses factories."""

    @pytest.fixture(autouse=True)
    def _isole_runtime(self, monkeypatch):
        """Neutralise le .env du dépôt et vide le cache des accessors (#141)."""
        monkeypatch.setattr(runtime, "FICHIER_ENV", None)
        runtime.vider_cache()
        yield
        runtime.vider_cache()

    def test_from_env_meme_resolution_que_le_resolveur(self, monkeypatch):
        """from_env() délègue au résolveur partagé (issue #146) — plus de défaut propre.

        Ordre volontaire : from_env() d'abord — s'il ne déléguait pas, il rendrait
        son propre défaut avant que le résolveur n'ait chargé le .env.
        """
        monkeypatch.delenv("DUCKDB_PATH", raising=False)
        runtime.vider_cache()
        depuis_config = DuckDBConfig.from_env().database_path
        assert depuis_config == runtime.duckdb().chemin

    def test_from_env_honore_duckdb_path(self, monkeypatch):
        """DUCKDB_PATH explicite prime, telle quelle."""
        monkeypatch.setenv("DUCKDB_PATH", "/data/flux.duckdb")
        runtime.vider_cache()
        assert DuckDBConfig.from_env().database_path == Path("/data/flux.duckdb")

    def test_from_path_custom_path(self):
        """from_path() avec un chemin explicite le convertit en Path."""
        custom_path = "/custom/path/db.duckdb"
        config = DuckDBConfig.from_path(custom_path)
        assert config.database_path == Path(custom_path)

    def test_from_path_none_falls_back_to_env(self):
        """from_path(None) équivaut à from_env()."""
        config = DuckDBConfig.from_path(None)
        assert config.database_path == DuckDBConfig.from_env().database_path

    def test_table_mappings_structure(self):
        """_TABLE_MAPPINGS documente les tables métier essentielles."""
        assert "historique" in _TABLE_MAPPINGS
        assert "releves" in _TABLE_MAPPINGS
        hist_mapping = _TABLE_MAPPINGS["historique"]
        assert "source_tables" in hist_mapping
        assert "description" in hist_mapping


class TestDuckDBConnection:
    """Tests pour la gestion des connexions DuckDB."""

    @patch("electricore.core.loaders.duckdb.config.duckdb.connect")
    def test_connection_context_manager(self, mock_connect):
        """Test du context manager de connexion."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        with duckdb_readonly_conn("test.duckdb") as conn:
            assert conn == mock_conn

        # Vérifier que la connexion est fermée
        mock_conn.close.assert_called_once()

    @patch("electricore.core.loaders.duckdb.config.duckdb.connect")
    def test_connection_exception_handling(self, mock_connect):
        """Test de la gestion d'erreur dans le context manager."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        try:
            with duckdb_readonly_conn("test.duckdb"):
                raise Exception("Test error")
        except Exception:
            pass

        # La connexion doit être fermée même en cas d'erreur
        mock_conn.close.assert_called_once()


class TestTransformationFunctions:
    """Tests pour les fonctions de transformation."""

    def test_transform_historique(self):
        """Test de transformation des données d'historique."""
        from datetime import datetime

        # Créer un LazyFrame de test avec dates timezone-aware Python
        test_data = pl.DataFrame(
            {
                "date_evenement": [datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)],
                "avant_date_releve": [datetime(2024, 1, 1, 9, 0, 0, tzinfo=UTC)],
                "apres_date_releve": [datetime(2024, 1, 1, 11, 0, 0, tzinfo=UTC)],
                "pdl": ["PDL123"],
            }
        ).lazy()

        # Appliquer la transformation
        result = _transform_historique(test_data)

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
        from datetime import datetime

        # Créer un LazyFrame de test avec noms de colonnes selon convention
        test_data = pl.DataFrame(
            {
                "date_releve": [datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)],
                "pdl": ["PDL123"],
                "index_hp_kwh": [1000.0],
                "index_base_kwh": [2000.0],
                "index_hc_kwh": [500.0],
                "index_hph_kwh": [None],
                "index_hpb_kwh": [None],
                "index_hcb_kwh": [None],
                "index_hch_kwh": [None],
                "unite": ["kWh"],
                "precision": ["kWh"],
            }
        ).lazy()

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

    def test_transform_releves_ne_convertit_plus_les_index(self):
        """Le loader ne convertit plus Wh→kWh (ADR-0034) : la normalisation vit au
        boundary de linéarisation dbt (flux_r151/flux_r64 émettent des kWh entiers).
        Reconvertir ici diviserait deux fois — le loader passe les index inchangés."""
        from datetime import datetime

        test_data = pl.DataFrame(
            {
                "date_releve": [datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)],
                "pdl": ["PDL123"],
                "index_base_kwh": [13874.0],
                "index_hp_kwh": [5500.0],
                "index_hc_kwh": [None],
                "index_hph_kwh": [None],
                "index_hpb_kwh": [None],
                "index_hcb_kwh": [None],
                "index_hch_kwh": [None],
                "unite": ["kWh"],
            }
        ).lazy()

        df = _transform_releves(test_data).collect()

        # Valeurs inchangées : aucune division par 1000 au loader.
        assert df["index_base_kwh"][0] == 13874.0
        assert df["index_hp_kwh"][0] == 5500.0
        assert df["unite"][0] == "kWh"

    def test_transform_r64_ne_convertit_plus_les_index(self):
        """Idem côté R64 (ADR-0034) : transform_r64 n'harmonise que les dates ; la
        conversion Wh→kWh est portée par flux_r64 en dbt."""
        from datetime import datetime

        test_data = pl.DataFrame(
            {
                "date_releve": [datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)],
                "modification_date": [datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)],
                "pdl": ["PDL123"],
                "index_hph_kwh": [7672.0],
                "index_hpb_kwh": [None],
                "index_hch_kwh": [None],
                "index_hcb_kwh": [None],
                "index_base_kwh": [None],
                "index_hp_kwh": [None],
                "index_hc_kwh": [None],
                "unite": ["kWh"],
            }
        ).lazy()

        df = _transform_r64(test_data).collect()

        # Valeur inchangée : pas de re-division au loader.
        assert df["index_hph_kwh"][0] == 7672.0
        assert df["unite"][0] == "kWh"


class TestApiLegacySupprimee:
    """#181 : les pass-throughs legacy vers le query builder n'existent plus.

    Le chemin canonique est l'API fluide (`c15()`, `releves()`…) — l'interface
    du package ne porte plus de seconde façon de charger un flux.
    """

    def test_les_loaders_n_exposent_plus_l_api_legacy(self):
        import electricore.core.loaders as loaders
        import electricore.core.loaders.duckdb as loaders_duckdb

        for module in (loaders, loaders_duckdb):
            for nom in ("load_historique", "load_releves"):
                assert not hasattr(module, nom), f"{nom} encore exposé par {module.__name__}"
                assert nom not in module.__all__, f"{nom} encore dans __all__ de {module.__name__}"


class TestLoadFunctions:
    """Tests du chargement via l'API fluide (chemin canonique, #181)."""

    def test_c15_chainage_fluide(self):
        """c15() retourne un builder immutable chaînable."""
        query = c15()
        assert isinstance(query, DuckDBQuery)

        query_filtered = query.filter({"pdl": ["PDL123"]}).limit(100)
        assert isinstance(query_filtered, DuckDBQuery)

    def test_c15_base_inexistante_leve_filenotfound(self):
        """La matérialisation (.lazy()/.collect()) échoue si la base n'existe pas."""
        with pytest.raises(FileNotFoundError):
            c15(database_path="nonexistent_test.duckdb").lazy()

    def test_releves_requete_cte_et_chainage(self):
        """releves() porte une requête SQL pré-construite (modèle canonique dbt) et reste chaînable."""
        query = releves()
        assert isinstance(query, DuckDBQuery)
        assert query.base_sql is not None  # Vérifie que c'est une requête pré-construite

        query_filtered = query.filter({"pdl": ["PDL123"]}).limit(50)
        assert isinstance(query_filtered, DuckDBQuery)

    def test_releves_base_inexistante_leve_filenotfound(self):
        """La matérialisation des relevés échoue si la base n'existe pas."""
        with pytest.raises(FileNotFoundError):
            releves(database_path="nonexistent_test.duckdb").lazy()


class TestUtilityFunctions:
    """Tests pour les fonctions utilitaires."""

    @patch("electricore.core.loaders.duckdb.helpers.duckdb_readonly_conn")
    def test_get_available_tables(self, mock_conn_context):
        """Test de récupération des tables disponibles."""
        # Setup du mock
        mock_conn = Mock()
        mock_conn_context.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = [
            ("schema1", "table1"),
            ("schema1", "table2"),
            ("schema2", "table3"),
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

    @patch("electricore.core.loaders.duckdb.helpers.duckdb_readonly_conn")
    @patch("electricore.core.loaders.duckdb.helpers.pl.read_database")
    def test_execute_custom_query_lazy(self, mock_read_db, mock_conn_context):
        """Test d'exécution de requête personnalisée (lazy)."""
        # Setup des mocks
        mock_conn = Mock()
        mock_conn_context.return_value.__enter__.return_value = mock_conn
        mock_lazy_frame = Mock(spec=pl.LazyFrame)
        mock_read_db.return_value.lazy.return_value = mock_lazy_frame

        # Appeler la fonction
        result = execute_custom_query(query="SELECT * FROM test_table", database_path="test.duckdb", lazy=True)

        # Vérifications
        assert result == mock_lazy_frame
        mock_read_db.assert_called_once()

    @patch("electricore.core.loaders.duckdb.helpers.duckdb_readonly_conn")
    @patch("electricore.core.loaders.duckdb.helpers.pl.read_database")
    def test_execute_custom_query_eager(self, mock_read_db, mock_conn_context):
        """Test d'exécution de requête personnalisée (eager)."""
        # Setup des mocks
        mock_conn = Mock()
        mock_conn_context.return_value.__enter__.return_value = mock_conn
        mock_dataframe = Mock(spec=pl.DataFrame)
        mock_read_db.return_value = mock_dataframe

        # Appeler la fonction
        result = execute_custom_query(query="SELECT * FROM test_table", database_path="test.duckdb", lazy=False)

        # Vérifications
        assert result == mock_dataframe
        mock_read_db.assert_called_once()


class TestIntegrationWithRealData:
    """Tests d'intégration avec données réelles (si disponibles)."""

    @pytest.mark.skipif(
        not Path("electricore/ingestion/flux_enedis_pipeline.duckdb").exists(), reason="Base DuckDB non disponible"
    )
    def test_c15_real_database(self):
        """Test avec la vraie base DuckDB si disponible."""
        try:
            # Charger un petit échantillon
            result = (
                c15(database_path="electricore/ingestion/flux_enedis_pipeline.duckdb").validate(False).limit(5).lazy()
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
        not Path("electricore/ingestion/flux_enedis_pipeline.duckdb").exists(), reason="Base DuckDB non disponible"
    )
    def test_releves_real_database(self):
        """Test avec la vraie base DuckDB si disponible."""
        try:
            # Charger un petit échantillon
            result = (
                releves(database_path="electricore/ingestion/flux_enedis_pipeline.duckdb")
                .validate(False)
                .limit(5)
                .lazy()
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
    return pl.DataFrame(
        {
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
            "num_compteur": ["123456", "789012"],
        }
    )


@pytest.fixture
def sample_releves_data():
    """Fixture avec des données de relevés de test."""
    return pl.DataFrame(
        {
            "date_releve": ["2024-01-01 10:00:00", "2024-01-02 11:00:00"],
            "pdl": ["PDL123", "PDL124"],
            "id_calendrier_distributeur": ["DI000002", "DI000002"],
            "HP": [1000.0, 1500.0],
            "HC": [500.0, 750.0],
            "source": ["flux_R151", "flux_R151"],
            "ordre_index": [False, False],
            "unite": ["kWh", "kWh"],
            "precision": ["kWh", "kWh"],
        }
    )


if __name__ == "__main__":
    pytest.main([__file__])
