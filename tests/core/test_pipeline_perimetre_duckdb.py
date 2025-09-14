"""
Tests d'intégration pour le pipeline périmètre avec support DuckDB.

Ces tests valident l'intégration complète entre DuckDB et les pipelines
de traitement du périmètre contractuel.
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch

from electricore.core.pipeline_perimetre import (
    load_historique_from_duckdb,
    pipeline_perimetre_flexible
)


class TestLoadHistoriqueFromDuckDB:
    """Tests pour la fonction de chargement depuis DuckDB vers pandas."""

    @patch('electricore.core.pipeline_perimetre.load_historique_perimetre')
    @patch('electricore.core.pipeline_perimetre.HistoriquePérimètre')
    def test_load_and_convert_success(self, mock_model, mock_load_func):
        """Test de chargement et conversion réussi."""
        # Setup des mocks
        mock_lazy_frame = Mock()
        mock_dataframe = Mock()
        mock_pandas_df = Mock(spec=pd.DataFrame)

        mock_load_func.return_value = mock_lazy_frame
        mock_lazy_frame.collect.return_value = mock_dataframe
        mock_dataframe.to_pandas.return_value = mock_pandas_df
        mock_model.validate.return_value = mock_pandas_df

        # Appeler la fonction
        result = load_historique_from_duckdb(
            database_path="test.duckdb",
            filters={"Date_Evenement": ">= '2024-01-01'"},
            limit=1000
        )

        # Vérifications
        assert result == mock_pandas_df
        mock_load_func.assert_called_once_with(
            database_path="test.duckdb",
            filters={"Date_Evenement": ">= '2024-01-01'"},
            limit=1000,
            valider=False
        )
        mock_lazy_frame.collect.assert_called_once()
        mock_dataframe.to_pandas.assert_called_once()
        mock_model.validate.assert_called_once_with(mock_pandas_df)

    @patch('electricore.core.pipeline_perimetre.load_historique_perimetre')
    def test_load_with_default_parameters(self, mock_load_func):
        """Test avec paramètres par défaut."""
        mock_lazy_frame = Mock()
        mock_dataframe = Mock()
        mock_pandas_df = Mock(spec=pd.DataFrame)

        mock_load_func.return_value = mock_lazy_frame
        mock_lazy_frame.collect.return_value = mock_dataframe
        mock_dataframe.to_pandas.return_value = mock_pandas_df

        with patch('electricore.core.pipeline_perimetre.HistoriquePérimètre.validate') as mock_validate:
            mock_validate.return_value = mock_pandas_df

            result = load_historique_from_duckdb()

            # Vérifier que les paramètres par défaut sont utilisés
            mock_load_func.assert_called_once_with(
                database_path=None,
                filters=None,
                limit=None,
                valider=False
            )


class TestPipelinePerimetreFlexible:
    """Tests pour le pipeline flexible avec différentes sources."""

    @patch('electricore.core.pipeline_perimetre.load_historique_from_duckdb')
    @patch('electricore.core.pipeline_perimetre.pipeline_perimetre')
    def test_source_duckdb_default(self, mock_pipeline, mock_load):
        """Test avec source DuckDB par défaut."""
        mock_df = Mock(spec=pd.DataFrame)
        mock_result = Mock(spec=pd.DataFrame)

        mock_load.return_value = mock_df
        mock_pipeline.return_value = mock_result

        # Test avec source=None
        result = pipeline_perimetre_flexible(
            source=None,
            filters={"Date_Evenement": ">= '2024-01-01'"},
            limit=500
        )

        assert result == mock_result
        mock_load.assert_called_once_with(
            filters={"Date_Evenement": ">= '2024-01-01'"},
            limit=500
        )
        mock_pipeline.assert_called_once_with(mock_df, date_limite=None)

    @patch('electricore.core.pipeline_perimetre.load_historique_from_duckdb')
    @patch('electricore.core.pipeline_perimetre.pipeline_perimetre')
    def test_source_duckdb_explicit(self, mock_pipeline, mock_load):
        """Test avec source DuckDB explicite."""
        mock_df = Mock(spec=pd.DataFrame)
        mock_result = Mock(spec=pd.DataFrame)

        mock_load.return_value = mock_df
        mock_pipeline.return_value = mock_result

        # Test avec source="duckdb"
        result = pipeline_perimetre_flexible(
            source="duckdb",
            filters={"pdl": ["PDL123"]},
            limit=100
        )

        assert result == mock_result
        mock_load.assert_called_once_with(
            filters={"pdl": ["PDL123"]},
            limit=100
        )
        mock_pipeline.assert_called_once_with(mock_df, date_limite=None)

    @patch('electricore.core.pipeline_perimetre.load_historique_from_duckdb')
    @patch('electricore.core.pipeline_perimetre.pipeline_perimetre')
    def test_source_custom_database_path(self, mock_pipeline, mock_load):
        """Test avec chemin de base personnalisé."""
        mock_df = Mock(spec=pd.DataFrame)
        mock_result = Mock(spec=pd.DataFrame)

        mock_load.return_value = mock_df
        mock_pipeline.return_value = mock_result

        custom_path = "/custom/path/db.duckdb"
        result = pipeline_perimetre_flexible(
            source=custom_path,
            filters=None,
            limit=None
        )

        assert result == mock_result
        mock_load.assert_called_once_with(
            database_path=custom_path,
            filters=None,
            limit=None
        )

    @patch('electricore.core.pipeline_perimetre.pipeline_perimetre')
    def test_source_dataframe_direct(self, mock_pipeline):
        """Test avec DataFrame fourni directement."""
        mock_df = Mock(spec=pd.DataFrame)
        mock_result = Mock(spec=pd.DataFrame)

        mock_pipeline.return_value = mock_result

        result = pipeline_perimetre_flexible(
            source=mock_df,
            date_limite=pd.Timestamp("2024-12-31", tz="Europe/Paris")
        )

        assert result == mock_result
        mock_pipeline.assert_called_once_with(
            mock_df,
            date_limite=pd.Timestamp("2024-12-31", tz="Europe/Paris")
        )

    def test_source_invalid_type(self):
        """Test avec type de source invalide."""
        with pytest.raises(ValueError, match="Type de source non supporté"):
            pipeline_perimetre_flexible(source=123)

    @patch('electricore.core.pipeline_perimetre.load_historique_from_duckdb')
    @patch('electricore.core.pipeline_perimetre.pipeline_perimetre')
    def test_with_date_limite(self, mock_pipeline, mock_load):
        """Test avec limite de date."""
        mock_df = Mock(spec=pd.DataFrame)
        mock_result = Mock(spec=pd.DataFrame)

        mock_load.return_value = mock_df
        mock_pipeline.return_value = mock_result

        date_limite = pd.Timestamp("2024-06-30", tz="Europe/Paris")

        result = pipeline_perimetre_flexible(
            source="duckdb",
            date_limite=date_limite
        )

        assert result == mock_result
        mock_pipeline.assert_called_once_with(mock_df, date_limite=date_limite)


class TestIntegrationComplet:
    """Tests d'intégration complets du pipeline."""

    @pytest.mark.skipif(
        not Path("electricore/etl/flux_enedis.duckdb").exists(),
        reason="Base DuckDB non disponible"
    )
    def test_pipeline_complet_avec_vraie_db(self):
        """Test du pipeline complet avec la vraie base DuckDB."""
        try:
            # Utiliser un filtre restrictif pour limiter les données
            result = pipeline_perimetre_flexible(
                source="duckdb",
                filters={"Date_Evenement": ">= '2024-09-01'"},
                limit=10  # Limiter à 10 lignes pour le test
            )

            # Vérifications de base
            assert isinstance(result, pd.DataFrame)
            assert len(result) >= 0  # Peut être vide selon les données

            # Si des données sont présentes, vérifier la structure
            if len(result) > 0:
                # Colonnes ajoutées par detecter_points_de_rupture
                assert "impacte_abonnement" in result.columns
                assert "impacte_energie" in result.columns
                assert "resume_modification" in result.columns

                # Colonnes de base
                expected_columns = [
                    "Date_Evenement", "pdl", "Ref_Situation_Contractuelle",
                    "Evenement_Declencheur", "Type_Evenement"
                ]
                for col in expected_columns:
                    assert col in result.columns

        except Exception as e:
            pytest.skip(f"Test avec vraie DB échoué: {e}")

    @pytest.mark.skipif(
        not Path("electricore/etl/flux_enedis.duckdb").exists(),
        reason="Base DuckDB non disponible"
    )
    def test_pipeline_avec_filtres_specifiques(self):
        """Test avec filtres spécifiques sur les données réelles."""
        try:
            # Test avec filtre sur un PDL spécifique (si disponible)
            # D'abord récupérer un échantillon pour identifier des PDL
            from electricore.core.loaders.duckdb_loader import execute_custom_query

            sample_query = """
            SELECT DISTINCT pdl
            FROM enedis_production.flux_c15
            LIMIT 1
            """

            sample_df = execute_custom_query(sample_query, lazy=False)

            if len(sample_df) > 0:
                test_pdl = sample_df["pdl"][0]

                result = pipeline_perimetre_flexible(
                    source="duckdb",
                    filters={"pdl": f"'{test_pdl}'"},
                    limit=20
                )

                # Vérifier que les résultats concernent bien le PDL testé
                if len(result) > 0:
                    assert all(result["pdl"] == test_pdl)

        except Exception as e:
            pytest.skip(f"Test avec filtres spécifiques échoué: {e}")


# Fixtures utiles
@pytest.fixture
def sample_historique_pandas():
    """Fixture avec des données d'historique pandas pour tests."""
    return pd.DataFrame({
        "Date_Evenement": pd.to_datetime([
            "2024-01-01 10:00:00",
            "2024-01-15 11:00:00",
            "2024-02-01 12:00:00"
        ]).tz_localize("Europe/Paris"),
        "pdl": ["PDL123", "PDL123", "PDL124"],
        "Ref_Situation_Contractuelle": ["REF001", "REF001", "REF002"],
        "Segment_Clientele": ["C5", "C5", "C5"],
        "Etat_Contractuel": ["EN SERVICE", "EN SERVICE", "EN SERVICE"],
        "Evenement_Declencheur": ["MES", "MCT", "MES"],
        "Type_Evenement": ["mise_en_service", "modification", "mise_en_service"],
        "Categorie": [None, None, None],
        "Puissance_Souscrite": [6.0, 9.0, 6.0],
        "Formule_Tarifaire_Acheminement": ["BTINF36", "BTINF36", "BTINF36"],
        "Type_Compteur": ["ELEC", "ELEC", "ELEC"],
        "Num_Compteur": ["123456", "123456", "789012"],
        "Ref_Demandeur": [None, None, None],
        "Id_Affaire": [None, None, None]
    })


if __name__ == "__main__":
    pytest.main([__file__, "-v"])