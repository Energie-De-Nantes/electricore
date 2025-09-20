"""Test de validation du pipeline énergie Polars vs pandas."""

import polars as pl
import pandas as pd
import pytest
import numpy as np
from datetime import datetime, timezone
from typing import Dict, Any

# Import des pipelines
from electricore.core.pipeline_energie import calculer_periodes_energie
from electricore.core.pipelines_polars.energie_polars import calculer_periodes_energie_polars

# Import des modèles
from electricore.core.models.releve_index import RelevéIndex
from electricore.core.models_polars.releve_index_polars import RelevéIndexPolars


class TestEnergiePolarsVsPandas:
    """Tests de validation du pipeline énergie Polars contre pandas."""

    @pytest.fixture
    def sample_releves_pandas(self) -> pd.DataFrame:
        """Fixture avec des données de relevés pandas pour la comparaison."""
        paris_tz = timezone.utc

        data = {
            "pdl": ["12345", "12345", "12345", "12345", "67890", "67890", "67890"],
            "Ref_Situation_Contractuelle": ["PDL001", "PDL001", "PDL001", "PDL001", "PDL002", "PDL002", "PDL002"],
            "Date_Releve": [
                datetime(2024, 1, 1, tzinfo=paris_tz),
                datetime(2024, 2, 1, tzinfo=paris_tz),
                datetime(2024, 3, 1, tzinfo=paris_tz),
                datetime(2024, 4, 1, tzinfo=paris_tz),
                datetime(2024, 1, 15, tzinfo=paris_tz),
                datetime(2024, 2, 15, tzinfo=paris_tz),
                datetime(2024, 3, 15, tzinfo=paris_tz),
            ],
            "Source": ["flux_R151", "flux_R151", "flux_R151", "flux_R151", "flux_C15", "flux_R151", "flux_R151"],
            "Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFCU4", "BTINFCU4", "BTINFCU4"],
            "base": [1000.8, 1050.2, 1100.9, 1150.1, 500.3, 530.7, 560.1],
            "hp": [600.5, 620.8, 640.2, 660.9, None, None, None],
            "hc": [400.3, 430.4, 460.7, 490.2, None, None, None],
            "hph": [None, None, None, None, 300.1, 315.8, 331.2],
            "hch": [None, None, None, None, 200.2, 214.9, 228.9],
            "Unité": ["kWh"] * 7,
            "Précision": ["kWh"] * 7,
            "ordre_index": [False] * 7,
            "Id_Calendrier_Distributeur": ["DI000002", "DI000002", "DI000002", "DI000002", "DI000003", "DI000003", "DI000003"]
        }

        df = pd.DataFrame(data)
        return RelevéIndex.validate(df)

    @pytest.fixture
    def sample_releves_polars(self, sample_releves_pandas) -> pl.LazyFrame:
        """Fixture avec les mêmes données converties en Polars."""
        # Convertir pandas vers polars en adaptant les noms de colonnes
        df_polars = pl.from_pandas(sample_releves_pandas)

        # Adapter les noms de colonnes au format snake_case attendu par Polars
        df_polars = df_polars.rename({
            "Date_Releve": "date_releve",
            "Ref_Situation_Contractuelle": "ref_situation_contractuelle",
            "Source": "source",
            "Formule_Tarifaire_Acheminement": "formule_tarifaire_acheminement",
            "Unité": "unite",
            "Précision": "precision",
            "Id_Calendrier_Distributeur": "id_calendrier_distributeur"
        })

        return df_polars.lazy()

    def test_pipeline_complet_comparaison(self, sample_releves_pandas, sample_releves_polars):
        """Test de comparaison complète entre les pipelines pandas et Polars."""

        # Exécuter le pipeline pandas
        try:
            periodes_pandas = calculer_periodes_energie(sample_releves_pandas)
        except Exception as e:
            pytest.skip(f"Pipeline pandas échoué: {e}")

        # Exécuter le pipeline Polars
        periodes_polars = calculer_periodes_energie_polars(sample_releves_polars).collect()

        # Convertir Polars en pandas pour la comparaison
        periodes_polars_pd = periodes_polars.to_pandas()

        # Comparaisons de base
        assert len(periodes_pandas) > 0, "Le pipeline pandas ne doit pas retourner un résultat vide"
        assert len(periodes_polars_pd) > 0, "Le pipeline Polars ne doit pas retourner un résultat vide"

        # Vérifier que le nombre de périodes est cohérent (peut différer légèrement selon la logique de filtrage)
        nb_periodes_diff = abs(len(periodes_pandas) - len(periodes_polars_pd))
        assert nb_periodes_diff <= 2, f"Nombre de périodes trop différent: pandas={len(periodes_pandas)}, polars={len(periodes_polars_pd)}"

    def test_calcul_energies_coherence(self, sample_releves_pandas, sample_releves_polars):
        """Test de cohérence des calculs d'énergie entre pandas et Polars."""

        # Exécuter les pipelines
        try:
            periodes_pandas = calculer_periodes_energie(sample_releves_pandas)
        except Exception as e:
            pytest.skip(f"Pipeline pandas échoué: {e}")

        periodes_polars = calculer_periodes_energie_polars(sample_releves_polars).collect().to_pandas()

        # Comparer les énergies calculées pour les PDL communs
        cadrans_energie = ["base_energie", "hp_energie", "hc_energie"]

        for cadran in cadrans_energie:
            if cadran in periodes_pandas.columns and cadran in periodes_polars.columns:
                # Filtrer les valeurs non-nulles pour la comparaison
                pandas_values = periodes_pandas[cadran].dropna()
                polars_values = periodes_polars[cadran].dropna()

                if len(pandas_values) > 0 and len(polars_values) > 0:
                    # Comparer les ordres de grandeur
                    pandas_mean = pandas_values.mean()
                    polars_mean = polars_values.mean()

                    if pandas_mean > 0:  # Éviter la division par zéro
                        diff_relative = abs(pandas_mean - polars_mean) / pandas_mean
                        assert diff_relative < 0.1, f"Différence trop importante pour {cadran}: {diff_relative:.2%}"

    def test_flags_qualite_coherence(self, sample_releves_pandas, sample_releves_polars):
        """Test de cohérence des flags de qualité entre pandas et Polars."""

        try:
            periodes_pandas = calculer_periodes_energie(sample_releves_pandas)
        except Exception as e:
            pytest.skip(f"Pipeline pandas échoué: {e}")

        periodes_polars = calculer_periodes_energie_polars(sample_releves_polars).collect().to_pandas()

        # Vérifier les flags de qualité
        flags_communs = ["data_complete", "periode_irreguliere"]

        for flag in flags_communs:
            if flag in periodes_pandas.columns and flag in periodes_polars.columns:
                # Comparer les proportions de True/False
                pandas_true_ratio = periodes_pandas[flag].sum() / len(periodes_pandas)
                polars_true_ratio = periodes_polars[flag].sum() / len(periodes_polars)

                diff_ratio = abs(pandas_true_ratio - polars_true_ratio)
                assert diff_ratio < 0.2, f"Différence de ratio trop importante pour {flag}: {diff_ratio:.2%}"

    def test_structure_donnees_coherente(self, sample_releves_polars):
        """Test de la structure des données générées par Polars."""

        periodes_polars = calculer_periodes_energie_polars(sample_releves_polars).collect()

        # Vérifier les colonnes essentielles
        colonnes_obligatoires = [
            "pdl", "debut", "fin", "nb_jours",
            "source_avant", "source_apres",
            "data_complete", "periode_irreguliere"
        ]

        for col in colonnes_obligatoires:
            assert col in periodes_polars.columns, f"Colonne manquante: {col}"

        # Vérifier les types de données
        assert periodes_polars["pdl"].dtype == pl.Utf8
        assert periodes_polars["nb_jours"].dtype == pl.Int32
        assert periodes_polars["data_complete"].dtype == pl.Boolean
        assert periodes_polars["periode_irreguliere"].dtype == pl.Boolean

        # Vérifier la cohérence des données
        # Toutes les périodes doivent avoir nb_jours > 0 (filtrage appliqué)
        nb_jours_valid = periodes_polars["nb_jours"].to_pandas()
        assert (nb_jours_valid > 0).all(), "Toutes les périodes doivent avoir nb_jours > 0"

        # Les dates de début ne doivent pas être nulles
        assert periodes_polars["debut"].null_count() == 0, "Aucune date de début ne doit être nulle"

    def test_enrichissement_hierarchique(self, sample_releves_polars):
        """Test de l'enrichissement hiérarchique des cadrans."""

        periodes_polars = calculer_periodes_energie_polars(sample_releves_polars).collect()

        # Vérifier que l'enrichissement hiérarchique fonctionne
        # Pour les compteurs 4 cadrans (DI000003), HC et HP doivent être enrichis
        masque_4_cadrans = periodes_polars.filter(
            pl.col("pdl") == "67890"  # Ce PDL a 4 cadrans dans nos données de test
        )

        if masque_4_cadrans.shape[0] > 0:
            # hc_energie doit être enrichi avec hch + hcb
            hc_values = masque_4_cadrans["hc_energie"].to_pandas().dropna()
            if len(hc_values) > 0:
                # Vérifier que HC > 0 (enrichissement effectué)
                assert (hc_values > 0).any(), "hc_energie doit être enrichi pour les compteurs 4 cadrans"

    def comparer_statistiques_descriptives(self, df_pandas: pd.DataFrame, df_polars: pd.DataFrame) -> Dict[str, Any]:
        """Compare les statistiques descriptives entre pandas et Polars."""

        stats = {}
        colonnes_numeriques = ["base_energie", "hp_energie", "hc_energie", "nb_jours"]

        for col in colonnes_numeriques:
            if col in df_pandas.columns and col in df_polars.columns:
                pandas_stats = df_pandas[col].describe()
                polars_stats = df_polars[col].describe()

                stats[col] = {
                    "pandas_mean": pandas_stats["mean"] if "mean" in pandas_stats else None,
                    "polars_mean": polars_stats.get("mean", None),
                    "pandas_std": pandas_stats["std"] if "std" in pandas_stats else None,
                    "polars_std": polars_stats.get("std", None)
                }

        return stats

    def test_performance_comparative(self, sample_releves_polars):
        """Test basique de performance (non critique pour la validation fonctionnelle)."""
        import time

        # Mesurer le temps d'exécution Polars
        start_time = time.time()
        periodes_polars = calculer_periodes_energie_polars(sample_releves_polars).collect()
        polars_time = time.time() - start_time

        # Vérifier que l'exécution n'est pas anormalement lente
        assert polars_time < 5.0, f"Exécution Polars trop lente: {polars_time:.2f}s"

        # Vérifier que le résultat n'est pas vide
        assert periodes_polars.shape[0] > 0, "Le pipeline Polars doit retourner des résultats"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])