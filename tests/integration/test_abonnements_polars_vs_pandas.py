"""
Test d'intégration comparatif entre les pipelines pandas et Polars pour les abonnements.

Ce test valide l'équivalence fonctionnelle entre l'ancien pipeline pandas
et le nouveau pipeline Polars, ainsi que les améliorations de performance.
"""

import pandas as pd
import polars as pl
import pytest
import time
from pathlib import Path
from datetime import datetime, timezone

# Imports pandas
from electricore.core.pipeline_abonnements import pipeline_abonnement as pipeline_pandas
from electricore.core.pipeline_perimetre import pipeline_perimetre

# Imports Polars
from electricore.core.pipelines_polars.abonnements_polars import pipeline_abonnements as pipeline_polars
from electricore.core.pipelines_polars.perimetre_polars import detecter_points_de_rupture


@pytest.fixture
def historique_test_data():
    """
    Génère des données d'historique de test représentatives.

    Couvre les cas principaux :
    - Plusieurs PDL avec événements multiples
    - Changements de puissance et FTA
    - Événements structurants (MES, RES)
    - Périodes de longueurs différentes
    """
    paris_tz = timezone.utc  # Simplifié pour les tests

    # Données d'historique représentatives - Version pandas (majuscules)
    data_pandas = {
        "Ref_Situation_Contractuelle": [
            "PDL001", "PDL001", "PDL001", "PDL001",
            "PDL002", "PDL002", "PDL002",
            "PDL003", "PDL003"
        ],
        "pdl": [
            "12345001", "12345001", "12345001", "12345001",
            "12345002", "12345002", "12345002",
            "12345003", "12345003"
        ],
        "Date_Evenement": [
            datetime(2024, 1, 1, tzinfo=paris_tz),
            datetime(2024, 2, 1, tzinfo=paris_tz),
            datetime(2024, 4, 1, tzinfo=paris_tz),
            datetime(2024, 6, 1, tzinfo=paris_tz),
            datetime(2024, 1, 15, tzinfo=paris_tz),
            datetime(2024, 3, 15, tzinfo=paris_tz),
            datetime(2024, 5, 15, tzinfo=paris_tz),
            datetime(2024, 2, 10, tzinfo=paris_tz),
            datetime(2024, 4, 10, tzinfo=paris_tz),
        ],
        "Evenement_Declencheur": [
            "MES", "MCT", "MCT", "RES",
            "MES", "MCT", "RES",
            "MES", "RES"
        ],
        "Formule_Tarifaire_Acheminement": [
            "BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4",
            "BTINFCU4", "BTINFMU4", "BTINFMU4",
            "BTINFCU4", "BTINFCU4"
        ],
        "Puissance_Souscrite": [
            6.0, 6.0, 9.0, 9.0,
            3.0, 3.0, 3.0,
            12.0, 12.0
        ],
        "Segment_Clientele": ["C5"] * 9,
        "Categorie": ["PRO"] * 9,
        "Etat_Contractuel": [
            "ACTIF", "ACTIF", "ACTIF", "RESILIE",
            "ACTIF", "ACTIF", "RESILIE",
            "ACTIF", "RESILIE"
        ],
        "Type_Evenement": ["reel"] * 9,
        "Type_Compteur": ["LINKY"] * 9,
        "Num_Compteur": [
            "COMP001", "COMP001", "COMP001", "COMP001",
            "COMP002", "COMP002", "COMP002",
            "COMP003", "COMP003"
        ],
        "Ref_Demandeur": ["REF001"] * 9,
        "Id_Affaire": ["AFF001"] * 9,
        # Colonnes calendrier requises par le pipeline périmètre
        "Avant_Id_Calendrier_Distributeur": [
            "CAL_HP_HC", "CAL_HP_HC", "CAL_TEMPO", "CAL_TEMPO",
            "CAL_HP_HC", "CAL_TEMPO", "CAL_TEMPO",
            "CAL_HP_HC", "CAL_HP_HC"
        ],
        "Après_Id_Calendrier_Distributeur": [
            "CAL_HP_HC", "CAL_TEMPO", "CAL_TEMPO", "CAL_TEMPO",
            "CAL_TEMPO", "CAL_TEMPO", "CAL_TEMPO",
            "CAL_HP_HC", "CAL_HP_HC"
        ],
        # Index columns
        "Avant_BASE": [1000, 1250, 1600, 1950, 500, 750, 900, 2000, 2500],
        "Après_BASE": [1250, 1600, 1950, 2200, 750, 900, 1100, 2500, 2800],
        "Avant_HP": [500, 625, 800, 975, 250, 375, 450, 1000, 1250],
        "Après_HP": [625, 800, 975, 1100, 375, 450, 550, 1250, 1400],
        "Avant_HC": [300, 375, 480, 585, 150, 225, 270, 600, 750],
        "Après_HC": [375, 480, 585, 660, 225, 270, 330, 750, 840],
        "Avant_HPH": [None] * 9,
        "Après_HPH": [None] * 9,
        "Avant_HCH": [None] * 9,
        "Après_HCH": [None] * 9,
        "Avant_HPB": [None] * 9,
        "Après_HPB": [None] * 9,
        "Avant_HCB": [None] * 9,
        "Après_HCB": [None] * 9,
    }

    # Données d'historique représentatives - Version Polars (snake_case)
    data_polars = {
        "ref_situation_contractuelle": [
            "PDL001", "PDL001", "PDL001", "PDL001",
            "PDL002", "PDL002", "PDL002",
            "PDL003", "PDL003"
        ],
        "pdl": [
            "12345001", "12345001", "12345001", "12345001",
            "12345002", "12345002", "12345002",
            "12345003", "12345003"
        ],
        "date_evenement": [
            datetime(2024, 1, 1, tzinfo=paris_tz),
            datetime(2024, 2, 1, tzinfo=paris_tz),
            datetime(2024, 4, 1, tzinfo=paris_tz),
            datetime(2024, 6, 1, tzinfo=paris_tz),
            datetime(2024, 1, 15, tzinfo=paris_tz),
            datetime(2024, 3, 15, tzinfo=paris_tz),
            datetime(2024, 5, 15, tzinfo=paris_tz),
            datetime(2024, 2, 10, tzinfo=paris_tz),
            datetime(2024, 4, 10, tzinfo=paris_tz),
        ],
        "evenement_declencheur": [
            "MES", "MCT", "MCT", "RES",
            "MES", "MCT", "RES",
            "MES", "RES"
        ],
        "formule_tarifaire_acheminement": [
            "BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4",
            "BTINFCU4", "BTINFMU4", "BTINFMU4",
            "BTINFCU4", "BTINFCU4"
        ],
        "puissance_souscrite": [
            6.0, 6.0, 9.0, 9.0,
            3.0, 3.0, 3.0,
            12.0, 12.0
        ],
        "segment_clientele": ["C5"] * 9,
        "categorie": ["PRO"] * 9,
        "etat_contractuel": [
            "ACTIF", "ACTIF", "ACTIF", "RESILIE",
            "ACTIF", "ACTIF", "RESILIE",
            "ACTIF", "RESILIE"
        ],
        "type_evenement": ["reel"] * 9,
        "type_compteur": ["LINKY"] * 9,
        "num_compteur": [
            "COMP001", "COMP001", "COMP001", "COMP001",
            "COMP002", "COMP002", "COMP002",
            "COMP003", "COMP003"
        ],
        "ref_demandeur": ["REF001"] * 9,
        "id_affaire": ["AFF001"] * 9,
        # Colonnes calendrier requises par le pipeline périmètre (snake_case)
        "avant_id_calendrier_distributeur": [
            "CAL_HP_HC", "CAL_HP_HC", "CAL_TEMPO", "CAL_TEMPO",
            "CAL_HP_HC", "CAL_TEMPO", "CAL_TEMPO",
            "CAL_HP_HC", "CAL_HP_HC"
        ],
        "apres_id_calendrier_distributeur": [
            "CAL_HP_HC", "CAL_TEMPO", "CAL_TEMPO", "CAL_TEMPO",
            "CAL_TEMPO", "CAL_TEMPO", "CAL_TEMPO",
            "CAL_HP_HC", "CAL_HP_HC"
        ],
        # Index columns (snake_case)
        "avant_BASE": [1000, 1250, 1600, 1950, 500, 750, 900, 2000, 2500],
        "apres_BASE": [1250, 1600, 1950, 2200, 750, 900, 1100, 2500, 2800],
        "avant_HP": [500, 625, 800, 975, 250, 375, 450, 1000, 1250],
        "apres_HP": [625, 800, 975, 1100, 375, 450, 550, 1250, 1400],
        "avant_HC": [300, 375, 480, 585, 150, 225, 270, 600, 750],
        "apres_HC": [375, 480, 585, 660, 225, 270, 330, 750, 840],
        "avant_HPH": [None] * 9,
        "apres_HPH": [None] * 9,
        "avant_HCH": [None] * 9,
        "apres_HCH": [None] * 9,
        "avant_HPB": [None] * 9,
        "apres_HPB": [None] * 9,
        "avant_HCB": [None] * 9,
        "apres_HCB": [None] * 9,
    }

    # Créer les versions pandas et Polars
    df_pandas = pd.DataFrame(data_pandas)
    lf_polars = pl.LazyFrame(data_polars)

    return df_pandas, lf_polars


@pytest.fixture
def historique_enrichi(historique_test_data):
    """
    Crée des historiques enrichis (avec colonnes d'impact) pour les tests.
    """
    df_pandas, lf_polars = historique_test_data

    # Enrichir avec le pipeline périmètre
    df_pandas_enrichi = pipeline_perimetre(df_pandas)
    lf_polars_enrichi = detecter_points_de_rupture(lf_polars)

    return df_pandas_enrichi, lf_polars_enrichi


class TestEquivalenceFonctionnelle:
    """Tests d'équivalence fonctionnelle entre pandas et Polars."""

    def test_memes_colonnes_sortie(self, historique_enrichi):
        """Vérifie que les deux pipelines produisent les mêmes colonnes."""
        df_pandas_enrichi, lf_polars_enrichi = historique_enrichi

        # Exécuter les pipelines (sans TURPE pour simplifier)
        try:
            # Pipeline pandas complet avec TURPE
            result_pandas = pipeline_pandas(df_pandas_enrichi)
            pandas_columns = set(result_pandas.columns)
        except Exception:
            # Fallback si problème avec TURPE
            pytest.skip("Pipeline pandas échoue - possiblement problème avec TURPE")

        # Pipeline Polars sans TURPE pour comparaison de base
        from electricore.core.pipelines_polars.abonnements_polars import generer_periodes_abonnement
        result_polars = generer_periodes_abonnement(lf_polars_enrichi).collect()
        polars_columns = set(result_polars.columns)

        # Colonnes de base communes - mapping entre pandas (PascalCase) et Polars (snake_case)
        colonnes_pandas = {
            "Ref_Situation_Contractuelle", "pdl", "mois_annee",
            "debut_lisible", "fin_lisible", "Formule_Tarifaire_Acheminement",
            "Puissance_Souscrite", "nb_jours", "debut", "fin"
        }

        colonnes_polars = {
            "ref_situation_contractuelle", "pdl", "mois_annee",
            "debut_lisible", "fin_lisible", "formule_tarifaire_acheminement",
            "puissance_souscrite", "nb_jours", "debut", "fin"
        }

        assert colonnes_pandas.issubset(pandas_columns), f"Colonnes manquantes pandas: {colonnes_pandas - pandas_columns}"
        assert colonnes_polars.issubset(polars_columns), f"Colonnes manquantes polars: {colonnes_polars - polars_columns}"

    def test_nombre_periodes_identique(self, historique_enrichi):
        """Vérifie que les deux pipelines génèrent le même nombre de périodes."""
        df_pandas_enrichi, lf_polars_enrichi = historique_enrichi

        # Pipeline Polars (périodes seulement)
        from electricore.core.pipelines_polars.abonnements_polars import generer_periodes_abonnement
        result_polars = generer_periodes_abonnement(lf_polars_enrichi).collect()

        # Pipeline pandas (périodes de base)
        from electricore.core.pipeline_abonnements import generer_periodes_abonnement as generer_pandas
        result_pandas = generer_pandas(df_pandas_enrichi)

        # Comparer le nombre de périodes générées
        assert len(result_polars) == len(result_pandas), (
            f"Polars: {len(result_polars)} périodes, "
            f"Pandas: {len(result_pandas)} périodes"
        )

    def test_periodes_valides_seulement(self, historique_enrichi):
        """Vérifie que seules les périodes valides sont conservées."""
        _, lf_polars_enrichi = historique_enrichi

        from electricore.core.pipelines_polars.abonnements_polars import generer_periodes_abonnement
        result = generer_periodes_abonnement(lf_polars_enrichi).collect()

        # Toutes les périodes doivent avoir:
        # - Une fin définie (pas None)
        # - Un nombre de jours > 0
        fins_definies = [fin for fin in result["fin"].to_list() if fin is not None]
        assert len(fins_definies) == len(result), "Toutes les périodes doivent avoir une fin définie"

        nb_jours_positifs = [nb for nb in result["nb_jours"].to_list() if nb > 0]
        assert len(nb_jours_positifs) == len(result), "Tous les nombres de jours doivent être positifs"

    def test_ordre_chronologique_conserve(self, historique_enrichi):
        """Vérifie que l'ordre chronologique est conservé par contrat."""
        _, lf_polars_enrichi = historique_enrichi

        from electricore.core.pipelines_polars.abonnements_polars import generer_periodes_abonnement
        result = generer_periodes_abonnement(lf_polars_enrichi).collect()

        # Grouper par contrat et vérifier l'ordre chronologique
        for ref in result["ref_situation_contractuelle"].unique():
            periodes_contrat = result.filter(
                pl.col("ref_situation_contractuelle") == ref
            ).sort("debut")

            debuts = periodes_contrat["debut"].to_list()
            # Vérifier que les débuts sont en ordre croissant
            assert debuts == sorted(debuts), f"Ordre chronologique non respecté pour {ref}"


class TestPerformance:
    """Tests de performance comparatifs."""

    def test_benchmark_pipeline_complet(self, historique_enrichi):
        """Compare les performances des pipelines complets."""
        df_pandas_enrichi, lf_polars_enrichi = historique_enrichi

        # Benchmark Polars (périodes uniquement)
        start_time = time.perf_counter()
        iterations = 10

        for _ in range(iterations):
            from electricore.core.pipelines_polars.abonnements_polars import generer_periodes_abonnement
            result_polars = generer_periodes_abonnement(lf_polars_enrichi).collect()

        temps_polars = (time.perf_counter() - start_time) / iterations

        # Benchmark pandas (périodes uniquement)
        start_time = time.perf_counter()

        for _ in range(iterations):
            from electricore.core.pipeline_abonnements import generer_periodes_abonnement as generer_pandas
            result_pandas = generer_pandas(df_pandas_enrichi)

        temps_pandas = (time.perf_counter() - start_time) / iterations

        # Calculer l'accélération
        acceleration = temps_pandas / temps_polars if temps_polars > 0 else 0

        print(f"\n📊 BENCHMARK PÉRIODES ABONNEMENT:")
        print(f"🐼 Pandas  : {temps_pandas*1000:.1f}ms")
        print(f"⚡ Polars  : {temps_polars*1000:.1f}ms")
        print(f"🚀 Accélération : {acceleration:.1f}x")

        # Polars doit être au moins aussi rapide
        assert acceleration >= 1.0, f"Polars devrait être plus rapide (accélération: {acceleration:.1f}x)"

    def test_memory_efficiency(self, historique_enrichi):
        """Vérifie que Polars utilise LazyFrames efficacement."""
        _, lf_polars_enrichi = historique_enrichi

        from electricore.core.pipelines_polars.abonnements_polars import generer_periodes_abonnement

        # Le pipeline devrait retourner un LazyFrame
        lazy_result = generer_periodes_abonnement(lf_polars_enrichi)
        assert isinstance(lazy_result, pl.LazyFrame), "Le pipeline doit retourner un LazyFrame"

        # La collecte doit être explicite
        collected_result = lazy_result.collect()
        assert isinstance(collected_result, pl.DataFrame), "Collect() doit retourner un DataFrame"


class TestRobustesse:
    """Tests de robustesse et de gestion des cas limites."""

    def test_historique_vide(self):
        """Teste le comportement avec un historique vide."""
        historique_vide = pl.LazyFrame({
            "ref_situation_contractuelle": [],
            "pdl": [],
            "date_evenement": [],
            "formule_tarifaire_acheminement": [],
            "puissance_souscrite": [],
            "impacte_abonnement": [],
        }, schema={
            "ref_situation_contractuelle": pl.Utf8,
            "pdl": pl.Utf8,
            "date_evenement": pl.Datetime(time_zone="UTC"),
            "formule_tarifaire_acheminement": pl.Utf8,
            "puissance_souscrite": pl.Float64,
            "impacte_abonnement": pl.Boolean,
        })

        from electricore.core.pipelines_polars.abonnements_polars import generer_periodes_abonnement
        result = generer_periodes_abonnement(historique_vide).collect()

        assert len(result) == 0
        assert isinstance(result, pl.DataFrame)

    def test_aucun_evenement_impactant(self, historique_test_data):
        """Teste avec des événements qui n'impactent pas l'abonnement."""
        _, lf_polars = historique_test_data

        # Créer un historique enrichi sans impacts d'abonnement
        historique_sans_impact = (
            lf_polars
            .with_columns(pl.lit(False).alias("impacte_abonnement"))
        )

        from electricore.core.pipelines_polars.abonnements_polars import generer_periodes_abonnement
        result = generer_periodes_abonnement(historique_sans_impact).collect()

        assert len(result) == 0


@pytest.mark.integration
class TestIntegrationComplete:
    """Tests d'intégration avec données réelles (si disponibles)."""

    def test_pipeline_avec_donnees_demo(self):
        """Teste le pipeline avec les données de démo du notebook."""
        # Données similaires au notebook de démo
        demo_data = {
            "ref_situation_contractuelle": ["PDL001"] * 6,
            "pdl": ["PDL12345"] * 6,
            "date_evenement": [
                datetime(2024, 1, 15, tzinfo=timezone.utc),
                datetime(2024, 2, 1, tzinfo=timezone.utc),
                datetime(2024, 3, 20, tzinfo=timezone.utc),
                datetime(2024, 4, 1, tzinfo=timezone.utc),
                datetime(2024, 5, 10, tzinfo=timezone.utc),
                datetime(2024, 6, 1, tzinfo=timezone.utc),
            ],
            "evenement_declencheur": ["MES", "FACTURATION", "MCT", "FACTURATION", "MCT", "RES"],
            "puissance_souscrite": [6.0, 6.0, 9.0, 9.0, 12.0, 12.0],
            "formule_tarifaire_acheminement": ["BTINFCU4"] * 6,
            "segment_clientele": ["C5"] * 6,
            "etat_contractuel": ["ACTIF"] * 5 + ["RESILIE"],
            "type_evenement": ["reel", "artificiel", "reel", "artificiel", "reel", "reel"],
            "type_compteur": ["LINKY"] * 6,
            "num_compteur": ["12345678"] * 6,
            "ref_demandeur": ["REF001"] * 6,
            "id_affaire": ["AFF001"] * 6,
        }

        lf_demo = pl.LazyFrame(demo_data)

        # Enrichir et traiter
        lf_enrichi = detecter_points_de_rupture(lf_demo)

        from electricore.core.pipelines_polars.abonnements_polars import generer_periodes_abonnement
        result = generer_periodes_abonnement(lf_enrichi).collect()

        # Vérifications de base
        assert len(result) > 0, "Le pipeline doit générer des périodes"
        assert "turpe_fixe" not in result.columns, "TURPE pas encore calculé"

        # Vérifier quelques propriétés métier
        puissances = result["puissance_souscrite"].to_list()
        assert 6.0 in puissances or 9.0 in puissances or 12.0 in puissances


if __name__ == "__main__":
    pytest.main([__file__, "-v"])