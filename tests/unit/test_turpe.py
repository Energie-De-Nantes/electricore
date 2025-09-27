"""
Tests unitaires pour le pipeline TURPE Polars.

Ce module teste toutes les expressions et fonctions du pipeline TURPE
en utilisant des données contrôlées pour valider les calculs.
"""

import pytest
import polars as pl
from datetime import datetime
from pathlib import Path

from electricore.core.pipelines.turpe import (
    # Chargement des règles
    load_turpe_rules,

    # Expressions TURPE fixe
    expr_calculer_turpe_fixe_annuel,
    expr_calculer_turpe_fixe_journalier,
    expr_calculer_turpe_fixe_periode,

    # Expressions TURPE variable
    expr_calculer_turpe_cadran,
    expr_calculer_turpe_contributions_cadrans,
    expr_sommer_turpe_cadrans,

    # Expressions communes
    expr_filtrer_regles_temporelles,
    valider_regles_presentes,

    # Fonctions pipeline
    ajouter_turpe_fixe,
    ajouter_turpe_variable,

    # Fonctions de debug
    debug_turpe_variable,
    comparer_avec_pandas,
)


class TestChargementRegles:
    """Tests pour le chargement des règles TURPE."""

    def test_load_turpe_rules_basic(self):
        """Test basique du chargement des règles."""
        regles = load_turpe_rules()
        df = regles.collect()

        # Vérifications de base
        assert df.shape[0] > 0, "Aucune règle chargée"
        assert df.shape[1] == 13, f"Nombre de colonnes incorrect: {df.shape[1]}"

        # Vérification des colonnes obligatoires
        colonnes_attendues = [
            "Formule_Tarifaire_Acheminement", "start", "end",
            "cg", "cc", "b", "hph", "hch", "hpb", "hcb", "hp", "hc", "base"
        ]
        for col in colonnes_attendues:
            assert col in df.columns, f"Colonne manquante: {col}"

    def test_load_turpe_rules_types(self):
        """Test des types de colonnes après chargement."""
        regles = load_turpe_rules()
        schema = regles.collect_schema()

        # Types attendus
        assert schema["Formule_Tarifaire_Acheminement"] == pl.String
        assert "Europe/Paris" in str(schema["start"])
        assert "Europe/Paris" in str(schema["end"])

        # Toutes les colonnes numériques doivent être Float64
        colonnes_numeriques = ["cg", "cc", "b", "hph", "hch", "hpb", "hcb", "hp", "hc", "base"]
        for col in colonnes_numeriques:
            assert schema[col] == pl.Float64, f"Type incorrect pour {col}: {schema[col]}"

    def test_load_turpe_rules_valeurs_coherentes(self):
        """Test de cohérence des valeurs chargées."""
        regles = load_turpe_rules()
        df = regles.collect()

        # Toutes les FTA doivent être non-nulles et non-vides
        assert df["Formule_Tarifaire_Acheminement"].null_count() == 0
        assert not df.filter(pl.col("Formule_Tarifaire_Acheminement") == "").shape[0] > 0

        # Les dates de début doivent être valides
        assert df["start"].null_count() == 0

        # Les composantes fixes doivent être positives ou nulles
        for col in ["cg", "cc", "b"]:
            assert df[col].min() >= 0, f"Valeur négative trouvée dans {col}"


class TestExpressionsTurpeFixe:
    """Tests pour les expressions de calcul TURPE fixe."""

    @pytest.fixture
    def df_test_fixe(self):
        """DataFrame de test pour le TURPE fixe."""
        return pl.DataFrame({
            "formule_tarifaire_acheminement": ["BTINFCUST", "BTINFCU4"],
            "puissance_souscrite": [6.0, 9.0],
            "nb_jours": [30, 31],
            "debut": [
                datetime(2024, 1, 1),
                datetime(2024, 2, 1)
            ],
            # Valeurs règles TURPE (simulées)
            "b": [10.44, 9.36],  # €/kW/an
            "cg": [16.2, 16.2],  # €/an
            "cc": [20.88, 20.88] # €/an
        }).with_columns(
            pl.col("debut").dt.replace_time_zone("Europe/Paris")
        )

    def test_expr_calculer_turpe_fixe_annuel(self, df_test_fixe):
        """Test du calcul TURPE fixe annuel."""
        df_result = df_test_fixe.with_columns(
            expr_calculer_turpe_fixe_annuel().alias("turpe_annuel")
        )

        # Calcul attendu: (b * puissance) + cg + cc
        # Ligne 1: (10.44 * 6.0) + 16.2 + 20.88 = 62.64 + 37.08 = 99.72
        # Ligne 2: (9.36 * 9.0) + 16.2 + 20.88 = 84.24 + 37.08 = 121.32

        attendu = [99.72, 121.32]
        resultats = df_result["turpe_annuel"].to_list()

        for i, (res, att) in enumerate(zip(resultats, attendu)):
            assert abs(res - att) < 0.01, f"Ligne {i}: attendu {att}, obtenu {res}"

    def test_expr_calculer_turpe_fixe_journalier(self, df_test_fixe):
        """Test du calcul TURPE fixe journalier."""
        df_result = df_test_fixe.with_columns(
            expr_calculer_turpe_fixe_journalier().alias("turpe_journalier")
        )

        # Calcul attendu: turpe_annuel / 365
        # Ligne 1: 99.72 / 365 = 0.273260
        # Ligne 2: 121.32 / 365 = 0.332384

        attendu = [99.72 / 365, 121.32 / 365]
        resultats = df_result["turpe_journalier"].to_list()

        for i, (res, att) in enumerate(zip(resultats, attendu)):
            assert abs(res - att) < 0.000001, f"Ligne {i}: attendu {att}, obtenu {res}"

    def test_expr_calculer_turpe_fixe_periode(self, df_test_fixe):
        """Test du calcul TURPE fixe pour une période."""
        df_result = df_test_fixe.with_columns(
            expr_calculer_turpe_fixe_periode().alias("turpe_periode")
        )

        # Calcul attendu: (turpe_annuel / 365) * nb_jours, arrondi à 2 décimales
        # Ligne 1: (99.72 / 365) * 30 = 8.20 (arrondi)
        # Ligne 2: (121.32 / 365) * 31 = 10.30 (arrondi)

        attendu = [8.20, 10.30]
        resultats = df_result["turpe_periode"].to_list()

        for i, (res, att) in enumerate(zip(resultats, attendu)):
            assert abs(res - att) < 0.01, f"Ligne {i}: attendu {att}, obtenu {res}"


class TestExpressionsTurpeVariable:
    """Tests pour les expressions de calcul TURPE variable."""

    @pytest.fixture
    def df_test_variable(self):
        """DataFrame de test pour le TURPE variable."""
        return pl.DataFrame({
            "formule_tarifaire_acheminement": ["BTINFCUST", "BTINFMU4"],
            "debut": [
                datetime(2024, 1, 1),
                datetime(2024, 2, 1)
            ],
            # Énergies par cadran (kWh)
            "base_energie": [100.0, 0.0],
            "hp_energie": [0.0, 50.0],
            "hc_energie": [0.0, 30.0],
            "hph_energie": [0.0, 40.0],
            "hch_energie": [0.0, 25.0],
            "hpb_energie": [0.0, 0.0],
            "hcb_energie": [0.0, 0.0],
            # Tarifs TURPE (€/MWh)
            "taux_turpe_base": [4.37, 0.0],
            "taux_turpe_hp": [0.0, 4.68],
            "taux_turpe_hc": [0.0, 3.31],
            "taux_turpe_hph": [0.0, 6.39],
            "taux_turpe_hch": [0.0, 4.43],
            "taux_turpe_hpb": [0.0, 0.0],
            "taux_turpe_hcb": [0.0, 0.0],
        }).with_columns(
            pl.col("debut").dt.replace_time_zone("Europe/Paris")
        )

    def test_expr_calculer_turpe_cadran_base(self, df_test_variable):
        """Test du calcul TURPE pour le cadran base."""
        df_result = df_test_variable.with_columns(
            expr_calculer_turpe_cadran("base").alias("turpe_base")
        )

        # Calcul attendu: energie * tarif / 100
        # Ligne 1: 100.0 * 4.37 / 100 = 4.37
        # Ligne 2: 0.0 * 0.0 / 100 = 0.0

        attendu = [4.37, 0.0]
        resultats = df_result["turpe_base"].to_list()

        for i, (res, att) in enumerate(zip(resultats, attendu)):
            assert abs(res - att) < 0.01, f"Ligne {i}: attendu {att}, obtenu {res}"

    def test_expr_calculer_turpe_cadran_hp(self, df_test_variable):
        """Test du calcul TURPE pour le cadran HP."""
        df_result = df_test_variable.with_columns(
            expr_calculer_turpe_cadran("hp").alias("turpe_hp")
        )

        # Calcul attendu: energie * tarif / 100
        # Ligne 1: 0.0 * 0.0 / 100 = 0.0
        # Ligne 2: 50.0 * 4.68 / 100 = 2.34

        attendu = [0.0, 2.34]
        resultats = df_result["turpe_hp"].to_list()

        for i, (res, att) in enumerate(zip(resultats, attendu)):
            assert abs(res - att) < 0.01, f"Ligne {i}: attendu {att}, obtenu {res}"

    def test_expr_calculer_turpe_contributions_cadrans(self, df_test_variable):
        """Test du calcul des contributions de tous les cadrans."""
        df_result = df_test_variable.with_columns(
            expr_calculer_turpe_contributions_cadrans()
        )

        # Vérifier que toutes les colonnes turpe_* ont été créées
        cadrans = ["hph", "hch", "hpb", "hcb", "hp", "hc", "base"]
        for cadran in cadrans:
            col_name = f"turpe_{cadran}"
            assert col_name in df_result.columns, f"Colonne manquante: {col_name}"

        # Vérifier quelques valeurs
        assert abs(df_result["turpe_base"][0] - 4.37) < 0.01
        assert abs(df_result["turpe_hp"][1] - 2.34) < 0.01
        assert abs(df_result["turpe_hc"][1] - 0.993) < 0.01  # 30 * 3.31 / 100

    def test_expr_sommer_turpe_cadrans(self, df_test_variable):
        """Test de la somme des contributions TURPE."""
        df_result = (df_test_variable
                    .with_columns(expr_calculer_turpe_contributions_cadrans())
                    .with_columns(expr_sommer_turpe_cadrans().alias("turpe_total"))
        )

        # Ligne 1: Seulement BASE = 4.37
        # Ligne 2: HP + HC + HPH + HCH = 2.34 + 0.993 + 2.556 + 1.1075 = 6.9965 ≈ 7.00 (arrondi)

        attendu = [4.37, 7.00]
        resultats = df_result["turpe_total"].to_list()

        for i, (res, att) in enumerate(zip(resultats, attendu)):
            assert abs(res - att) < 0.02, f"Ligne {i}: attendu {att}, obtenu {res}"


class TestExpressionsFiltrage:
    """Tests pour les expressions de filtrage et validation."""

    @pytest.fixture
    def df_test_filtrage(self):
        """DataFrame de test pour le filtrage."""
        return pl.DataFrame({
            "formule_tarifaire_acheminement": ["BTINFCUST", "BTINFMU4", "BTINFCU4"],
            "debut": [
                datetime(2024, 1, 15),  # Dans la période
                datetime(2023, 6, 1),   # Avant la période
                datetime(2025, 1, 1),   # Après la période
            ],
            "start": [
                datetime(2024, 1, 1),
                datetime(2024, 1, 1),
                datetime(2024, 1, 1),
            ],
            "end": [
                datetime(2024, 12, 31),
                datetime(2024, 6, 1),    # Période courte
                None,                    # Pas de fin
            ]
        }).with_columns([
            pl.col("debut").dt.replace_time_zone("Europe/Paris"),
            pl.col("start").dt.replace_time_zone("Europe/Paris"),
            pl.col("end").dt.replace_time_zone("Europe/Paris")
        ])

    def test_expr_filtrer_regles_temporelles(self, df_test_filtrage):
        """Test du filtrage temporel des règles."""
        df_result = df_test_filtrage.filter(expr_filtrer_regles_temporelles())

        # Seules les lignes 0 et 2 devraient passer le filtre
        # Ligne 0: 2024-01-15 >= 2024-01-01 and < 2024-12-31 ✓
        # Ligne 1: 2023-06-01 >= 2024-01-01 ✗ (avant)
        # Ligne 2: 2025-01-01 >= 2024-01-01 and < 2100-01-01 ✓ (end=null remplacé)

        assert df_result.shape[0] == 2, f"Nombre de lignes incorrect: {df_result.shape[0]}"
        ftas_resultats = df_result["formule_tarifaire_acheminement"].to_list()
        assert "BTINFCUST" in ftas_resultats
        assert "BTINFCU4" in ftas_resultats
        assert "BTINFMU4" not in ftas_resultats

    def test_valider_regles_presentes_ok(self):
        """Test de validation avec toutes les règles présentes."""
        df_test = pl.LazyFrame({
            "formule_tarifaire_acheminement": ["BTINFCUST", "BTINFMU4"],
            "start": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
        }).with_columns(
            pl.col("start").dt.replace_time_zone("Europe/Paris")
        )

        # Ne doit pas lever d'exception
        df_result = valider_regles_presentes(df_test).collect()
        assert df_result.shape[0] == 2

    def test_valider_regles_presentes_erreur(self):
        """Test de validation avec des règles manquantes."""
        df_test = pl.LazyFrame({
            "formule_tarifaire_acheminement": ["BTINFCUST", "FTA_INEXISTANTE"],
            "start": [datetime(2024, 1, 1), None],  # start=null pour FTA manquante
        })

        # Doit lever une ValueError
        with pytest.raises(ValueError, match="Règles TURPE manquantes"):
            valider_regles_presentes(df_test).collect()


class TestPipelinesIntegration:
    """Tests d'intégration pour les pipelines complets."""

    @pytest.fixture
    def periodes_abonnement_test(self):
        """Périodes d'abonnement de test."""
        return pl.LazyFrame({
            "formule_tarifaire_acheminement": ["BTINFCUST", "BTINFMU4"],
            "puissance_souscrite": [6.0, 9.0],
            "nb_jours": [30, 31],
            "debut": [datetime(2024, 1, 1), datetime(2024, 2, 1)],
            "pdl": ["PDL001", "PDL002"],
        }).with_columns(
            pl.col("debut").dt.replace_time_zone("Europe/Paris")
        )

    @pytest.fixture
    def periodes_energie_test(self):
        """Périodes d'énergie de test."""
        return pl.LazyFrame({
            "formule_tarifaire_acheminement": ["BTINFCUST", "BTINFMU4"],
            "debut": [datetime(2024, 1, 1), datetime(2024, 2, 1)],
            "pdl": ["PDL001", "PDL002"],
            "base_energie": [100.0, 0.0],
            "hp_energie": [0.0, 50.0],
            "hc_energie": [0.0, 30.0],
            "hph_energie": [0.0, 40.0],
            "hch_energie": [0.0, 25.0],
            "hpb_energie": [0.0, 0.0],
            "hcb_energie": [0.0, 0.0],
        }).with_columns(
            pl.col("debut").dt.replace_time_zone("Europe/Paris")
        )

    def test_ajouter_turpe_fixe_integration(self, periodes_abonnement_test):
        """Test d'intégration du pipeline TURPE fixe."""
        resultat = ajouter_turpe_fixe(periodes_abonnement_test).collect()

        # Vérifications de base
        assert "turpe_fixe" in resultat.columns
        assert resultat.shape[0] == 2
        assert resultat["turpe_fixe"].null_count() == 0

        # Toutes les valeurs doivent être positives
        assert resultat["turpe_fixe"].min() > 0

        # Les colonnes originales doivent être préservées
        colonnes_originales = ["formule_tarifaire_acheminement", "puissance_souscrite", "nb_jours", "debut", "pdl"]
        for col in colonnes_originales:
            assert col in resultat.columns, f"Colonne originale manquante: {col}"

    def test_ajouter_turpe_variable_integration(self, periodes_energie_test):
        """Test d'intégration du pipeline TURPE variable."""
        resultat = ajouter_turpe_variable(periodes_energie_test).collect()

        # Vérifications de base
        assert "turpe_variable" in resultat.columns
        assert resultat.shape[0] == 2
        assert resultat["turpe_variable"].null_count() == 0

        # Toutes les valeurs doivent être positives ou nulles
        assert resultat["turpe_variable"].min() >= 0

        # Les colonnes originales doivent être préservées
        colonnes_originales = ["formule_tarifaire_acheminement", "debut", "pdl"]
        for col in colonnes_originales:
            assert col in resultat.columns, f"Colonne originale manquante: {col}"

    def test_calculs_coherents_avec_regles_reelles(self):
        """Test avec les vraies règles TURPE pour vérifier la cohérence."""
        # Test TURPE fixe avec une FTA connue
        periodes_fixe = pl.LazyFrame({
            "formule_tarifaire_acheminement": ["BTINFCUST"],
            "puissance_souscrite": [6.0],
            "nb_jours": [30],
            "debut": [datetime(2024, 11, 15)],  # Date dans les règles 2024-11-01 à 2025-02-01
        }).with_columns(
            pl.col("debut").dt.replace_time_zone("Europe/Paris")
        )

        resultat_fixe = ajouter_turpe_fixe(periodes_fixe).collect()
        turpe_fixe = resultat_fixe["turpe_fixe"][0]

        # Vérification cohérence (calcul approximatif)
        # BTINFCUST 2024-11-01: b=10.44, cg=16.2, cc=20.88
        # Annuel: (10.44 * 6) + 16.2 + 20.88 = 99.72
        # 30 jours: 99.72 / 365 * 30 ≈ 8.20
        assert 8.0 <= turpe_fixe <= 8.5, f"TURPE fixe incohérent: {turpe_fixe}"

        # Test TURPE variable avec une FTA connue
        periodes_variable = pl.LazyFrame({
            "formule_tarifaire_acheminement": ["BTINFCUST"],
            "debut": [datetime(2024, 11, 15)],
            "base_energie": [100.0],
            "hp_energie": [0.0], "hc_energie": [0.0],
            "hph_energie": [0.0], "hch_energie": [0.0],
            "hpb_energie": [0.0], "hcb_energie": [0.0],
        }).with_columns(
            pl.col("debut").dt.replace_time_zone("Europe/Paris")
        )

        resultat_variable = ajouter_turpe_variable(periodes_variable).collect()
        turpe_variable = resultat_variable["turpe_variable"][0]

        # Vérification cohérence
        # BTINFCUST 2024-11-01: BASE=4.58
        # 100 kWh: 100 * 4.58 / 100 = 4.58
        assert 4.5 <= turpe_variable <= 4.7, f"TURPE variable incohérent: {turpe_variable}"


class TestFonctionsDebug:
    """Tests pour les fonctions de debug et validation."""

    def test_debug_turpe_variable(self):
        """Test de la fonction debug_turpe_variable."""
        df_test = pl.LazyFrame({
            "base_energie": [100.0],
            "hp_energie": [50.0],
            "hc_energie": [30.0],
            "hph_energie": [40.0],
            "hch_energie": [25.0],
            "hpb_energie": [0.0],
            "hcb_energie": [0.0],
            "base": [4.37],
            "hp": [4.68],
            "hc": [3.31],
            "hph": [6.39],
            "hch": [4.43],
            "hpb": [1.46],
            "hcb": [0.91],
            "taux_turpe_base": [4.37],
            "taux_turpe_hp": [4.68],
            "taux_turpe_hc": [3.31],
            "taux_turpe_hph": [6.39],
            "taux_turpe_hch": [4.43],
            "taux_turpe_hpb": [1.46],
            "taux_turpe_hcb": [0.91],
        })

        df_debug = debug_turpe_variable(df_test).collect()

        # Vérifier que les colonnes de debug ont été ajoutées
        debug_cols = [
            "debug_energie_base", "debug_tarif_base", "debug_contribution_base",
            "debug_energie_hp", "debug_tarif_hp", "debug_contribution_hp"
        ]

        for col in debug_cols:
            assert col in df_debug.columns, f"Colonne debug manquante: {col}"

        # Vérifier les valeurs de debug
        assert df_debug["debug_energie_base"][0] == 100.0
        assert df_debug["debug_tarif_base"][0] == 4.37
        assert abs(df_debug["debug_contribution_base"][0] - 4.37) < 0.01

    def test_comparer_avec_pandas(self):
        """Test de la fonction de comparaison avec pandas."""
        # Créer des données de test
        lf = pl.LazyFrame({
            "turpe_variable": [4.37, 2.34],
            "turpe_fixe": [8.20, 10.30],
            "energie": [100.0, 50.0]
        })

        # Simuler un DataFrame pandas équivalent
        import pandas as pd
        df_pandas = pd.DataFrame({
            "turpe_variable": [4.37, 2.34],  # Identique
            "turpe_fixe": [8.21, 10.29],     # Légèrement différent
            "energie": [100.0, 50.0],        # Identique
            "autre_col": ["A", "B"]          # Colonne supplémentaire
        })

        stats = comparer_avec_pandas(lf, df_pandas)

        # Vérifier la structure des statistiques
        assert "turpe_variable_diff_max" in stats
        assert "turpe_variable_diff_moyenne" in stats
        assert "turpe_fixe_diff_max" in stats
        assert "turpe_fixe_diff_moyenne" in stats
        assert "energie_diff_max" in stats
        assert "energie_diff_moyenne" in stats

        # Vérifier les valeurs (avec tolérance pour la précision flottante)
        assert stats["turpe_variable_diff_max"] == 0.0  # Identique
        assert abs(stats["turpe_fixe_diff_max"] - 0.01) < 1e-10  # Différence maximale
        assert stats["energie_diff_max"] == 0.0         # Identique


class TestCasLimites:
    """Tests pour les cas limites et la gestion d'erreurs."""

    def test_donnees_vides(self):
        """Test avec des DataFrames vides."""
        df_vide = pl.LazyFrame({
            "formule_tarifaire_acheminement": [],
            "puissance_souscrite": [],
            "nb_jours": [],
            "debut": [],
        }, schema={
            "formule_tarifaire_acheminement": pl.String,
            "puissance_souscrite": pl.Float64,
            "nb_jours": pl.Int32,
            "debut": pl.Datetime(time_zone="Europe/Paris"),
        })

        # Ne doit pas lever d'erreur
        resultat_fixe = ajouter_turpe_fixe(df_vide).collect()
        assert resultat_fixe.shape[0] == 0
        assert "turpe_fixe" in resultat_fixe.columns

    def test_valeurs_nulles_energie(self):
        """Test avec des valeurs nulles dans les énergies."""
        df_test = pl.LazyFrame({
            "formule_tarifaire_acheminement": ["BTINFCUST"],
            "debut": [datetime(2024, 1, 1)],
            "base_energie": [None],  # Valeur nulle
            "hp_energie": [0.0],
            "hc_energie": [0.0],
            "hph_energie": [0.0],
            "hch_energie": [0.0],
            "hpb_energie": [0.0],
            "hcb_energie": [0.0],
        }).with_columns(
            pl.col("debut").dt.replace_time_zone("Europe/Paris")
        )

        resultat = ajouter_turpe_variable(df_test).collect()

        # Doit retourner 0.0 pour la contribution du cadran BASE
        assert resultat["turpe_variable"][0] == 0.0

    def test_fta_inexistante(self):
        """Test avec une FTA qui n'existe pas dans les règles."""
        df_test = pl.LazyFrame({
            "formule_tarifaire_acheminement": ["FTA_INEXISTANTE"],
            "puissance_souscrite": [6.0],
            "nb_jours": [30],
            "debut": [datetime(2024, 1, 1)],
        }).with_columns(
            pl.col("debut").dt.replace_time_zone("Europe/Paris")
        )

        # Doit lever une ValueError
        with pytest.raises(ValueError, match="Règles TURPE manquantes"):
            ajouter_turpe_fixe(df_test).collect()


if __name__ == "__main__":
    # Lancer les tests en mode standalone
    pytest.main([__file__, "-v"])