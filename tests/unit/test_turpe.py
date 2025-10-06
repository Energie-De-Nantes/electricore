"""
Tests unitaires pour le pipeline TURPE Polars.

Ce module teste toutes les expressions et fonctions du pipeline TURPE
en utilisant des données contrôlées pour valider les calculs.
"""

import pytest
import polars as pl
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from electricore.core.pipelines.turpe import (
    # Chargement des règles
    load_turpe_rules,

    # Expressions TURPE fixe
    expr_calculer_turpe_fixe_annuel,
    expr_calculer_turpe_fixe_journalier,
    expr_calculer_turpe_fixe_periode,
    expr_valider_puissances_croissantes_c4,

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
        assert df.shape[1] == 18, f"Nombre de colonnes incorrect: {df.shape[1]}"

        # Vérification des colonnes obligatoires
        colonnes_attendues = [
            "Formule_Tarifaire_Acheminement", "start", "end",
            "cg", "cc", "b",
            "b_hph", "b_hch", "b_hpb", "b_hcb",
            "c_hph", "c_hch", "c_hpb", "c_hcb",
            "c_hp", "c_hc", "c_base",
            "cmdps"
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
        colonnes_numeriques = [
            "cg", "cc", "b",
            "b_hph", "b_hch", "b_hpb", "b_hcb",
            "c_hph", "c_hch", "c_hpb", "c_hcb",
            "c_hp", "c_hc", "c_base",
            "cmdps"
        ]
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
            "puissance_souscrite_hph": [None, None],  # C5 n'utilise pas ces colonnes
            "puissance_souscrite_hch": [None, None],
            "puissance_souscrite_hpb": [None, None],
            "puissance_souscrite_hcb": [None, None],
            "nb_jours": [30, 31],
            "debut": [
                datetime(2024, 1, 1),
                datetime(2024, 2, 1)
            ],
            # Valeurs règles TURPE (simulées)
            "b": [10.44, 9.36],  # €/kW/an
            "b_hph": [None, None],  # C5 n'utilise pas C4
            "b_hch": [None, None],
            "b_hpb": [None, None],
            "b_hcb": [None, None],
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
            # Tarifs TURPE (c€/kWh) - nomenclature CRE
            "c_base": [4.37, 0.0],
            "c_hp": [0.0, 4.68],
            "c_hc": [0.0, 3.31],
            "c_hph": [0.0, 6.39],
            "c_hch": [0.0, 4.43],
            "c_hpb": [0.0, 0.0],
            "c_hcb": [0.0, 0.0],
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




class TestComposanteDepassement:
    """Tests pour la composante de dépassement (CMDPS) - Phase 1."""

    def test_chargement_cmdps_dans_regles(self):
        """Test que la colonne cmdps est bien chargée depuis le CSV."""
        regles = load_turpe_rules()
        df = regles.collect()

        # Vérifier que la colonne cmdps existe
        assert "cmdps" in df.columns, "Colonne cmdps manquante"

        # Vérifier le type
        schema = regles.collect_schema()
        assert schema["cmdps"] == pl.Float64, f"Type incorrect pour cmdps: {schema['cmdps']}"

        # Vérifier que BTSUPCU/BTSUPLU ont cmdps=12.41
        btsup = df.filter(pl.col("Formule_Tarifaire_Acheminement").str.starts_with("BTSUP"))
        assert btsup.shape[0] >= 2, "Règles BTSUP manquantes"
        assert (btsup["cmdps"] == 12.41).all(), "Valeur cmdps incorrecte pour BTSUP"

        # Vérifier que BTINF* ont cmdps NULL
        btinf = df.filter(pl.col("Formule_Tarifaire_Acheminement").str.starts_with("BTINF"))
        assert btinf["cmdps"].is_null().all(), "cmdps devrait être NULL pour BTINF (C5)"

    def test_expr_calculer_composante_depassement_c4_avec_depassement(self):
        """Test C4 avec dépassement → pénalités calculées."""
        from electricore.core.pipelines.turpe import expr_calculer_composante_depassement

        df_test = pl.DataFrame({
            "cmdps": [12.41],  # €/h (C4)
            "depassement_puissance_h": [10.0],  # 10 heures de dépassement
        })

        df_result = df_test.with_columns(
            expr_calculer_composante_depassement().alias("penalites")
        )

        # Calcul attendu: 10.0 × 12.41 = 124.10 €
        attendu = 124.10
        resultat = df_result["penalites"][0]

        assert abs(resultat - attendu) < 0.01, f"Attendu {attendu}, obtenu {resultat}"

    def test_expr_calculer_composante_depassement_c5_pas_penalites(self):
        """Test C5 avec cmdps=NULL → pas de pénalités."""
        from electricore.core.pipelines.turpe import expr_calculer_composante_depassement

        df_test = pl.DataFrame({
            "cmdps": [None],  # NULL (C5 - pas de pénalités)
            "depassement_puissance_h": [10.0],  # Dépassement présent mais ignoré
        })

        df_result = df_test.with_columns(
            expr_calculer_composante_depassement().alias("penalites")
        )

        # Attendu: 0 (pas de pénalités pour C5)
        resultat = df_result["penalites"][0]

        assert resultat == 0.0, f"C5 ne devrait pas avoir de pénalités, obtenu {resultat}"

    def test_expr_calculer_composante_depassement_sans_depassement(self):
        """Test C4 sans dépassement → pas de pénalités."""
        from electricore.core.pipelines.turpe import expr_calculer_composante_depassement

        df_test = pl.DataFrame({
            "cmdps": [12.41],  # €/h (C4)
            "depassement_puissance_h": [0.0],  # Pas de dépassement
        })

        df_result = df_test.with_columns(
            expr_calculer_composante_depassement().alias("penalites")
        )

        # Attendu: 0 (pas de dépassement)
        resultat = df_result["penalites"][0]

        assert resultat == 0.0, f"Pas de dépassement → pas de pénalités, obtenu {resultat}"

    def test_expr_calculer_composante_depassement_colonne_absente(self):
        """Test robustesse si colonne depassement_puissance_h absente."""
        from electricore.core.pipelines.turpe import expr_calculer_composante_depassement

        df_test = pl.DataFrame({
            "cmdps": [12.41],  # €/h (C4)
            "depassement_puissance_h": [None],  # Colonne NULL
        })

        df_result = df_test.with_columns(
            expr_calculer_composante_depassement().alias("penalites")
        )

        # Attendu: 0 (pas d'erreur, valeur par défaut)
        resultat = df_result["penalites"][0]

        assert resultat == 0.0, f"Colonne absente → 0, obtenu {resultat}"

    def test_integration_turpe_variable_avec_cmdps(self):
        """Test d'intégration : ajouter_turpe_variable() intègre CMDPS."""
        # Préparer données de test avec BTSUPCU (C4)
        periodes_c4 = pl.LazyFrame({
            "formule_tarifaire_acheminement": ["BTSUPCU"],
            "debut": [datetime(2025, 9, 1)],  # Après 2025-08-01 (règles actuelles)
            "pdl": ["PDL_C4_001"],
            # Énergies (kWh)
            "base_energie": [0.0],
            "hp_energie": [0.0],
            "hc_energie": [0.0],
            "hph_energie": [100.0],  # 100 kWh en HPH
            "hch_energie": [50.0],   # 50 kWh en HCH
            "hpb_energie": [30.0],   # 30 kWh en HPB
            "hcb_energie": [20.0],   # 20 kWh en HCB
            # Dépassement
            "depassement_puissance_h": [5.0],  # 5 heures de dépassement
        }).with_columns(
            pl.col("debut").dt.replace_time_zone("Europe/Paris")
        )

        # Appliquer le pipeline
        resultat = ajouter_turpe_variable(periodes_c4).collect()

        # Vérifications
        assert "turpe_variable" in resultat.columns
        assert resultat.shape[0] == 1

        turpe_total = resultat["turpe_variable"][0]

        # Calcul attendu :
        # - Cadrans : (100×6.91 + 50×4.21 + 30×2.13 + 20×1.52) / 100
        #           = (691 + 210.5 + 63.9 + 30.4) / 100 = 995.8 / 100 = 9.958 €
        # - CMDPS : 5 × 12.41 = 62.05 €
        # - Total : 9.958 + 62.05 = 72.008 €

        turpe_cadrans = (100*6.91 + 50*4.21 + 30*2.13 + 20*1.52) / 100
        turpe_cmdps = 5.0 * 12.41
        attendu = turpe_cadrans + turpe_cmdps

        assert abs(turpe_total - attendu) < 0.01, \
            f"Turpe total attendu {attendu:.2f} €, obtenu {turpe_total:.2f} €"

    def test_integration_turpe_variable_c5_sans_cmdps(self):
        """Test que C5 (BTINF*) fonctionne toujours sans CMDPS."""
        # Préparer données de test avec BTINFCU4 (C5)
        periodes_c5 = pl.LazyFrame({
            "formule_tarifaire_acheminement": ["BTINFCU4"],
            "debut": [datetime(2025, 9, 1)],
            "pdl": ["PDL_C5_001"],
            # Énergies (kWh)
            "base_energie": [0.0],
            "hp_energie": [0.0],
            "hc_energie": [0.0],
            "hph_energie": [100.0],
            "hch_energie": [50.0],
            "hpb_energie": [30.0],
            "hcb_energie": [20.0],
            # PAS de colonne depassement_puissance_h (C5 n'en a pas besoin)
        }).with_columns(
            pl.col("debut").dt.replace_time_zone("Europe/Paris")
        )

        # Appliquer le pipeline (ne devrait pas planter)
        resultat = ajouter_turpe_variable(periodes_c5).collect()

        # Vérifications
        assert "turpe_variable" in resultat.columns
        assert resultat.shape[0] == 1

        turpe_total = resultat["turpe_variable"][0]

        # Calcul attendu : seulement les cadrans (pas de CMDPS pour C5)
        # (100×7.49 + 50×3.97 + 30×1.66 + 20×1.16) / 100 = 1021.7 / 100 = 10.217 €
        turpe_cadrans = (100*7.49 + 50*3.97 + 30*1.66 + 20*1.16) / 100

        assert abs(turpe_total - turpe_cadrans) < 0.01, \
            f"C5 devrait avoir seulement cadrans ({turpe_cadrans:.2f} €), obtenu {turpe_total:.2f} €"


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
            "c_base": [4.37],
            "c_hp": [4.68],
            "c_hc": [3.31],
            "c_hph": [6.39],
            "c_hch": [4.43],
            "c_hpb": [1.46],
            "c_hcb": [0.91],
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


class TestTurpeFixeC4:
    """Tests pour le TURPE fixe C4 (BT > 36 kVA) avec 4 puissances souscrites."""

    def test_chargement_colonnes_c4_dans_regles(self):
        """Test que les colonnes C4 (b_*, c_*) sont bien présentes dans turpe_rules.csv."""
        regles = load_turpe_rules()
        df = regles.collect()

        # Vérifier colonnes coefficients puissance C4
        colonnes_b_c4 = ["b_hph", "b_hch", "b_hpb", "b_hcb"]
        for col in colonnes_b_c4:
            assert col in df.columns, f"Colonne C4 manquante: {col}"

        # Vérifier colonnes coefficients énergie C4
        colonnes_c_c4 = ["c_hph", "c_hch", "c_hpb", "c_hcb"]
        for col in colonnes_c_c4:
            assert col in df.columns, f"Colonne C4 manquante: {col}"

        # Vérifier colonnes coefficients énergie C5 (renommées avec prefix c_)
        colonnes_c_c5 = ["c_hp", "c_hc", "c_base"]
        for col in colonnes_c_c5:
            assert col in df.columns, f"Colonne C5 manquante: {col}"

    def test_valeurs_btsupcu_correctes(self):
        """Test que BTSUPCU a les bonnes valeurs CRE pour les coefficients C4."""
        regles = load_turpe_rules()
        df = regles.filter(pl.col("Formule_Tarifaire_Acheminement") == "BTSUPCU").collect()

        assert df.shape[0] > 0, "BTSUPCU introuvable"

        # Prendre la dernière ligne (tarifs 2025-08-01)
        row = df.row(-1, named=True)

        # Coefficients puissance (€/kVA/an) - CRE officiel
        assert row["b_hph"] == 17.61, f"b_hph incorrect: {row['b_hph']}"
        assert row["b_hch"] == 15.96, f"b_hch incorrect: {row['b_hch']}"
        assert row["b_hpb"] == 14.56, f"b_hpb incorrect: {row['b_hpb']}"
        assert row["b_hcb"] == 11.98, f"b_hcb incorrect: {row['b_hcb']}"

        # Coefficients énergie (c€/kWh) - CRE officiel
        assert row["c_hph"] == 6.91, f"c_hph incorrect: {row['c_hph']}"
        assert row["c_hch"] == 4.21, f"c_hch incorrect: {row['c_hch']}"
        assert row["c_hpb"] == 2.13, f"c_hpb incorrect: {row['c_hpb']}"
        assert row["c_hcb"] == 1.52, f"c_hcb incorrect: {row['c_hcb']}"

    def test_expr_valider_puissances_croissantes_c4_valide(self):
        """Test validation P₁ ≤ P₂ ≤ P₃ ≤ P₄ - cas valide."""
        df = pl.DataFrame({
            "puissance_souscrite_hph": [36.0, 50.0],
            "puissance_souscrite_hch": [36.0, 60.0],
            "puissance_souscrite_hpb": [60.0, 80.0],
            "puissance_souscrite_hcb": [60.0, 100.0],
        })

        resultat = df.with_columns(
            expr_valider_puissances_croissantes_c4().alias("valide")
        )

        assert resultat["valide"].to_list() == [True, True]

    def test_expr_valider_puissances_croissantes_c4_invalide(self):
        """Test validation P₁ ≤ P₂ ≤ P₃ ≤ P₄ - cas invalides."""
        df = pl.DataFrame({
            "puissance_souscrite_hph": [60.0, 36.0, 36.0],  # P₁
            "puissance_souscrite_hch": [36.0, 50.0, 36.0],  # P₂
            "puissance_souscrite_hpb": [36.0, 40.0, 36.0],  # P₃
            "puissance_souscrite_hcb": [36.0, 36.0, 30.0],  # P₄
        })

        resultat = df.with_columns(
            expr_valider_puissances_croissantes_c4().alias("valide")
        )

        # Tous invalides : P₁ > P₂, P₂ > P₃, P₄ < P₃
        assert resultat["valide"].to_list() == [False, False, False]

    def test_calcul_turpe_fixe_c4_puissance_constante(self):
        """Test calcul TURPE fixe C4 - Exemple 1 : puissance constante (60 kVA)."""
        df = pl.DataFrame({
            "formule_tarifaire_acheminement": ["BTSUPCU"],
            "puissance_souscrite": [0.0],  # Non utilisé en C4
            "puissance_souscrite_hph": [60.0],
            "puissance_souscrite_hch": [60.0],
            "puissance_souscrite_hpb": [60.0],
            "puissance_souscrite_hcb": [60.0],
            "nb_jours": [365],
            "debut": [datetime(2025, 8, 1, tzinfo=ZoneInfo("Europe/Paris"))],
        })

        regles = load_turpe_rules()

        resultat = (
            df.lazy()
            .join(
                regles,
                left_on="formule_tarifaire_acheminement",
                right_on="Formule_Tarifaire_Acheminement",
                how="left"
            )
            .filter(pl.col("start").is_not_null())
            .with_columns(expr_calculer_turpe_fixe_annuel().alias("turpe_fixe_annuel"))
            .collect()
        )

        # Calcul attendu (tarifs CRE officiels) : 17.61×60 + cg + cc = 1056.6 + 217.8 + 283.27 = 1557.67 €
        turpe_annuel = resultat["turpe_fixe_annuel"][0]
        assert abs(turpe_annuel - 1557.67) < 0.01, f"TURPE annuel incorrect: {turpe_annuel}"

    def test_calcul_turpe_fixe_c4_modulation_moderee(self):
        """Test calcul TURPE fixe C4 - Exemple 2 : modulation modérée (36/36/60/60 kVA)."""
        df = pl.DataFrame({
            "formule_tarifaire_acheminement": ["BTSUPCU"],
            "puissance_souscrite": [0.0],
            "puissance_souscrite_hph": [36.0],
            "puissance_souscrite_hch": [36.0],
            "puissance_souscrite_hpb": [60.0],
            "puissance_souscrite_hcb": [60.0],
            "nb_jours": [365],
            "debut": [datetime(2025, 8, 1, tzinfo=ZoneInfo("Europe/Paris"))],
        })

        regles = load_turpe_rules()

        resultat = (
            df.lazy()
            .join(regles, left_on="formule_tarifaire_acheminement", right_on="Formule_Tarifaire_Acheminement", how="left")
            .filter(pl.col("start").is_not_null())
            .with_columns(expr_calculer_turpe_fixe_annuel().alias("turpe_fixe_annuel"))
            .collect()
        )

        # Calcul attendu (tarifs CRE officiels) :
        # b₁×P₁ + b₂×(P₂-P₁) + b₃×(P₃-P₂) + b₄×(P₄-P₃) + cg + cc
        # = 17.61×36 + 15.96×0 + 14.56×24 + 11.98×0 + 217.8 + 283.27
        # = 633.96 + 0 + 349.44 + 0 + 501.07 = 1484.47 €
        turpe_annuel = resultat["turpe_fixe_annuel"][0]
        assert abs(turpe_annuel - 1484.47) < 0.01, f"TURPE annuel incorrect: {turpe_annuel}"

    def test_calcul_turpe_fixe_c4_modulation_extreme(self):
        """Test calcul TURPE fixe C4 - Exemple 3 : modulation extrême (36/36/36/100 kVA)."""
        df = pl.DataFrame({
            "formule_tarifaire_acheminement": ["BTSUPCU"],
            "puissance_souscrite": [0.0],
            "puissance_souscrite_hph": [36.0],
            "puissance_souscrite_hch": [36.0],
            "puissance_souscrite_hpb": [36.0],
            "puissance_souscrite_hcb": [100.0],
            "nb_jours": [365],
            "debut": [datetime(2025, 8, 1, tzinfo=ZoneInfo("Europe/Paris"))],
        })

        regles = load_turpe_rules()

        resultat = (
            df.lazy()
            .join(regles, left_on="formule_tarifaire_acheminement", right_on="Formule_Tarifaire_Acheminement", how="left")
            .filter(pl.col("start").is_not_null())
            .with_columns(expr_calculer_turpe_fixe_annuel().alias("turpe_fixe_annuel"))
            .collect()
        )

        # Calcul attendu (tarifs CRE officiels) :
        # b₁×P₁ + b₂×(P₂-P₁) + b₃×(P₃-P₂) + b₄×(P₄-P₃) + cg + cc
        # = 17.61×36 + 15.96×0 + 14.56×0 + 11.98×64 + 217.8 + 283.27
        # = 633.96 + 0 + 0 + 766.72 + 501.07 = 1901.75 €
        turpe_annuel = resultat["turpe_fixe_annuel"][0]
        assert abs(turpe_annuel - 1901.75) < 0.01, f"TURPE annuel incorrect: {turpe_annuel}"

    def test_retrocompatibilite_c5_inchangee(self):
        """Test que le calcul C5 reste inchangé (pas de régression)."""
        df = pl.DataFrame({
            "formule_tarifaire_acheminement": ["BTINFCU4"],
            "puissance_souscrite": [36.0],
            "puissance_souscrite_hph": [None],  # C5 n'utilise pas ces colonnes
            "puissance_souscrite_hch": [None],
            "puissance_souscrite_hpb": [None],
            "puissance_souscrite_hcb": [None],
            "nb_jours": [365],
            "debut": [datetime(2025, 8, 1, tzinfo=ZoneInfo("Europe/Paris"))],
        })

        regles = load_turpe_rules()

        resultat = (
            df.lazy()
            .join(regles, left_on="formule_tarifaire_acheminement", right_on="Formule_Tarifaire_Acheminement", how="left")
            .filter(pl.col("start").is_not_null())
            .filter(expr_filtrer_regles_temporelles())  # Filtrer pour garder seulement la règle applicable
            .with_columns(expr_calculer_turpe_fixe_annuel().alias("turpe_fixe_annuel"))
            .collect()
        )

        # Calcul C5 : (b × P) + cg + cc = (10.11 × 36) + 16.8 + 22 = 363.96 + 38.8 = 402.76 €
        turpe_annuel = resultat["turpe_fixe_annuel"][0]
        assert abs(turpe_annuel - 402.76) < 0.01, f"TURPE C5 incorrect (régression): {turpe_annuel}"

    def test_integration_pipeline_c4_complet(self):
        """Test intégration complète du pipeline avec données C4."""
        periodes = pl.LazyFrame({
            "formule_tarifaire_acheminement": ["BTSUPCU"],
            "puissance_souscrite": [0.0],
            "puissance_souscrite_hph": [36.0],
            "puissance_souscrite_hch": [50.0],
            "puissance_souscrite_hpb": [70.0],
            "puissance_souscrite_hcb": [90.0],
            "nb_jours": [30],
            "debut": [datetime(2025, 9, 1, tzinfo=ZoneInfo("Europe/Paris"))],
        })

        resultat = ajouter_turpe_fixe(periodes).collect()

        assert "turpe_fixe" in resultat.columns
        assert resultat.shape[0] == 1
        assert resultat["turpe_fixe"][0] > 0

    def test_integration_pipeline_mixte_c4_et_c5(self):
        """Test intégration pipeline avec mélange C4 et C5."""
        periodes = pl.LazyFrame({
            "formule_tarifaire_acheminement": ["BTSUPCU", "BTINFCU4"],
            "puissance_souscrite": [0.0, 36.0],
            "puissance_souscrite_hph": [36.0, None],
            "puissance_souscrite_hch": [40.0, None],
            "puissance_souscrite_hpb": [50.0, None],
            "puissance_souscrite_hcb": [60.0, None],
            "nb_jours": [30, 30],
            "debut": [
                datetime(2025, 9, 1, tzinfo=ZoneInfo("Europe/Paris")),
                datetime(2025, 9, 1, tzinfo=ZoneInfo("Europe/Paris"))
            ],
        })

        resultat = ajouter_turpe_fixe(periodes).collect()

        assert resultat.shape[0] == 2
        assert all(resultat["turpe_fixe"] > 0)


if __name__ == "__main__":
    # Lancer les tests en mode standalone
    pytest.main([__file__, "-v"])