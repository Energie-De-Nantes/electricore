"""
Tests unitaires pour les expressions du pipeline facturation Polars.

Ce module teste chaque expression atomique individuellement avant de tester
les fonctions d'agrégation complètes.
"""

import polars as pl
import pytest
from datetime import datetime
import polars.testing as pl_testing

from electricore.core.pipelines.facturation import (
    expr_puissance_moyenne,
    expr_memo_puissance_simple,
    expr_coverage_temporelle,
    expr_data_complete,
    agreger_abonnements_mensuel,
    agreger_energies_mensuel,
    joindre_meta_periodes
)


class TestExpressionsAtomiques:
    """Tests des expressions atomiques."""

    def test_expr_puissance_moyenne(self):
        """Test du calcul de puissance moyenne pondérée."""
        # Données de test : 2 périodes avec puissances différentes
        data = pl.DataFrame({
            "ref_situation_contractuelle": ["REF1", "REF1"],
            "pdl": ["PDL1", "PDL1"],
            "mois_annee": ["mars 2025", "mars 2025"],
            "puissance_souscrite_kva": [6.0, 9.0],
            "nb_jours": [10, 20]  # 10j à 6kVA + 20j à 9kVA
        })

        # Test dans un groupby
        result = (
            data
            .group_by(["ref_situation_contractuelle", "pdl", "mois_annee"])
            .agg(expr_puissance_moyenne().alias("puissance_moyenne_kva"))
        )

        # Vérification : (6*10 + 9*20) / (10+20) = (60+180)/30 = 8.0
        expected = pl.DataFrame({
            "ref_situation_contractuelle": ["REF1"],
            "pdl": ["PDL1"],
            "mois_annee": ["mars 2025"],
            "puissance_moyenne_kva": [8.0]
        })

        pl_testing.assert_frame_equal(result, expected)

    def test_expr_memo_puissance_simple(self):
        """Test de la construction du mémo simple."""
        data = pl.DataFrame({
            "nb_jours": [14, 17],
            "puissance_souscrite_kva": [6.0, 9.0]
        })

        result = data.with_columns(
            expr_memo_puissance_simple().alias("memo")
        )

        expected_memos = ["14j à 6kVA", "17j à 9kVA"]
        assert result["memo"].to_list() == expected_memos

    def test_expr_coverage_temporelle(self):
        """Test du calcul de couverture temporelle."""
        data = pl.DataFrame({
            "nb_jours": [25, 31, 35],  # Dernier > 31 pour test du clip
            "nb_jours_total": [31, 31, 31]
        })

        result = data.with_columns(
            expr_coverage_temporelle().alias("coverage")
        )

        # Vérification des taux de couverture avec clip à 1.0
        expected_coverages = [25/31, 1.0, 1.0]  # Le dernier clippé à 1.0
        assert result["coverage"].to_list() == pytest.approx(expected_coverages)

    def test_expr_data_complete(self):
        """Test de la détection de données complètes."""
        data = pl.DataFrame({
            "coverage_abo": [1.0, 1.0, 0.8, 1.0],
            "coverage_energie": [1.0, 0.9, 1.0, 1.0]
        })

        result = data.with_columns(
            expr_data_complete().alias("data_complete")
        )

        # Seulement True si les deux coverages = 1.0
        expected = [True, False, False, True]
        assert result["data_complete"].to_list() == expected


class TestAgregatioAbonnements:
    """Tests de l'agrégation des abonnements."""

    def test_agregation_basique(self):
        """Test d'agrégation avec une seule période par mois."""
        data = pl.LazyFrame({
            "ref_situation_contractuelle": ["REF1"],
            "pdl": ["PDL1"],
            "mois_annee": ["mars 2025"],
            "puissance_souscrite_kva": [6.0],
            "nb_jours": [31],
            "turpe_fixe_eur": [50.0],
            "formule_tarifaire_acheminement": ["BTINF"],
            "debut": [datetime(2025, 3, 1)],
            "fin": [datetime(2025, 3, 31)],
            "debut_lisible": ["1 mars 2025"],
            "fin_lisible": ["31 mars 2025"],
            "coverage_abo": [1.0]
        })

        result = agreger_abonnements_mensuel(data)

        # Vérifications
        collected = result.collect()
        assert len(collected) == 1
        assert collected["puissance_moyenne_kva"][0] == 6.0  # Une seule période
        assert collected["nb_jours"][0] == 31
        assert collected["turpe_fixe_eur"][0] == 50.0
        assert collected["nb_sous_periodes_abo"][0] == 1
        assert collected["has_changement_abo"][0] is False
        assert collected["memo_puissance"][0] == ""  # Pas de changement

    def test_agregation_plusieurs_periodes(self):
        """Test d'agrégation avec plusieurs périodes dans le mois."""
        data = pl.LazyFrame({
            "ref_situation_contractuelle": ["REF1", "REF1"],
            "pdl": ["PDL1", "PDL1"],
            "mois_annee": ["mars 2025", "mars 2025"],
            "puissance_souscrite_kva": [6.0, 9.0],
            "nb_jours": [15, 16],
            "turpe_fixe_eur": [25.0, 30.0],
            "formule_tarifaire_acheminement": ["BTINF", "BTINF"],
            "debut": [datetime(2025, 3, 1), datetime(2025, 3, 16)],
            "fin": [datetime(2025, 3, 15), datetime(2025, 3, 31)],
            "debut_lisible": ["1 mars 2025", "16 mars 2025"],
            "fin_lisible": ["15 mars 2025", "31 mars 2025"]
        })

        result = agreger_abonnements_mensuel(data)

        # Vérifications
        collected = result.collect()
        assert len(collected) == 1

        # Puissance moyenne : (6*15 + 9*16) / (15+16) = (90+144)/31 ≈ 7.55
        expected_puissance = (6*15 + 9*16) / (15+16)
        assert collected["puissance_moyenne_kva"][0] == pytest.approx(expected_puissance)

        assert collected["nb_jours"][0] == 31  # Total
        assert collected["turpe_fixe_eur"][0] == 55.0  # Somme
        assert collected["nb_sous_periodes_abo"][0] == 2
        assert collected["has_changement_abo"][0] is True
        assert "15j à 6kVA, 16j à 9kVA" in collected["memo_puissance"][0]


class TestAgregatioEnergies:
    """Tests de l'agrégation des énergies."""

    def test_agregation_energies_basique(self):
        """Test d'agrégation des énergies avec une période."""
        data = pl.LazyFrame({
            "ref_situation_contractuelle": ["REF1"],
            "pdl": ["PDL1"],
            "mois_annee": ["mars 2025"],
            "energie_base_kwh": [1000.0],
            "energie_hp_kwh": [500.0],
            "energie_hc_kwh": [300.0],
            "turpe_variable_eur": [25.0],
            "data_complete": [True],
            "debut": [datetime(2025, 3, 1)],
            "fin": [datetime(2025, 3, 31)],
            "source_avant": ["C15"],
            "source_apres": ["R151"],
            "periode_irreguliere": [False]
        }).with_columns([
            pl.col("debut").dt.convert_time_zone("Europe/Paris"),
            pl.col("fin").dt.convert_time_zone("Europe/Paris")
        ])

        result = agreger_energies_mensuel(data)

        # Vérifications
        collected = result.collect()
        assert len(collected) == 1
        assert collected["energie_base_kwh"][0] == 1000.0
        assert collected["energie_hp_kwh"][0] == 500.0
        assert collected["energie_hc_kwh"][0] == 300.0
        assert collected["turpe_variable_eur"][0] == 25.0
        assert collected["data_complete"][0] is True
        assert collected["nb_sous_periodes_energie"][0] == 1
        assert collected["has_changement_energie"][0] is False

    def test_agregation_energies_plusieurs_periodes(self):
        """Test d'agrégation avec plusieurs périodes d'énergie."""
        data = pl.LazyFrame({
            "ref_situation_contractuelle": ["REF1", "REF1"],
            "pdl": ["PDL1", "PDL1"],
            "mois_annee": ["mars 2025", "mars 2025"],
            "energie_base_kwh": [500.0, 800.0],
            "energie_hp_kwh": [200.0, 300.0],
            "energie_hc_kwh": [100.0, 200.0],
            "turpe_variable_eur": [12.0, 18.0],
            "data_complete": [True, False],  # Une incomplète
            "debut": [datetime(2025, 3, 1), datetime(2025, 3, 15)],
            "fin": [datetime(2025, 3, 15), datetime(2025, 3, 31)],
            "source_avant": ["C15", "C15"],
            "source_apres": ["R151", "R151"],
            "periode_irreguliere": [False, False]
        }).with_columns([
            pl.col("debut").dt.convert_time_zone("Europe/Paris"),
            pl.col("fin").dt.convert_time_zone("Europe/Paris")
        ])

        result = agreger_energies_mensuel(data)

        # Vérifications
        collected = result.collect()
        assert len(collected) == 1
        assert collected["energie_base_kwh"][0] == 1300.0  # Somme
        assert collected["energie_hp_kwh"][0] == 500.0    # Somme
        assert collected["energie_hc_kwh"][0] == 300.0    # Somme
        assert collected["turpe_variable_eur"][0] == 30.0  # Somme
        assert collected["data_complete"][0] is False  # AND logique
        assert collected["nb_sous_periodes_energie"][0] == 2
        assert collected["has_changement_energie"][0] is True


class TestJointureMetaPeriodes:
    """Tests de la jointure et réconciliation."""

    def test_jointure_donnees_completes(self):
        """Test avec données abonnements et énergies complètes."""
        abo_data = pl.LazyFrame({
            "ref_situation_contractuelle": ["REF1"],
            "pdl": ["PDL1"],
            "mois_annee": ["mars 2025"],
            "puissance_moyenne_kva": [6.0],
            "nb_jours": [31],
            "turpe_fixe_eur": [50.0],
            "formule_tarifaire_acheminement": ["BTINF"],
            "debut": [datetime(2025, 3, 1)],
            "fin": [datetime(2025, 3, 31)],
            "nb_sous_periodes_abo": [1],
            "has_changement_abo": [False],
            "memo_puissance": [""],
            "debut_lisible": ["1 mars 2025"],
            "fin_lisible": ["31 mars 2025"],
            "coverage_abo": [1.0]
        })

        energie_data = pl.LazyFrame({
            "ref_situation_contractuelle": ["REF1"],
            "pdl": ["PDL1"],
            "mois_annee": ["mars 2025"],
            "energie_base_kwh": [1000.0],
            "energie_hp_kwh": [500.0],
            "energie_hc_kwh": [300.0],
            "turpe_variable_eur": [25.0],
            "data_complete": [True],
            "debut": [datetime(2025, 3, 1)],
            "fin": [datetime(2025, 3, 31)],
            "nb_sous_periodes_energie": [1],
            "has_changement_energie": [False],
            "coverage_energie": [1.0]
        })

        result = joindre_meta_periodes(abo_data, energie_data)

        # Vérifications
        collected = result.collect()
        assert len(collected) == 1
        assert collected["puissance_moyenne_kva"][0] == 6.0
        assert collected["energie_base_kwh"][0] == 1000.0
        assert collected["has_changement"][0] is False
        assert collected["coverage_abo"][0] == 1.0  # Placeholder
        assert collected["coverage_energie"][0] == 1.0  # Placeholder

    def test_jointure_donnees_decalees_temporellement(self):
        """Test avec données abonnement et énergie présentes mais décalées."""
        # Abonnement en mars, énergie en avril
        abo_data = pl.LazyFrame({
            "ref_situation_contractuelle": ["REF1"],
            "pdl": ["PDL1"],
            "mois_annee": ["mars 2025"],
            "puissance_moyenne_kva": [6.0],
            "nb_jours": [31],
            "turpe_fixe_eur": [50.0],
            "formule_tarifaire_acheminement": ["BTINF"],
            "debut": [datetime(2025, 3, 1)],
            "fin": [datetime(2025, 3, 31)],
            "nb_sous_periodes_abo": [1],
            "has_changement_abo": [False],
            "memo_puissance": [""],
            "debut_lisible": ["1 mars 2025"],
            "fin_lisible": ["31 mars 2025"],
            "coverage_abo": [1.0]
        })

        energie_data = pl.LazyFrame({
            "ref_situation_contractuelle": ["REF1"],
            "pdl": ["PDL1"],
            "mois_annee": ["avril 2025"],  # Mois différent
            "energie_base_kwh": [1000.0],
            "energie_hp_kwh": [500.0],
            "energie_hc_kwh": [300.0],
            "turpe_variable_eur": [25.0],
            "data_complete": [True],
            "debut": [datetime(2025, 4, 1)],
            "fin": [datetime(2025, 4, 30)],
            "nb_sous_periodes_energie": [1],
            "has_changement_energie": [False],
            "coverage_energie": [1.0]
        })

        result = joindre_meta_periodes(abo_data, energie_data)

        # Vérifications : doit avoir 2 lignes (une pour mars, une pour avril)
        collected = result.collect()
        assert len(collected) == 2

        # Ligne mars : abo présent, énergie null
        ligne_mars = collected.filter(pl.col("mois_annee") == "mars 2025")
        assert len(ligne_mars) == 1
        assert ligne_mars["puissance_moyenne_kva"][0] == 6.0
        assert ligne_mars["energie_base_kwh"][0] == 0.0  # Fill null

        # Ligne avril : abo null, énergie présente
        ligne_avril = collected.filter(pl.col("mois_annee") == "avril 2025")
        assert len(ligne_avril) == 1
        assert ligne_avril["puissance_moyenne_kva"][0] == 0.0  # Fill null
        assert ligne_avril["energie_base_kwh"][0] == 1000.0

    def test_jointure_avec_donnees_partielles_meme_mois(self):
        """Test avec certains PDL ayant abonnement mais pas énergie dans le même mois."""
        abo_data = pl.LazyFrame({
            "ref_situation_contractuelle": ["REF1", "REF2"],
            "pdl": ["PDL1", "PDL2"],
            "mois_annee": ["mars 2025", "mars 2025"],
            "puissance_moyenne_kva": [6.0, 9.0],
            "nb_jours": [31, 31],
            "turpe_fixe_eur": [50.0, 75.0],
            "formule_tarifaire_acheminement": ["BTINF", "BTINF"],
            "debut": [datetime(2025, 3, 1), datetime(2025, 3, 1)],
            "fin": [datetime(2025, 3, 31), datetime(2025, 3, 31)],
            "nb_sous_periodes_abo": [1, 1],
            "has_changement_abo": [False, False],
            "memo_puissance": ["", ""],
            "debut_lisible": ["1 mars 2025", "1 mars 2025"],
            "fin_lisible": ["31 mars 2025", "31 mars 2025"],
            "coverage_abo": [1.0, 1.0]
        })

        # Seulement PDL1 a de l'énergie
        energie_data = pl.LazyFrame({
            "ref_situation_contractuelle": ["REF1"],
            "pdl": ["PDL1"],
            "mois_annee": ["mars 2025"],
            "energie_base_kwh": [1000.0],
            "energie_hp_kwh": [500.0],
            "energie_hc_kwh": [300.0],
            "turpe_variable_eur": [25.0],
            "data_complete": [True],
            "debut": [datetime(2025, 3, 1)],
            "fin": [datetime(2025, 3, 31)],
            "nb_sous_periodes_energie": [1],
            "has_changement_energie": [False],
            "coverage_energie": [1.0]
        })

        result = joindre_meta_periodes(abo_data, energie_data)

        # Vérifications : doit avoir 2 lignes
        collected = result.collect()
        assert len(collected) == 2

        # PDL1 : données complètes
        pdl1 = collected.filter(pl.col("pdl") == "PDL1")
        assert len(pdl1) == 1
        assert pdl1["puissance_moyenne_kva"][0] == 6.0
        assert pdl1["energie_base_kwh"][0] == 1000.0

        # PDL2 : seulement abonnement
        pdl2 = collected.filter(pl.col("pdl") == "PDL2")
        assert len(pdl2) == 1
        assert pdl2["puissance_moyenne_kva"][0] == 9.0
        assert pdl2["energie_base_kwh"][0] == 0.0  # Fill null