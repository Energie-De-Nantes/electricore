"""
Tests unitaires pour les expressions du pipeline facturation Polars.

Ce module teste chaque expression atomique individuellement avant de tester
les fonctions d'agrégation complètes.
"""

from datetime import datetime

import polars as pl
import polars.testing as pl_testing
import pytest

from electricore.core.pipelines.facturation import (
    agreger_abonnements_mensuel,
    agreger_energies_mensuel,
    expr_coverage_temporelle,
    expr_data_complete_meta_periode,
    expr_memo_puissance_simple,
    expr_puissance_moyenne,
    joindre_meta_periodes,
)


def _energie_sous_periodes_statut(qualites: list[str], statuts: list[str], mois: str = "2025-03") -> pl.LazyFrame:
    """Sous-périodes d'énergie (PeriodeEnergie-shaped) portant les axes jumeaux qualité +
    communication par sous-période, pour tester les rollups méta (pire-gagne / plein-ou-rien)."""
    n = len(qualites)
    return pl.LazyFrame(
        {
            "ref_situation_contractuelle": ["REF1"] * n,
            "pdl": ["PDL1"] * n,
            "mois_annee": [mois] * n,
            "energie_base_kwh": [1000.0] * n,
            "energie_hp_kwh": [0.0] * n,
            "energie_hc_kwh": [0.0] * n,
            "turpe_variable_eur": [0.0] * n,
            "data_complete": [True] * n,
            "qualite": qualites,
            "statut_communication": statuts,
            "debut": [datetime(2025, 3, 1 + i) for i in range(n)],
            "fin": [datetime(2025, 3, 2 + i) for i in range(n)],
            "source_avant": ["C15"] * n,
            "source_apres": ["R151"] * n,
        }
    ).with_columns(
        [pl.col("debut").dt.convert_time_zone("Europe/Paris"), pl.col("fin").dt.convert_time_zone("Europe/Paris")]
    )


class TestExpressionsAtomiques:
    """Tests des expressions atomiques."""

    def test_expr_puissance_moyenne(self):
        """Test du calcul de puissance moyenne pondérée."""
        # Données de test : 2 périodes avec puissances différentes
        data = pl.DataFrame(
            {
                "ref_situation_contractuelle": ["REF1", "REF1"],
                "pdl": ["PDL1", "PDL1"],
                "mois_annee": ["2025-03", "2025-03"],
                "puissance_souscrite_kva": [6.0, 9.0],
                "nb_jours": [10, 20],  # 10j à 6kVA + 20j à 9kVA
            }
        )

        # Test dans un groupby
        result = data.group_by(["ref_situation_contractuelle", "pdl", "mois_annee"]).agg(
            expr_puissance_moyenne().alias("puissance_moyenne_kva")
        )

        # Vérification : (6*10 + 9*20) / (10+20) = (60+180)/30 = 8.0
        expected = pl.DataFrame(
            {
                "ref_situation_contractuelle": ["REF1"],
                "pdl": ["PDL1"],
                "mois_annee": ["2025-03"],
                "puissance_moyenne_kva": [8.0],
            }
        )

        pl_testing.assert_frame_equal(result, expected)

    def test_expr_memo_puissance_simple(self):
        """Test de la construction du mémo simple."""
        data = pl.DataFrame({"nb_jours": [14, 17], "puissance_souscrite_kva": [6.0, 9.0]})

        result = data.with_columns(expr_memo_puissance_simple().alias("memo"))

        expected_memos = ["14j à 6kVA", "17j à 9kVA"]
        assert result["memo"].to_list() == expected_memos

    def test_expr_coverage_temporelle(self):
        """Test du calcul de couverture temporelle."""
        data = pl.DataFrame(
            {
                "nb_jours": [25, 31, 35],  # Dernier > 31 pour test du clip
                "nb_jours_total": [31, 31, 31],
            }
        )

        result = data.with_columns(expr_coverage_temporelle().alias("coverage"))

        # Vérification des taux de couverture avec clip à 1.0
        expected_coverages = [25 / 31, 1.0, 1.0]  # Le dernier clippé à 1.0
        assert result["coverage"].to_list() == pytest.approx(expected_coverages)

    def test_expr_data_complete(self):
        """Test de la détection de données complètes."""
        data = pl.DataFrame(
            {
                "coverage_abo": [1.0, 1.0, 0.8, 1.0],
                "coverage_energie": [1.0, 0.9, 1.0, 1.0],
                "nb_sous_periodes_energie": [1, 1, 1, 0],
            }
        )

        result = data.with_columns(expr_data_complete_meta_periode().alias("data_complete"))

        # True si coverage_abo=1.0 ET coverage_energie=1.0 ET nb_sous_periodes_energie > 0
        expected = [True, False, False, False]
        assert result["data_complete"].to_list() == expected


class TestAgregatioAbonnements:
    """Tests de l'agrégation des abonnements."""

    def test_agregation_basique(self):
        """Test d'agrégation avec une seule période par mois."""
        data = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["REF1"],
                "pdl": ["PDL1"],
                "mois_annee": ["2025-03"],
                "puissance_souscrite_kva": [6.0],
                "nb_jours": [31],
                "turpe_fixe_eur": [50.0],
                "formule_tarifaire_acheminement": ["BTINF"],
                "debut": [datetime(2025, 3, 1)],
                "fin": [datetime(2025, 3, 31)],
                "debut_lisible": ["1 mars 2025"],
                "fin_lisible": ["31 mars 2025"],
                "coverage_abo": [1.0],
            }
        )

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
        data = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["REF1", "REF1"],
                "pdl": ["PDL1", "PDL1"],
                "mois_annee": ["2025-03", "2025-03"],
                "puissance_souscrite_kva": [6.0, 9.0],
                "nb_jours": [15, 16],
                "turpe_fixe_eur": [25.0, 30.0],
                "formule_tarifaire_acheminement": ["BTINF", "BTINF"],
                "debut": [datetime(2025, 3, 1), datetime(2025, 3, 16)],
                "fin": [datetime(2025, 3, 15), datetime(2025, 3, 31)],
                "debut_lisible": ["1 mars 2025", "16 mars 2025"],
                "fin_lisible": ["15 mars 2025", "31 mars 2025"],
            }
        )

        result = agreger_abonnements_mensuel(data)

        # Vérifications
        collected = result.collect()
        assert len(collected) == 1

        # Puissance moyenne : (6*15 + 9*16) / (15+16) = (90+144)/31 ≈ 7.55
        expected_puissance = (6 * 15 + 9 * 16) / (15 + 16)
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
        data = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["REF1"],
                "pdl": ["PDL1"],
                "mois_annee": ["2025-03"],
                "energie_base_kwh": [1000.0],
                "energie_hp_kwh": [500.0],
                "energie_hc_kwh": [300.0],
                "turpe_variable_eur": [25.0],
                "data_complete": [True],
                "debut": [datetime(2025, 3, 1)],
                "fin": [datetime(2025, 3, 31)],
                "source_avant": ["C15"],
                "source_apres": ["R151"],
                "periode_irreguliere": [False],
            }
        ).with_columns(
            [pl.col("debut").dt.convert_time_zone("Europe/Paris"), pl.col("fin").dt.convert_time_zone("Europe/Paris")]
        )

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
        data = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["REF1", "REF1"],
                "pdl": ["PDL1", "PDL1"],
                "mois_annee": ["2025-03", "2025-03"],
                "energie_base_kwh": [500.0, 800.0],
                "energie_hp_kwh": [200.0, 300.0],
                "energie_hc_kwh": [100.0, 200.0],
                "turpe_variable_eur": [12.0, 18.0],
                "data_complete": [True, False],  # Une incomplète
                "debut": [datetime(2025, 3, 1), datetime(2025, 3, 15)],
                "fin": [datetime(2025, 3, 15), datetime(2025, 3, 31)],
                "source_avant": ["C15", "C15"],
                "source_apres": ["R151", "R151"],
                "periode_irreguliere": [False, False],
            }
        ).with_columns(
            [pl.col("debut").dt.convert_time_zone("Europe/Paris"), pl.col("fin").dt.convert_time_zone("Europe/Paris")]
        )

        result = agreger_energies_mensuel(data)

        # Vérifications
        collected = result.collect()
        assert len(collected) == 1
        assert collected["energie_base_kwh"][0] == 1300.0  # Somme de toutes les énergies
        assert collected["energie_hp_kwh"][0] == 500.0  # Somme
        assert collected["energie_hc_kwh"][0] == 300.0  # Somme
        assert collected["turpe_variable_eur"][0] == 30.0  # Somme
        assert collected["data_complete"][0] is False  # AND logique (une période incomplète)
        assert collected["nb_sous_periodes_energie"][0] == 1  # SEULEMENT les périodes complètes
        assert collected["has_changement_energie"][0] is False  # 1 seule période complète
        # Coverage = 14j (période complète) / 30j (total mars) ≈ 0.467
        assert collected["coverage_energie"][0] == pytest.approx(14 / 30, abs=0.01)

    def test_agregation_qualite_pire_gagne(self):
        """ADR-0033 : la qualité d'une méta-période est le rollup PIRE-GAGNE de la qualité
        de ses sous-périodes d'énergie (incalculable > estimée > réelle)."""
        data = _energie_sous_periodes_statut(["réelle", "estimée"], ["communicante", "communicante"])
        assert agreger_energies_mensuel(data).collect()["qualite"][0] == "estimée"

    def test_agregation_communication_plein_ou_rien(self):
        """ADR-0036 : la méta-période est communicante ssi TOUTES ses sous-périodes le sont
        (plein-ou-rien). Couvre les cas #325 : mois plein communicant → communicante ;
        bascule mid-mois (une sous-période non-communicante) → non-communicante."""
        plein = _energie_sous_periodes_statut(["réelle", "réelle"], ["communicante", "communicante"])
        bascule = _energie_sous_periodes_statut(["réelle", "réelle"], ["communicante", "non_communicante"])
        assert agreger_energies_mensuel(plein).collect()["statut_communication"][0] == "communicante"
        assert agreger_energies_mensuel(bascule).collect()["statut_communication"][0] == "non_communicante"


class TestJointureMetaPeriodes:
    """Tests de la jointure et réconciliation."""

    def test_jointure_donnees_completes(self):
        """Test avec données abonnements et énergies complètes."""
        abo_data = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["REF1"],
                "pdl": ["PDL1"],
                "mois_annee": ["2025-03"],
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
                "coverage_abo": [1.0],
            }
        )

        energie_data = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["REF1"],
                "pdl": ["PDL1"],
                "mois_annee": ["2025-03"],
                "energie_base_kwh": [1000.0],
                "energie_hp_kwh": [500.0],
                "energie_hc_kwh": [300.0],
                "turpe_variable_eur": [25.0],
                "data_complete": [True],
                "debut": [datetime(2025, 3, 1)],
                "fin": [datetime(2025, 3, 31)],
                "nb_sous_periodes_energie": [1],
                "has_changement_energie": [False],
                "coverage_energie": [1.0],
            }
        )

        result = joindre_meta_periodes(abo_data, energie_data)

        # Vérifications
        collected = result.collect()
        assert len(collected) == 1
        assert collected["puissance_moyenne_kva"][0] == 6.0
        assert collected["energie_base_kwh"][0] == 1000.0
        assert collected["has_changement"][0] is False
        assert collected["coverage_abo"][0] == 1.0  # Placeholder
        assert collected["coverage_energie"][0] == 1.0  # Placeholder

    def test_jointure_porte_axes_statut(self):
        """Les verdicts méta jumeaux (qualité ADR-0033 / communication ADR-0036) de
        l'agrégat énergie sont portés sur la méta-période ; un mois sans énergie
        (abonnement seul) tombe à incalculable / non-communicante (pas de donnée énergie
        à router)."""
        abo_data = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["REF1", "REF2"],
                "pdl": ["PDL1", "PDL2"],
                "mois_annee": ["2025-03", "2025-04"],
                "puissance_moyenne_kva": [6.0, 6.0],
                "nb_jours": [31, 30],
                "turpe_fixe_eur": [50.0, 50.0],
                "formule_tarifaire_acheminement": ["BTINF", "BTINF"],
                "debut": [datetime(2025, 3, 1), datetime(2025, 4, 1)],
                "fin": [datetime(2025, 3, 31), datetime(2025, 4, 30)],
                "nb_sous_periodes_abo": [1, 1],
                "has_changement_abo": [False, False],
                "memo_puissance": ["", ""],
                "coverage_abo": [1.0, 1.0],
            }
        )
        energie_data = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["REF1"],
                "pdl": ["PDL1"],
                "mois_annee": ["2025-03"],
                "energie_base_kwh": [1000.0],
                "energie_hp_kwh": [500.0],
                "energie_hc_kwh": [300.0],
                "turpe_variable_eur": [25.0],
                "data_complete": [True],
                "qualite": ["estimée"],
                "statut_communication": ["communicante"],
                "debut": [datetime(2025, 3, 1)],
                "fin": [datetime(2025, 3, 31)],
                "nb_sous_periodes_energie": [1],
                "has_changement_energie": [False],
                "coverage_energie": [1.0],
            }
        )

        collected = joindre_meta_periodes(abo_data, energie_data).collect()
        ref1 = collected.filter(pl.col("ref_situation_contractuelle") == "REF1")
        ref2 = collected.filter(pl.col("ref_situation_contractuelle") == "REF2")

        # REF1 (avec énergie) : passe-plat des verdicts.
        assert ref1["qualite"][0] == "estimée"
        assert ref1["statut_communication"][0] == "communicante"
        # REF2 (abonnement seul, pas d'énergie) : incalculable / non-communicante.
        assert ref2["qualite"][0] == "incalculable"
        assert ref2["statut_communication"][0] == "non_communicante"

    def test_jointure_donnees_decalees_temporellement(self):
        """Test avec données abonnement et énergie présentes mais décalées."""
        # Abonnement en mars, énergie en avril
        abo_data = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["REF1"],
                "pdl": ["PDL1"],
                "mois_annee": ["2025-03"],
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
                "coverage_abo": [1.0],
            }
        )

        energie_data = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["REF1"],
                "pdl": ["PDL1"],
                "mois_annee": ["2025-04"],  # Mois différent
                "energie_base_kwh": [1000.0],
                "energie_hp_kwh": [500.0],
                "energie_hc_kwh": [300.0],
                "turpe_variable_eur": [25.0],
                "data_complete": [True],
                "debut": [datetime(2025, 4, 1)],
                "fin": [datetime(2025, 4, 30)],
                "nb_sous_periodes_energie": [1],
                "has_changement_energie": [False],
                "coverage_energie": [1.0],
            }
        )

        result = joindre_meta_periodes(abo_data, energie_data)

        # Vérifications : doit avoir 2 lignes (une pour mars, une pour avril)
        collected = result.collect()
        assert len(collected) == 2

        # Ligne mars : abo présent, énergie null
        ligne_mars = collected.filter(pl.col("mois_annee") == "2025-03")
        assert len(ligne_mars) == 1
        assert ligne_mars["puissance_moyenne_kva"][0] == 6.0
        assert ligne_mars["energie_base_kwh"][0] == 0.0  # Fill null

        # Ligne avril : abo null, énergie présente
        ligne_avril = collected.filter(pl.col("mois_annee") == "2025-04")
        assert len(ligne_avril) == 1
        assert ligne_avril["puissance_moyenne_kva"][0] == 0.0  # Fill null
        assert ligne_avril["energie_base_kwh"][0] == 1000.0

    def test_jointure_avec_donnees_partielles_meme_mois(self):
        """Test avec certains PDL ayant abonnement mais pas énergie dans le même mois."""
        abo_data = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["REF1", "REF2"],
                "pdl": ["PDL1", "PDL2"],
                "mois_annee": ["2025-03", "2025-03"],
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
                "coverage_abo": [1.0, 1.0],
            }
        )

        # Seulement PDL1 a de l'énergie
        energie_data = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["REF1"],
                "pdl": ["PDL1"],
                "mois_annee": ["2025-03"],
                "energie_base_kwh": [1000.0],
                "energie_hp_kwh": [500.0],
                "energie_hc_kwh": [300.0],
                "turpe_variable_eur": [25.0],
                "data_complete": [True],
                "debut": [datetime(2025, 3, 1)],
                "fin": [datetime(2025, 3, 31)],
                "nb_sous_periodes_energie": [1],
                "has_changement_energie": [False],
                "coverage_energie": [1.0],
            }
        )

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


class TestDatesLisiblesFrancaises:
    """Issue #178 : les méta-périodes portent des dates lisibles en français.

    Les sous-périodes (abonnements, énergie) formatent leurs libellés via
    `expr_date_formatee_fr` (« 01 mars 2025 ») ; les méta-périodes doivent
    suivre la même convention — pas le `%B` anglais de chrono (« 01 March 2025 »).
    """

    def test_meta_periodes_dates_lisibles_en_francais(self):
        from zoneinfo import ZoneInfo

        from electricore.core.pipelines.facturation import pipeline_facturation

        paris = ZoneInfo("Europe/Paris")

        abonnements = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["RSC1"],
                "pdl": ["PDL1"],
                "mois_annee": ["2025-03"],
                "debut_lisible": ["01 mars 2025"],
                "fin_lisible": ["31 mars 2025"],
                "formule_tarifaire_acheminement": ["BTINFCU4"],
                "puissance_souscrite_kva": [6.0],
                "nb_jours": [30],
                "debut": [datetime(2025, 3, 1, tzinfo=paris)],
                "fin": [datetime(2025, 3, 31, tzinfo=paris)],
                "turpe_fixe_eur": [50.0],
            }
        )

        energies = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["RSC1"],
                "pdl": ["PDL1"],
                "mois_annee": ["2025-03"],
                "debut": [datetime(2025, 3, 1, tzinfo=paris)],
                "fin": [datetime(2025, 3, 31, tzinfo=paris)],
                "source_avant": ["flux_C15"],
                "source_apres": ["flux_R151"],
                "data_complete": [True],
                "energie_base_kwh": [1000.0],
                "energie_hp_kwh": [500.0],
                "energie_hc_kwh": [300.0],
                "turpe_variable_eur": [25.0],
            }
        )

        meta = pipeline_facturation(abonnements, energies).collect()

        assert len(meta) == 1
        assert meta["debut_lisible"][0] == "01 mars 2025"
        assert meta["fin_lisible"][0] == "31 mars 2025"


class TestTriChronologique:
    """La motivation de l'issue #115 : mois_annee est triable chronologiquement.

    L'ancien format libellé français triait `avril 2025` avant `décembre 2024`
    (ordre alphabétique). Le test compose le pipeline abonnements (production
    réelle de mois_annee) avec l'agrégation facturation.
    """

    def test_sort_par_mois_annee_est_chronologique(self):
        from datetime import UTC

        from electricore.core.pipelines.abonnements import calculer_periodes_abonnement

        historique = pl.LazyFrame(
            {
                "ref_situation_contractuelle": ["RSC1", "RSC1", "RSC1"],
                "pdl": ["PDL1", "PDL1", "PDL1"],
                "date_evenement": [
                    datetime(2024, 12, 1, tzinfo=UTC),
                    datetime(2025, 4, 1, tzinfo=UTC),
                    datetime(2025, 5, 1, tzinfo=UTC),
                ],
                "formule_tarifaire_acheminement": ["BTINFCU4"] * 3,
                "puissance_souscrite_kva": [6.0, 6.0, 6.0],
                "impacte_abonnement": [True, True, True],
            }
        )

        # turpe_fixe_eur est normalement ajouté par ajouter_turpe_fixe entre
        # les deux pipelines — stub ici, hors du comportement testé.
        periodes = calculer_periodes_abonnement(historique).with_columns(pl.lit(10.0).alias("turpe_fixe_eur"))
        meta = agreger_abonnements_mensuel(periodes).collect()

        assert meta.sort("mois_annee")["mois_annee"].to_list() == ["2024-12", "2025-04"]
