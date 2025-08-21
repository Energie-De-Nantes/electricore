"""
Tests pour le pipeline de facturation avec méta-périodes.

Tests de l'agrégation mensuelle et de la validation mathématique
de l'équivalence avec les calculs détaillés.
"""

import pandas as pd
import pytest
import numpy as np
from datetime import datetime

from electricore.core.pipeline_facturation import (
    pipeline_facturation,
    agreger_abonnements_mensuel,
    agreger_energies_mensuel,
    joindre_agregats
)
from electricore.core.models.periode_abonnement import PeriodeAbonnement
from electricore.core.models.periode_energie import PeriodeEnergie
from electricore.core.models.periode_meta import PeriodeMeta


class TestAgregerAbonnementsMensuel:
    """Tests de l'agrégation mensuelle des abonnements."""
    
    def test_cas_simple_une_periode(self):
        """Test avec une seule période par mois."""
        data = pd.DataFrame({
            'Ref_Situation_Contractuelle': ['PDL001'],
            'pdl': ['PDL001'],
            'mois_annee': ['janvier 2024'],
            'debut': [pd.Timestamp('2024-01-01', tz='Europe/Paris')],
            'fin': [pd.Timestamp('2024-01-31', tz='Europe/Paris')],
            'debut_lisible': ['1 janvier 2024'],
            'fin_lisible': ['31 janvier 2024'],
            'nb_jours': [31],
            'Puissance_Souscrite': [6.0],
            'Formule_Tarifaire_Acheminement': ['BASE'],
            'turpe_fixe': [100.0]
        })
        
        abonnements = PeriodeAbonnement.validate(data)
        result = agreger_abonnements_mensuel(abonnements)
        
        assert len(result) == 1
        assert result.iloc[0]['puissance_moyenne'] == 6.0
        assert result.iloc[0]['nb_jours'] == 31
        assert result.iloc[0]['turpe_fixe'] == 100.0
        assert result.iloc[0]['nb_sous_periodes_abo'] == 1
        assert not result.iloc[0]['has_changement_abo']
    
    def test_cas_mct_changement_puissance(self):
        """Test avec changement de puissance (MCT) en cours de mois."""
        data = pd.DataFrame({
            'Ref_Situation_Contractuelle': ['PDL001', 'PDL001'],
            'pdl': ['PDL001', 'PDL001'],
            'mois_annee': ['janvier 2024', 'janvier 2024'],
            'debut': [
                pd.Timestamp('2024-01-01', tz='Europe/Paris'),
                pd.Timestamp('2024-01-15', tz='Europe/Paris')
            ],
            'fin': [
                pd.Timestamp('2024-01-15', tz='Europe/Paris'),
                pd.Timestamp('2024-01-31', tz='Europe/Paris')
            ],
            'debut_lisible': ['1 janvier 2024', '15 janvier 2024'],
            'fin_lisible': ['15 janvier 2024', '31 janvier 2024'],
            'nb_jours': [14, 17],
            'Puissance_Souscrite': [6.0, 9.0],
            'Formule_Tarifaire_Acheminement': ['BASE', 'BASE'],
            'turpe_fixe': [70.0, 85.0]
        })
        
        abonnements = PeriodeAbonnement.validate(data)
        result = agreger_abonnements_mensuel(abonnements)
        
        assert len(result) == 1
        
        # Vérification de la puissance moyenne pondérée
        # (6.0 * 14 + 9.0 * 17) / (14 + 17) = (84 + 153) / 31 = 237 / 31 ≈ 7.65
        puissance_attendue = (6.0 * 14 + 9.0 * 17) / 31
        assert abs(result.iloc[0]['puissance_moyenne'] - puissance_attendue) < 0.01
        
        assert result.iloc[0]['nb_jours'] == 31
        assert result.iloc[0]['turpe_fixe'] == 155.0  # 70 + 85
        assert result.iloc[0]['nb_sous_periodes_abo'] == 2
        assert result.iloc[0]['has_changement_abo']
    
    def test_dataframe_vide(self):
        """Test avec DataFrame vide."""
        abonnements = pd.DataFrame()
        result = agreger_abonnements_mensuel(abonnements)
        
        assert result.empty


class TestAgregerEnergiesMensuel:
    """Tests de l'agrégation mensuelle des énergies."""
    
    def test_cas_simple_une_periode(self):
        """Test avec une seule période par mois."""
        data = pd.DataFrame({
            'Ref_Situation_Contractuelle': ['PDL001'],
            'pdl': ['PDL001'],
            'mois_annee': ['janvier 2024'],
            'debut': [pd.Timestamp('2024-01-01', tz='Europe/Paris')],
            'fin': [pd.Timestamp('2024-01-31', tz='Europe/Paris')],
            'nb_jours': [31],
            'source_avant': ['Flux_C15'],
            'source_apres': ['Flux_C15'],
            'data_complete': [True],
            'periode_irreguliere': [False],
            'BASE_energie': [1000.0],
            'HP_energie': [600.0],
            'HC_energie': [400.0],
            'HPH_energie': [None],
            'HPB_energie': [None],
            'HCH_energie': [None],
            'HCB_energie': [None],
            'turpe_variable': [50.0]
        })
        
        energies = PeriodeEnergie.validate(data)
        result = agreger_energies_mensuel(energies)
        
        assert len(result) == 1
        assert result.iloc[0]['BASE_energie'] == 1000.0
        assert result.iloc[0]['HP_energie'] == 600.0
        assert result.iloc[0]['HC_energie'] == 400.0
        assert result.iloc[0]['turpe_variable'] == 50.0
        assert result.iloc[0]['nb_sous_periodes_energie'] == 1
        assert not result.iloc[0]['has_changement_energie']
    
    def test_cas_multiples_periodes(self):
        """Test avec plusieurs périodes d'énergie dans le mois."""
        data = pd.DataFrame({
            'Ref_Situation_Contractuelle': ['PDL001', 'PDL001'],
            'pdl': ['PDL001', 'PDL001'],
            'mois_annee': ['janvier 2024', 'janvier 2024'],
            'debut': [
                pd.Timestamp('2024-01-01', tz='Europe/Paris'),
                pd.Timestamp('2024-01-15', tz='Europe/Paris')
            ],
            'fin': [
                pd.Timestamp('2024-01-15', tz='Europe/Paris'),
                pd.Timestamp('2024-01-31', tz='Europe/Paris')
            ],
            'nb_jours': [14, 17],
            'source_avant': ['Flux_C15', 'Flux_C15'],
            'source_apres': ['Flux_C15', 'Flux_C15'],
            'data_complete': [True, True],
            'periode_irreguliere': [False, False],
            'BASE_energie': [600.0, 400.0],
            'HP_energie': [300.0, 300.0],
            'HC_energie': [300.0, 100.0],
            'HPH_energie': [None, None],
            'HPB_energie': [None, None],
            'HCH_energie': [None, None],
            'HCB_energie': [None, None],
            'turpe_variable': [30.0, 20.0]
        })
        
        energies = PeriodeEnergie.validate(data)
        result = agreger_energies_mensuel(energies)
        
        assert len(result) == 1
        assert result.iloc[0]['BASE_energie'] == 1000.0  # 600 + 400
        assert result.iloc[0]['HP_energie'] == 600.0    # 300 + 300
        assert result.iloc[0]['HC_energie'] == 400.0    # 300 + 100
        assert result.iloc[0]['turpe_variable'] == 50.0  # 30 + 20
        assert result.iloc[0]['nb_sous_periodes_energie'] == 2
        assert result.iloc[0]['has_changement_energie']


class TestJoindreAgregats:
    """Tests de la jointure des agrégats."""
    
    def test_jointure_complete(self):
        """Test avec données d'abonnement et d'énergie."""
        abo_data = pd.DataFrame({
            'Ref_Situation_Contractuelle': ['PDL001'],
            'pdl': ['PDL001'],
            'mois_annee': ['janvier 2024'],
            'nb_jours': [31],
            'puissance_moyenne': [6.5],
            'turpe_fixe': [150.0],
            'Formule_Tarifaire_Acheminement': ['BASE'],
            'debut': [pd.Timestamp('2024-01-01', tz='Europe/Paris')],
            'fin': [pd.Timestamp('2024-01-31', tz='Europe/Paris')],
            'nb_sous_periodes_abo': [2],
            'has_changement_abo': [True]
        })
        
        ener_data = pd.DataFrame({
            'Ref_Situation_Contractuelle': ['PDL001'],
            'pdl': ['PDL001'],
            'mois_annee': ['janvier 2024'],
            'BASE_energie': [1000.0],
            'HP_energie': [600.0],
            'HC_energie': [400.0],
            'turpe_variable': [50.0],
            'nb_sous_periodes_energie': [1],
            'has_changement_energie': [False]
        })
        
        result = joindre_agregats(ener_data, abo_data)
        
        assert len(result) == 1
        assert result.iloc[0]['puissance_moyenne'] == 6.5
        assert result.iloc[0]['BASE_energie'] == 1000.0
        assert result.iloc[0]['has_changement']  # True OR False = True
    
    def test_jointure_abonnement_sans_energie(self):
        """Test avec abonnement mais sans énergie (relevés manquants)."""
        abo_data = pd.DataFrame({
            'Ref_Situation_Contractuelle': ['PDL001'],
            'pdl': ['PDL001'],
            'mois_annee': ['janvier 2024'],
            'nb_jours': [31],
            'puissance_moyenne': [6.0],
            'turpe_fixe': [150.0],
            'Formule_Tarifaire_Acheminement': ['BASE'],
            'debut': [pd.Timestamp('2024-01-01', tz='Europe/Paris')],
            'fin': [pd.Timestamp('2024-01-31', tz='Europe/Paris')],
            'nb_sous_periodes_abo': [1],
            'has_changement_abo': [False]
        })
        
        ener_data = pd.DataFrame()  # Pas d'énergie
        
        result = joindre_agregats(ener_data, abo_data)
        
        assert len(result) == 1
        assert result.iloc[0]['puissance_moyenne'] == 6.0
        assert result.iloc[0]['nb_sous_periodes_energie'] == 0
        assert not result.iloc[0]['has_changement']


class TestValidationMathematique:
    """Tests de validation de l'équivalence mathématique."""
    
    def test_equivalence_puissance_moyenne(self):
        """
        Vérifie que le calcul avec puissance moyenne pondérée
        donne le même résultat que le calcul détaillé.
        """
        # Scénario : 2 périodes avec puissances différentes
        # Période 1 : 14 jours à 6 kVA
        # Période 2 : 17 jours à 9 kVA
        
        j1, j2 = 14, 17
        p1, p2 = 6.0, 9.0
        prix_commun = 2.0  # €/jour
        supplement_puissance = 0.5  # €/kVA/jour
        
        # Calcul détaillé par périodes
        prix_periode_1 = prix_commun * j1 + supplement_puissance * p1 * j1
        prix_periode_2 = prix_commun * j2 + supplement_puissance * p2 * j2
        prix_detaille = prix_periode_1 + prix_periode_2
        
        # Calcul avec puissance moyenne pondérée
        jours_total = j1 + j2
        puissance_moyenne = (p1 * j1 + p2 * j2) / jours_total
        prix_agregge = prix_commun * jours_total + supplement_puissance * puissance_moyenne * jours_total
        
        # Vérification de l'équivalence
        assert abs(prix_detaille - prix_agregge) < 0.01, \
            f"Prix détaillé: {prix_detaille}, Prix agrégé: {prix_agregge}"
        
        # Vérification des calculs intermédiaires
        assert abs(puissance_moyenne - (6.0 * 14 + 9.0 * 17) / 31) < 0.01
        
    def test_generalisation_n_periodes(self):
        """Test de l'équivalence pour n périodes quelconques."""
        # Scénario : 4 périodes avec puissances variables
        jours = [10, 8, 7, 6]
        puissances = [6.0, 9.0, 12.0, 6.0]
        prix_commun = 1.5
        supplement_puissance = 0.3
        
        # Calcul détaillé
        prix_detaille = sum(
            prix_commun * j + supplement_puissance * p * j
            for j, p in zip(jours, puissances)
        )
        
        # Calcul agrégé
        jours_total = sum(jours)
        puissance_moyenne = sum(p * j for p, j in zip(puissances, jours)) / jours_total
        prix_agregge = prix_commun * jours_total + supplement_puissance * puissance_moyenne * jours_total
        
        assert abs(prix_detaille - prix_agregge) < 0.01


class TestPipelineComplet:
    """Tests d'intégration du pipeline complet."""
    
    @pytest.fixture
    def historique_simple(self):
        """Historique simple avec MES et RES."""
        return pd.DataFrame({
            'Ref_Situation_Contractuelle': ['PDL001', 'PDL001'],
            'pdl': ['PDL001', 'PDL001'],
            'Date_Evenement': [
                pd.Timestamp('2024-01-01', tz='Europe/Paris'),
                pd.Timestamp('2024-01-31', tz='Europe/Paris')
            ],
            'Evenement_Declencheur': ['MES', 'RES'],
            'Puissance_Souscrite': [6.0, 6.0],
            'Formule_Tarifaire_Acheminement': ['BASE', 'BASE'],
            'Id_Calendrier_Distributeur': ['CAL1', 'CAL1']
        })
    
    @pytest.fixture
    def releves_simples(self):
        """Relevés simples correspondant à l'historique."""
        return pd.DataFrame({
            'pdl': ['PDL001', 'PDL001'],
            'Date_Releve': [
                pd.Timestamp('2024-01-01', tz='Europe/Paris'),
                pd.Timestamp('2024-01-31', tz='Europe/Paris')
            ],
            'BASE': [1000.0, 2000.0],
            'Source': ['Flux_C15', 'Flux_C15'],
            'Unité': ['kWh', 'kWh'],
            'Précision': ['kWh', 'kWh'],
            'ordre_index': [0, 0]
        })
    
    def test_integration_pipeline_facturation(self, historique_simple, releves_simples):
        """Test d'intégration du pipeline complet."""
        # Note: Ce test nécessite que tous les modules soient correctement intégrés
        # Pour l'instant, on teste juste que la fonction ne lève pas d'erreur
        
        try:
            from electricore.core.périmètre import HistoriquePérimètre
            from electricore.core.relevés import RelevéIndex
            
            historique = HistoriquePérimètre.validate(historique_simple)
            releves = RelevéIndex.validate(releves_simples)
            
            result = pipeline_facturation(historique, releves)
            
            # Vérifications basiques
            assert not result.empty
            assert 'puissance_moyenne' in result.columns
            assert 'turpe_fixe' in result.columns
            
        except ImportError:
            pytest.skip("Modules de dépendances non disponibles pour test d'intégration")
    
    def test_coherence_modele_periode_meta(self):
        """Test que le résultat respecte le modèle PeriodeMeta."""
        # Test avec données minimales
        data = pd.DataFrame({
            'Ref_Situation_Contractuelle': ['PDL001'],
            'pdl': ['PDL001'],
            'mois_annee': ['janvier 2024'],
            'debut': [pd.Timestamp('2024-01-01', tz='Europe/Paris')],
            'fin': [pd.Timestamp('2024-01-31', tz='Europe/Paris')],
            'nb_jours': [31],
            'puissance_moyenne': [6.0],
            'Formule_Tarifaire_Acheminement': ['BASE'],
            'turpe_fixe': [150.0],
            'BASE_energie': [1000.0],
            'HP_energie': [600.0],
            'HC_energie': [400.0],
            'turpe_variable': [50.0],
            'nb_sous_periodes_abo': [1],
            'nb_sous_periodes_energie': [1],
            'has_changement': [False],
            'debut_lisible': ['1 janvier 2024'],
            'fin_lisible': ['31 janvier 2024']
        })
        
        # Validation avec le modèle Pandera
        try:
            result = PeriodeMeta.validate(data)
            assert len(result) == 1
            assert result.iloc[0]['puissance_moyenne'] == 6.0
        except Exception as e:
            pytest.fail(f"Validation du modèle PeriodeMeta échouée: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])