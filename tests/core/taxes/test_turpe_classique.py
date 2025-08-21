"""
Tests pour le calcul du TURPE variable dans le pipeline énergies.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from electricore.core.taxes.turpe import calculer_turpe_variable, load_turpe_rules


class TestTurpeVariable:
    
    def setup_method(self):
        """Prépare les données de test communes."""
        # Règles TURPE simplifiées pour les tests
        self.regles_turpe = pd.DataFrame({
            'Formule_Tarifaire_Acheminement': ['BTINFCU4', 'BTINFMU4', 'BTINFCUST'],
            'start': pd.to_datetime(['2023-08-01', '2023-08-01', '2023-08-01']).tz_localize('Europe/Paris'),
            'end': pd.to_datetime(['2025-12-31', '2025-12-31', '2025-12-31']).tz_localize('Europe/Paris'),
            'cg': [15.48, 15.48, 15.48],
            'cc': [19.92, 19.92, 19.92],
            'b': [9.0, 10.56, 9.96],
            'HPH': [6.67, 6.12, 0.0],
            'HCH': [4.56, 4.24, 0.0],
            'HPB': [1.43, 1.39, 0.0],
            'HCB': [0.88, 0.87, 0.0],
            'HP': [0.0, 0.0, 0.0],
            'HC': [0.0, 0.0, 0.0],
            'BASE': [0.0, 0.0, 4.37]
        })
        
        # Périodes d'énergie avec FTA (ordonnées chronologiquement pour merge_asof)
        self.periodes_avec_fta = pd.DataFrame({
            'pdl': ['PDL001', 'PDL002', 'PDL001'],  # Réorganisé pour ordre chronologique
            'debut': pd.to_datetime(['2024-01-01', '2024-01-15', '2024-02-01']).tz_localize('Europe/Paris'),
            'fin': pd.to_datetime(['2024-02-01', '2024-02-15', '2024-03-01']).tz_localize('Europe/Paris'),
            'nb_jours': [31, 31, 29],  # Réajusté selon nouveau ordre
            'source_avant': ['flux_C15', 'flux_R151', 'flux_C15'],
            'source_apres': ['flux_C15', 'flux_R151', 'flux_C15'],
            'data_complete': [True, True, True],
            'periode_irreguliere': [False, False, False],
            'BASE_energie': [1000.0, 500.0, 1200.0],  # PDL001 jan, PDL002 jan, PDL001 feb
            'HPH_energie': [300.0, 0.0, 350.0],
            'HCH_energie': [200.0, 0.0, 250.0],
            'HPB_energie': [150.0, 0.0, 180.0],
            'HCB_energie': [100.0, 0.0, 120.0],
            'HP_energie': [None, None, None],
            'HC_energie': [None, None, None],
            'Formule_Tarifaire_Acheminement': ['BTINFCU4', 'BTINFCUST', 'BTINFCU4']
        })

    def test_ajouter_turpe_variable_cas_nominal(self):
        """Test nominal : calcul TURPE variable avec énergies et FTA."""
        result = self.periodes_avec_fta.assign(
            turpe_variable=calculer_turpe_variable(self.regles_turpe, self.periodes_avec_fta)
        )
        
        # Vérifications de base
        assert 'turpe_variable' in result.columns
        assert len(result) == 3
        assert not result['turpe_variable'].isna().any()
        
        # Vérifier les calculs pour BTINFCU4 (première période PDL001 janvier)
        # HPH: 300 * 6.67 / 100 = 20.01
        # HCH: 200 * 4.56 / 100 = 9.12  
        # HPB: 150 * 1.43 / 100 = 2.145 ≈ 2.15
        # HCB: 100 * 0.88 / 100 = 0.88
        # Total attendu ≈ 32.16
        turpe_var_pdl001_jan = result[(result['pdl'] == 'PDL001') & (result['debut'].dt.month == 1)].iloc[0]['turpe_variable']
        assert abs(turpe_var_pdl001_jan - 32.16) < 0.01

    def test_ajouter_turpe_variable_base_simple(self):
        """Test avec compteur BASE simple (BTINFCUST)."""
        result = self.periodes_avec_fta.assign(
            turpe_variable=calculer_turpe_variable(self.regles_turpe, self.periodes_avec_fta)
        )
        
        # Pour PDL002 avec BTINFCUST : 500 * 4.37 / 100 = 21.85
        turpe_var_pdl002 = result[result['pdl'] == 'PDL002'].iloc[0]['turpe_variable']
        assert abs(turpe_var_pdl002 - 21.85) < 0.01

    def test_ajouter_turpe_variable_fta_manquant(self):
        """Test erreur quand FTA absent des périodes."""
        periodes_sans_fta = self.periodes_avec_fta.drop(columns=['Formule_Tarifaire_Acheminement'])
        
        with pytest.raises(ValueError, match="Formule_Tarifaire_Acheminement.*requise"):
            calculer_turpe_variable(self.regles_turpe, periodes_sans_fta)

    def test_ajouter_turpe_variable_regle_manquante(self):
        """Test erreur quand règle TURPE manquante pour un FTA."""
        periodes_fta_inconnu = self.periodes_avec_fta.copy()
        periodes_fta_inconnu.iloc[0, periodes_fta_inconnu.columns.get_loc('Formule_Tarifaire_Acheminement')] = 'FTA_INEXISTANT'
        
        with pytest.raises(ValueError, match="Règles.*manquantes pour.*FTA_INEXISTANT"):
            calculer_turpe_variable(self.regles_turpe, periodes_fta_inconnu)


class TestCalculerTurpeVariable:
    """Tests pour la fonction pure calculer_turpe_variable."""
    
    def setup_method(self):
        """Prépare les données de test communes."""
        # Règles TURPE simplifiées
        self.regles_turpe = pd.DataFrame({
            'Formule_Tarifaire_Acheminement': ['BTINFCU4', 'BTINFCUST'],
            'start': pd.to_datetime(['2023-08-01', '2023-08-01']).tz_localize('Europe/Paris'),
            'end': pd.to_datetime(['2025-12-31', '2025-12-31']).tz_localize('Europe/Paris'),
            'cg': [15.48, 15.48], 'cc': [19.92, 19.92], 'b': [9.0, 9.96],
            'HPH': [6.67, 0.0], 'HCH': [4.56, 0.0], 'HPB': [1.43, 0.0], 'HCB': [0.88, 0.0],
            'HP': [0.0, 0.0], 'HC': [0.0, 0.0], 'BASE': [0.0, 4.37]
        })
        
        # Périodes simples pour test
        self.periodes = pd.DataFrame({
            'pdl': ['PDL001', 'PDL002'],
            'debut': pd.to_datetime(['2024-01-01', '2024-01-15']).tz_localize('Europe/Paris'),
            'fin': pd.to_datetime(['2024-02-01', '2024-02-15']).tz_localize('Europe/Paris'),
            'BASE_energie': [1000.0, 500.0],
            'HPH_energie': [300.0, 0.0],
            'HCH_energie': [200.0, 0.0],
            'HPB_energie': [150.0, 0.0],
            'HCB_energie': [100.0, 0.0],
            'Formule_Tarifaire_Acheminement': ['BTINFCU4', 'BTINFCUST']
        })

    def test_calculer_turpe_variable_retourne_series(self):
        """Test que la fonction retourne bien une Series."""
        result = calculer_turpe_variable(self.regles_turpe, self.periodes)
        
        assert isinstance(result, pd.Series)
        assert result.name == 'turpe_variable'
        assert len(result) == len(self.periodes)

    def test_calculer_turpe_variable_calcul_correct(self):
        """Test que les calculs sont corrects."""
        result = calculer_turpe_variable(self.regles_turpe, self.periodes)
        
        # PDL001 (BTINFCU4): HPH=300*6.67/100 + HCH=200*4.56/100 + HPB=150*1.43/100 + HCB=100*0.88/100
        # = 20.01 + 9.12 + 2.145 + 0.88 = 32.155 ≈ 32.16
        expected_pdl001 = 32.16
        
        # PDL002 (BTINFCUST): BASE=500*4.37/100 = 21.85  
        expected_pdl002 = 21.85
        
        assert abs(result.iloc[0] - expected_pdl001) < 0.01
        assert abs(result.iloc[1] - expected_pdl002) < 0.01

    def test_calculer_turpe_variable_periodes_vides(self):
        """Test avec DataFrame vide."""
        periodes_vides = pd.DataFrame()
        result = calculer_turpe_variable(self.regles_turpe, periodes_vides)
        
        assert isinstance(result, pd.Series)
        assert len(result) == 0
        assert result.dtype == float

    def test_calculer_turpe_variable_fta_manquant(self):
        """Test erreur avec colonne FTA manquante."""
        periodes_sans_fta = self.periodes.drop(columns=['Formule_Tarifaire_Acheminement'])
        
        with pytest.raises(ValueError, match="Formule_Tarifaire_Acheminement.*requise"):
            calculer_turpe_variable(self.regles_turpe, periodes_sans_fta)

