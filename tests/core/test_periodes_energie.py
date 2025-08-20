import pytest
import pandas as pd
import numpy as np
from hypothesis import given, strategies as st, assume
from hypothesis.extra.pandas import data_frames, columns
from pandera.typing import DataFrame

from electricore.core.pipeline_energie import (
    calculer_periodes_energie,
    enrichir_cadrans_principaux
)
from electricore.core.models.periode_energie import PeriodeEnergie
from electricore.core.relevés.modèles import RelevéIndex
from electricore.core.périmètre.modèles import HistoriquePérimètre
# from electricore.core.services import generer_periodes_energie  # Function doesn't exist


# === STRATÉGIES HYPOTHESIS ===

@st.composite
def releves_index_strategy(draw):
    """Génère des DataFrames de relevés d'index valides selon le modèle RelevéIndex."""
    from electricore.core.relevés.modèles import RelevéIndex
    
    # Générer les paramètres de base
    n_pdls = draw(st.integers(min_value=1, max_value=3))
    # PDL = entier de 14 chiffres
    pdls = [str(draw(st.integers(min_value=10**13, max_value=10**14-1))) for _ in range(n_pdls)]
    
    # Dates mensuelles (premiers du mois)
    start_date = pd.Timestamp("2024-01-01", tz="Europe/Paris")
    n_mois = draw(st.integers(min_value=3, max_value=12))
    dates = pd.date_range(start=start_date, periods=n_mois, freq='MS')
    
    # Construire les données minimales que RelevéIndex peut valider
    rows = []
    for pdl in pdls:
        base_index = draw(st.integers(min_value=1000, max_value=50000))
        for i, date in enumerate(dates):
            consumption = draw(st.integers(min_value=100, max_value=2000))
            rows.append({
                'pdl': pdl,
                'Date_Releve': date,
                'Id_Calendrier_Distributeur': 'DI000001',  # Calendrier BASE uniquement
                'BASE': base_index + i * consumption,
                'HP': None,  # Toutes les colonnes de cadrans requises
                'HC': None,
                'HPH': None,
                'HPB': None,
                'HCH': None,
                'HCB': None,
                'Source': 'flux_R151',
                'Unité': 'kWh',
                'Précision': 'kWh',
            })
    
    # Créer le DataFrame et le valider avec Pandera
    df = pd.DataFrame(rows)
    return RelevéIndex.validate(df)


@st.composite 
def historique_evenements_strategy(draw):
    """Génère un historique d'événements pour tests."""
    n_events = draw(st.integers(min_value=1, max_value=5))
    pdl = f"PDL{draw(st.integers(min_value=1, max_value=3)):03d}"
    
    data = []
    base_date = pd.Timestamp("2024-06-15", tz="Europe/Paris")
    
    for i in range(n_events):
        data.append({
            'Ref_Situation_Contractuelle': f"REF_{pdl}_{i}",
            'pdl': pdl,
            'Date_Evenement': base_date + pd.Timedelta(days=i*30),
            'impact_energie': True,
            'impact_turpe_variable': False,
            'Evenement_Declencheur': 'MES',
            'Type_Evenement': 'contractuel',
            'Segment_Clientele': 'C5',
            'Etat_Contractuel': 'EN SERVICE',
            'Puissance_Souscrite': 6.0,
            'Formule_Tarifaire_Acheminement': 'BTINFCUST',
            'Type_Compteur': 'ELEC',
            'Num_Compteur': f"CPT_{i}",
            'Categorie': 'C5',
            'Ref_Demandeur': 'REF001',
            'Id_Affaire': 'AFF001',
        })
        
    return pd.DataFrame(data)


# === TESTS UNITAIRES ===

def create_valid_releves_data():
    """Helper pour créer des données de relevés valides."""
    from electricore.core.relevés.modèles import RelevéIndex
    
    # Créer d'abord un DataFrame minimal avec PDL de 14 chiffres
    data = pd.DataFrame({
        'pdl': ['12345678901234', '12345678901234', '12345678901234'],
        'Date_Releve': [
            pd.Timestamp("2024-01-01", tz="Europe/Paris"),  # Premier du mois
            pd.Timestamp("2024-01-15", tz="Europe/Paris"),  # Milieu du mois
            pd.Timestamp("2024-02-01", tz="Europe/Paris"),  # Premier du mois
        ],
        'Id_Calendrier_Distributeur': ['DI000001'] * 3,
        'BASE': [1000.0, 1500.0, 2000.0],  # Float pour éviter les erreurs de type
        'HP': [None, None, None],  # Toutes les colonnes de cadrans requises
        'HC': [None, None, None],
        'HPH': [None, None, None],
        'HPB': [None, None, None],
        'HCH': [None, None, None],
        'HCB': [None, None, None],
        'Source': ['flux_R151'] * 3,
        'Unité': ['kWh'] * 3,
        'Précision': ['kWh'] * 3,
    })
    
    # Utiliser le modèle pour valider et compléter les données
    return RelevéIndex.validate(data)


# def test_extraire_releves_mensuels_basic():
#     """Test basique de l'extraction des relevés mensuels."""
#     data = create_valid_releves_data()
#     
#     result = extraire_releves_mensuels(data)
#     
#     # Vérifications
#     assert len(result) == 2  # Seulement les premiers du mois
#     assert all(result['Date_Releve'].dt.day == 1)
#     assert all(result['source'] == 'regular')
#     assert all(result['ordre_index'] == 0)


def test_calculer_periodes_energie_coherence():
    """Test de cohérence : les énergies calculées correspondent aux différences d'index."""
    # Données de test simples conformes au modèle RelevéIndex
    releves_data = pd.DataFrame({
        'pdl': ['12345678901234', '12345678901234', '12345678901234'],
        'Date_Releve': [
            pd.Timestamp("2024-01-01", tz="Europe/Paris"),
            pd.Timestamp("2024-02-01", tz="Europe/Paris"),
            pd.Timestamp("2024-03-01", tz="Europe/Paris"),
        ],
        'BASE': [1000.0, 1500.0, 2200.0],
        'HP': [600.0, 900.0, 1320.0],
        'HC': [400.0, 600.0, 880.0],
        'HPH': [None, None, None],
        'HPB': [None, None, None],
        'HCH': [None, None, None],
        'HCB': [None, None, None],
        'Source': ['flux_R151'] * 3,
        'Unité': ['kWh'] * 3,
        'Précision': ['kWh'] * 3,
        'Id_Calendrier_Distributeur': ['DI000001'] * 3,
        'ordre_index': [0] * 3
    })
    
    result = calculer_periodes_energie(releves_data)
    
    # Vérifications de cohérence
    assert len(result) == 2  # 2 périodes entre 3 relevés
    
    # Test énergie BASE
    assert result.iloc[0]['BASE_energie'] == 500  # 1500 - 1000
    assert result.iloc[1]['BASE_energie'] == 700  # 2200 - 1500
    
    # Test flags qualité
    assert all(result['data_complete'] == True)
    assert all(result['duree_jours'] > 0)


@given(st.integers(min_value=28, max_value=35))
def test_periode_irreguliere_detection(duree):
    """Test de détection des périodes irrégulières (>35 jours)."""
    base_date = pd.Timestamp("2024-01-01", tz="Europe/Paris")
    releves_data = pd.DataFrame({
        'pdl': ['12345678901234', '12345678901234'],
        'Date_Releve': [base_date, base_date + pd.Timedelta(days=duree)],
        'BASE': [1000.0, 1500.0],
        'HP': [None, None],
        'HC': [None, None],
        'HPH': [None, None],
        'HPB': [None, None],
        'HCH': [None, None],
        'HCB': [None, None],
        'Source': ['flux_R151'] * 2,
        'Unité': ['kWh'] * 2,
        'Précision': ['kWh'] * 2,
        'Id_Calendrier_Distributeur': ['DI000001'] * 2,
        'ordre_index': [0] * 2
    })
    
    result = calculer_periodes_energie(releves_data)
    
    # Propriété : période irrégulière ssi durée > 35
    expected_irregular = duree > 35
    assert result.iloc[0]['periode_irreguliere'] == expected_irregular
    assert result.iloc[0]['duree_jours'] == duree


def test_periodes_sans_chevauchement():
    """Test de propriété : les périodes générées ne se chevauchent jamais."""
    releves_data = pd.DataFrame({
        'pdl': ['12345678901234'] * 5,
        'Date_Releve': pd.date_range(
            start="2024-01-01", 
            periods=5, 
            freq='MS',
            tz="Europe/Paris"
        ),
        'BASE': [1000.0, 1500.0, 2200.0, 3000.0, 3800.0],
        'HP': [None] * 5,
        'HC': [None] * 5,
        'HPH': [None] * 5,
        'HPB': [None] * 5,
        'HCH': [None] * 5,
        'HCB': [None] * 5,
        'Source': ['flux_R151'] * 5,
        'Unité': ['kWh'] * 5,
        'Précision': ['kWh'] * 5,
        'Id_Calendrier_Distributeur': ['DI000001'] * 5,
        'ordre_index': [0] * 5
    })
    
    result = calculer_periodes_energie(releves_data)
    
    # Propriété : pas de chevauchement
    for i in range(len(result) - 1):
        assert result.iloc[i]['Date_Fin'] <= result.iloc[i + 1]['Date_Debut']


def test_conservation_energie_totale():
    """Test de propriété : la somme des énergies égale la différence totale des index."""
    releves_data = pd.DataFrame({
        'pdl': ['12345678901234'] * 4,
        'Date_Releve': pd.date_range(
            start="2024-01-01",
            periods=4,
            freq='MS', 
            tz="Europe/Paris"
        ),
        'BASE': [1000.0, 1500.0, 2200.0, 3000.0],
        'HP': [None] * 4,
        'HC': [None] * 4,
        'HPH': [None] * 4,
        'HPB': [None] * 4,
        'HCH': [None] * 4,
        'HCB': [None] * 4,
        'Source': ['flux_R151'] * 4,
        'Unité': ['kWh'] * 4,
        'Précision': ['kWh'] * 4,
        'Id_Calendrier_Distributeur': ['DI000001'] * 4,
        'ordre_index': [0] * 4
    })
    
    result = calculer_periodes_energie(releves_data)
    
    # Propriété : conservation de l'énergie
    energie_totale = result['BASE_energie'].sum()
    difference_index = releves_data['BASE'].iloc[-1] - releves_data['BASE'].iloc[0]
    
    assert energie_totale == difference_index


# === TESTS D'INTÉGRATION ===

@pytest.mark.integration
def test_pipeline_complet_minimal():
    """Test d'intégration du pipeline complet avec données minimales."""
    # Historique minimal
    historique = pd.DataFrame({
        'Ref_Situation_Contractuelle': ['REF001'],
        'pdl': ['12345678901234'],
        'Date_Evenement': [pd.Timestamp("2024-01-01", tz="Europe/Paris")],
        'impact_energie': [False],
        'impact_turpe_variable': [False],
        'impact_turpe_fixe': [True],
        'Evenement_Declencheur': ['MES'],
        'Type_Evenement': ['contractuel'],
        'Segment_Clientele': ['C5'],
        'Etat_Contractuel': ['EN SERVICE'],
        'Puissance_Souscrite': [6.0],
        'Formule_Tarifaire_Acheminement': ['BTINFCUST'],
        'Type_Compteur': ['ELEC'],
        'Num_Compteur': ['CPT001'],
        'Categorie': ['C5'],
        'Ref_Demandeur': ['REF001'],
        'Id_Affaire': ['AFF001'],
    })
    
    # Relevés minimaux
    releves = pd.DataFrame({
        'pdl': ['12345678901234'] * 3,
        'Date_Releve': pd.date_range(
            start="2024-01-01",
            periods=3,
            freq='MS',
            tz="Europe/Paris"
        ),
        'Id_Calendrier_Distributeur': ['DI000001'] * 3,
        'BASE': [1000.0, 1500.0, 2000.0],
        'HP': [600.0, 900.0, 1200.0],
        'HC': [400.0, 600.0, 800.0],
        'HPH': [None] * 3,
        'HPB': [None] * 3,
        'HCH': [None] * 3,
        'HCB': [None] * 3,
        'Source': ['flux_R151'] * 3,
        'Unité': ['kWh'] * 3,
        'Précision': ['kWh'] * 3
    })
    
    # Vérifier que le pipeline ne plante pas
    # try:
    #     result = generer_periodes_energie(historique, releves)
    #     assert isinstance(result, pd.DataFrame)
    #     # Le résultat peut être vide si pas d'événements impactant l'énergie
    #     
    # except Exception as e:
    #     pytest.fail(f"Pipeline failed with error: {e}")
    
    # Test simplifié : juste valider que notre fonction ne plante pas
    try:
        result = calculer_periodes_energie(releves)
        assert isinstance(result, pd.DataFrame)
    except Exception as e:
        pytest.fail(f"calculer_periodes_energie failed with error: {e}")


# === TESTS D'ENRICHISSEMENT DES CADRANS ===

def test_enrichissement_sous_cadrans_vers_principaux():
    """Test de synthèse des sous-cadrans vers HC et HP."""
    # Données avec seulement les sous-cadrans
    data = pd.DataFrame({
        'pdl': ['test_pdl'],
        'Date_Debut': [pd.Timestamp("2024-01-01", tz="Europe/Paris")],
        'Date_Fin': [pd.Timestamp("2024-02-01", tz="Europe/Paris")],
        'duree_jours': [31],
        'source_avant': ['flux_R151'],
        'source_apres': ['flux_R151'],
        'data_complete': [True],
        'periode_irreguliere': [False],
        'HPH_energie': [100.0],
        'HPB_energie': [200.0],
        'HCH_energie': [150.0],
        'HCB_energie': [250.0],
        'BASE_energie': [np.nan]  # Pas de BASE initial
    })
    
    result = enrichir_cadrans_principaux(data)
    
    # Vérifications
    assert result.iloc[0]['HP_energie'] == 300.0  # 100 + 200
    assert result.iloc[0]['HC_energie'] == 400.0  # 150 + 250
    assert result.iloc[0]['BASE_energie'] == 700.0  # 300 + 400


def test_enrichissement_cadrans_principaux_vers_base():
    """Test de synthèse de HP et HC vers BASE."""
    data = pd.DataFrame({
        'pdl': ['test_pdl'],
        'Date_Debut': [pd.Timestamp("2024-01-01", tz="Europe/Paris")],
        'Date_Fin': [pd.Timestamp("2024-02-01", tz="Europe/Paris")],
        'duree_jours': [31],
        'source_avant': ['flux_R151'],
        'source_apres': ['flux_R151'],
        'data_complete': [True],
        'periode_irreguliere': [False],
        'HP_energie': [500.0],
        'HC_energie': [600.0],
        'BASE_energie': [np.nan]  # Pas de BASE initial
    })
    
    result = enrichir_cadrans_principaux(data)
    
    # Vérifications
    assert result.iloc[0]['HP_energie'] == 500.0  # Inchangé
    assert result.iloc[0]['HC_energie'] == 600.0  # Inchangé  
    assert result.iloc[0]['BASE_energie'] == 1100.0  # 500 + 600


def test_enrichissement_avec_valeurs_partielles_nan():
    """Test avec des valeurs NaN partielles."""
    data = pd.DataFrame({
        'pdl': ['test_pdl'],
        'Date_Debut': [pd.Timestamp("2024-01-01", tz="Europe/Paris")],
        'Date_Fin': [pd.Timestamp("2024-02-01", tz="Europe/Paris")],
        'duree_jours': [31],
        'source_avant': ['flux_R151'],
        'source_apres': ['flux_R151'],
        'data_complete': [True],
        'periode_irreguliere': [False],
        'HPH_energie': [100.0],
        'HPB_energie': [np.nan],  # NaN partiel
        'HCH_energie': [np.nan],  # NaN partiel
        'HCB_energie': [200.0],
        'BASE_energie': [np.nan]
    })
    
    result = enrichir_cadrans_principaux(data)
    
    # Vérifications : min_count=1 doit gérer les NaN correctement
    assert result.iloc[0]['HP_energie'] == 100.0  # Seulement HPH
    assert result.iloc[0]['HC_energie'] == 200.0  # Seulement HCB
    assert result.iloc[0]['BASE_energie'] == 300.0  # 100 + 200


def test_enrichissement_toutes_valeurs_nan():
    """Test avec toutes les valeurs NaN."""
    data = pd.DataFrame({
        'pdl': ['test_pdl'],
        'Date_Debut': [pd.Timestamp("2024-01-01", tz="Europe/Paris")],
        'Date_Fin': [pd.Timestamp("2024-02-01", tz="Europe/Paris")],
        'duree_jours': [31],
        'source_avant': ['flux_R151'],
        'source_apres': ['flux_R151'],
        'data_complete': [False],
        'periode_irreguliere': [False],
        'HPH_energie': [np.nan],
        'HPB_energie': [np.nan],
        'HCH_energie': [np.nan],
        'HCB_energie': [np.nan],
        'BASE_energie': [np.nan]
    })
    
    result = enrichir_cadrans_principaux(data)
    
    # Vérifications : tout doit rester NaN
    assert pd.isna(result.iloc[0]['HP_energie'])
    assert pd.isna(result.iloc[0]['HC_energie'])
    assert pd.isna(result.iloc[0]['BASE_energie'])


def test_enrichissement_base_existant():
    """Test quand BASE existe déjà avec HP et HC."""
    data = pd.DataFrame({
        'pdl': ['test_pdl'],
        'Date_Debut': [pd.Timestamp("2024-01-01", tz="Europe/Paris")],
        'Date_Fin': [pd.Timestamp("2024-02-01", tz="Europe/Paris")],
        'duree_jours': [31],
        'source_avant': ['flux_R151'],
        'source_apres': ['flux_R151'],
        'data_complete': [True],
        'periode_irreguliere': [False],
        'HP_energie': [500.0],
        'HC_energie': [600.0],
        'BASE_energie': [50.0]  # BASE existant (cas théorique)
    })
    
    result = enrichir_cadrans_principaux(data)
    
    # Vérifications : BASE enrichi = BASE + HP + HC
    assert result.iloc[0]['BASE_energie'] == 1150.0  # 50 + 500 + 600


def test_enrichissement_integration_pipeline():
    """Test d'intégration de l'enrichissement dans le pipeline complet."""
    # Données de test avec sous-cadrans uniquement
    releves_data = pd.DataFrame({
        'pdl': ['12345678901234', '12345678901234'],
        'Date_Releve': [
            pd.Timestamp("2024-01-01", tz="Europe/Paris"),
            pd.Timestamp("2024-02-01", tz="Europe/Paris"),
        ],
        'BASE': [np.nan, np.nan],  # Pas de BASE dans les relevés
        'HP': [np.nan, np.nan],    # Pas de HP dans les relevés
        'HC': [np.nan, np.nan],    # Pas de HC dans les relevés
        'HPH': [600.0, 900.0],     # Seulement les sous-cadrans
        'HPB': [400.0, 600.0],
        'HCH': [300.0, 450.0],
        'HCB': [200.0, 300.0],
        'Source': ['flux_R151'] * 2,
        'Unité': ['kWh'] * 2,
        'Précision': ['kWh'] * 2,
        'Id_Calendrier_Distributeur': ['DI000001'] * 2,
        'ordre_index': [0] * 2
    })
    
    result = calculer_periodes_energie(releves_data)
    
    # Vérifications de l'enrichissement
    assert len(result) == 1  # Une période entre deux relevés
    
    # Vérifier que HP a été synthétisé depuis HPH et HPB
    hp_attendu = (900.0 + 600.0) - (600.0 + 400.0)  # 300
    assert result.iloc[0]['HP_energie'] == hp_attendu
    
    # Vérifier que HC a été synthétisé depuis HCH et HCB  
    hc_attendu = (450.0 + 300.0) - (300.0 + 200.0)  # 250
    assert result.iloc[0]['HC_energie'] == hc_attendu
    
    # Vérifier que BASE a été synthétisé depuis HP et HC
    base_attendu = hp_attendu + hc_attendu  # 550
    assert result.iloc[0]['BASE_energie'] == base_attendu


if __name__ == "__main__":
    pytest.main([__file__, "-v"])