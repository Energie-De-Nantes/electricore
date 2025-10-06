"""
Fixtures pytest pour les cas métier critiques d'ElectriCore.

Ces fixtures fournissent des données réelles anonymisées couvrant les 6 cas
métier principaux identifiés pour la prévention des régressions.

Usage:
    @pytest.fixture
    def test_cas_mct_calendrier(cas_mct_changement_calendrier):
        # Test avec données réelles
        
Pour ajouter un nouveau cas :
1. Extraire les données de production 
2. Anonymiser avec le script d'anonymisation
3. Sauvegarder en parquet dans fixtures/donnees_anonymisees/
4. Créer la fixture pytest correspondante
"""

import polars as pl
import pytest
from pathlib import Path

# Chemin vers les données anonymisées
FIXTURES_DIR = Path(__file__).parent / "donnees_anonymisees"


@pytest.fixture
def cas_mct_changement_calendrier():
    """
    MCT avec changement de calendrier : passage de BASE vers HP/HC.

    Cas métier critique pour tester :
    - Continuité des index lors du changement de calendrier
    - Calcul correct des énergies avec nouveaux relevés
    - Gestion des périodes de transition

    Returns:
        dict: Contient 'historique' et 'releves' (LazyFrames)
    """
    from datetime import datetime

    # Données synthétiques réalistes pour ce cas
    historique = pl.LazyFrame({
        "pdl": ["PDL00001", "PDL00001"],
        "date_evenement": [
            datetime(2024, 1, 1, 9, 0, 0),   # MES en BASE
            datetime(2024, 6, 1, 10, 0, 0),  # MCT changement calendrier → HP/HC
        ],
        "evenement_declencheur": ["MES", "MCT"],
        "etat_contractuel": ["EN SERVICE", "EN SERVICE"],
        "puissance_souscrite": [6.0, 6.0],
        "formule_tarifaire_acheminement": ["BTINFCUST", "BTINFCUST"],
        "id_calendrier_distributeur": ["DI000001", "DI000002"],  # BASE → HP/HC
        "ref_situation_contractuelle": ["REF001", "REF001"],
    })

    releves = pl.LazyFrame({
        "pdl": ["PDL00001", "PDL00001", "PDL00001"],
        "date_releve": [
            datetime(2024, 2, 1),   # Relevé BASE
            datetime(2024, 7, 1),   # Premier relevé HP/HC après MCT
            datetime(2024, 8, 1),   # Deuxième relevé HP/HC
        ],
        "base": [1500.0, None, None],
        "hp": [None, 2000.0, 2300.0],
        "hc": [None, 1200.0, 1400.0],
        "id_calendrier_distributeur": ["DI000001", "DI000002", "DI000002"],
        "source": ["flux_R151", "flux_R151", "flux_R151"],
        "ref_situation_contractuelle": ["REF001", "REF001", "REF001"],
    })

    return {"historique": historique, "releves": releves}


@pytest.fixture
def cas_entree_perimetre():
    """
    Entrée sur le périmètre : MES, PMES, CFNE.

    Cas métier pour tester :
    - Premier relevé correct lors de l'entrée
    - Initialisation des compteurs
    - Création de la première période

    Returns:
        dict: Contient 'historique' et 'releves' (LazyFrames)
    """
    from datetime import datetime

    # 3 cas d'entrée différents
    historique = pl.LazyFrame({
        "pdl": ["PDL00001", "PDL00002", "PDL00003"],
        "date_evenement": [
            datetime(2024, 1, 15, 9, 0, 0),   # MES classique
            datetime(2024, 2, 10, 14, 30, 0), # PMES (première mise en service)
            datetime(2024, 3, 5, 11, 15, 0),  # CFNE (changement de fournisseur)
        ],
        "evenement_declencheur": ["MES", "PMES", "CFNE"],
        "etat_contractuel": ["EN SERVICE", "EN SERVICE", "EN SERVICE"],
        "puissance_souscrite": [6.0, 9.0, 3.0],
        "formule_tarifaire_acheminement": ["BTINFCUST", "BTINFCU4", "BTINFCU4"],
        "id_calendrier_distributeur": ["DI000001", "DI000002", "DI000001"],
        "ref_situation_contractuelle": ["REF001", "REF002", "REF003"],
    })

    releves = pl.LazyFrame({
        "pdl": ["PDL00001", "PDL00002", "PDL00003"],
        "date_releve": [
            datetime(2024, 2, 15),  # Premier relevé après MES
            datetime(2024, 3, 10),  # Premier relevé après PMES
            datetime(2024, 4, 5),   # Premier relevé après CFNE
        ],
        "base": [500.0, None, 300.0],
        "hp": [None, 600.0, None],
        "hc": [None, 400.0, None],
        "id_calendrier_distributeur": ["DI000001", "DI000002", "DI000001"],
        "source": ["flux_R151", "flux_R151", "flux_R151"],
        "ref_situation_contractuelle": ["REF001", "REF002", "REF003"],
    })

    return {"historique": historique, "releves": releves}


@pytest.fixture
def cas_sortie_perimetre():
    """
    Sortie du périmètre : RES, CFNS.
    
    Cas métier pour tester :
    - Dernier relevé correct lors de la sortie
    - Clôture des périodes en cours
    - Calcul final des énergies
    """
    raise pytest.skip("Fixture pas encore implémentée - données en cours d'extraction")


@pytest.fixture
def cas_changement_compteur():
    """
    Changement de compteur avec remise à zéro.
    
    Cas métier pour tester :
    - Gestion des index de départ après changement
    - Continuité des calculs d'énergie
    - Gestion des périodes avant/après
    """
    raise pytest.skip("Fixture pas encore implémentée - données en cours d'extraction")


@pytest.fixture
def cas_changement_puissance():
    """
    Modification de puissance souscrite.
    
    Cas métier pour tester :
    - Impact sur les calculs de taxes TURPE
    - Répartition des coûts avant/après changement
    - Validation des nouvelles règles tarifaires
    """
    raise pytest.skip("Fixture pas encore implémentée - données en cours d'extraction")


@pytest.fixture
def cas_changement_fta():
    """
    Changement de Formule Tarifaire Acheminement.
    
    Cas métier pour tester :
    - Nouveau calcul des coûts d'acheminement
    - Transition entre anciennes et nouvelles règles
    - Impact sur la facturation
    """
    raise pytest.skip("Fixture pas encore implémentée - données en cours d'extraction")


# =====================================================================
# FIXTURES HELPER - DONNÉES MINIMALES POUR TESTS UNITAIRES
# =====================================================================

@pytest.fixture
def releve_simple_hp_hc():
    """
    Relevé simple HP/HC pour tests unitaires d'expressions.
    
    Données créées à la main, suffisantes pour tester les transformations
    individuelles sans complexité métier.
    """
    return pl.DataFrame({
        "date_releve": ["2024-01-15T10:00:00+01:00"],
        "pdl": ["PDL00001"],
        "hp": [1000.5],
        "hc": [750.3],
        "id_calendrier_distributeur": ["DI000002"],
        "source": ["flux_R151"],
        "Unité": ["kWh"],
    })


@pytest.fixture
def releve_simple_base():
    """
    Relevé simple BASE pour tests unitaires.
    """
    return pl.DataFrame({
        "date_releve": ["2024-01-15T10:00:00+01:00"],
        "pdl": ["PDL00002"],
        "base": [1750.8],
        "id_calendrier_distributeur": ["DI000001"],
        "source": ["flux_R151"],
        "Unité": ["kWh"],
    })


@pytest.fixture
def historique_simple_mct():
    """
    Historique simple avec événement MCT pour tests unitaires.
    """
    return pl.DataFrame({
        "date_evenement": ["2024-01-10T09:00:00+01:00"],
        "pdl": ["PDL00001"],
        "evenement_declencheur": ["MCT"],
        "etat_contractuel": ["EN SERVICE"],
        "puissance_souscrite": [6.0],
        "source": ["flux_C15"],
    })