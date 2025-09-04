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
        dict: Contient 'historique', 'releves' et 'expected'
    """
    # TODO: Créer les fichiers parquet après extraction des données réelles
    # return {
    #     "historique": pl.read_parquet(FIXTURES_DIR / "mct_calendrier_historique.parquet"),
    #     "releves": pl.read_parquet(FIXTURES_DIR / "mct_calendrier_releves.parquet"),
    #     "expected": pl.read_parquet(FIXTURES_DIR / "mct_calendrier_expected.parquet")
    # }
    
    # Placeholder temporaire
    raise pytest.skip("Fixture pas encore implémentée - données en cours d'extraction")


@pytest.fixture
def cas_entree_perimetre():
    """
    Entrée sur le périmètre : MES, PMES, CFNE.
    
    Cas métier pour tester :
    - Premier relevé correct lors de l'entrée
    - Initialisation des compteurs
    - Création de la première période
    """
    raise pytest.skip("Fixture pas encore implémentée - données en cours d'extraction")


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
        "Date_Releve": ["2024-01-15T10:00:00+01:00"],
        "pdl": ["PDL00001"],
        "HP": [1000.5],
        "HC": [750.3],
        "Id_Calendrier_Distributeur": ["DI000002"],
        "Source": ["flux_R151"],
        "Unité": ["kWh"],
    })


@pytest.fixture
def releve_simple_base():
    """
    Relevé simple BASE pour tests unitaires.
    """
    return pl.DataFrame({
        "Date_Releve": ["2024-01-15T10:00:00+01:00"],
        "pdl": ["PDL00002"],
        "BASE": [1750.8],
        "Id_Calendrier_Distributeur": ["DI000001"],
        "Source": ["flux_R151"],
        "Unité": ["kWh"],
    })


@pytest.fixture
def historique_simple_mct():
    """
    Historique simple avec événement MCT pour tests unitaires.
    """
    return pl.DataFrame({
        "Date_Evenement": ["2024-01-10T09:00:00+01:00"],
        "pdl": ["PDL00001"],
        "Evenement_Declencheur": ["MCT"],
        "Etat_Contractuel": ["EN SERVICE"],
        "Puissance_Souscrite": [6.0],
        "Source": ["flux_C15"],
    })