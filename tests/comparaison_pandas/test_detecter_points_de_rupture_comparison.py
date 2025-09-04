"""
Tests de comparaison temporaires entre les versions pandas et Polars.

Ces tests valident l'équivalence entre l'ancienne version pandas et la nouvelle
version Polars de detecter_points_de_rupture. Ils peuvent être supprimés une fois
la migration validée.
"""

import pandas as pd
import polars as pl
import pytest
from electricore.core.pipeline_perimetre import detecter_points_de_rupture as detecter_pandas
from electricore.core.pipelines_polars.perimetre_polars import detecter_points_de_rupture as detecter_polars


def test_comparaison_pandas_polars_cas_simple():
    """Compare pandas vs polars sur un cas simple avec quelques événements."""
    # Données communes (format compatible pandas/polars)
    data_communes = {
        "Ref_Situation_Contractuelle": ["PDL1", "PDL1", "PDL1", "PDL1"],
        "Date_Evenement": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-15", "2024-04-01"]),
        "Evenement_Declencheur": ["MES", "FACTURATION", "MCT", "RES"],
        "Puissance_Souscrite": [6.0, 6.0, 9.0, 9.0],
        "Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4"],
        
        # Colonnes obligatoires du schéma HistoriquePérimètre
        "pdl": ["PDL001", "PDL001", "PDL001", "PDL001"],
        "Segment_Clientele": ["C5", "C5", "C5", "C5"],
        "Etat_Contractuel": ["ACTIF", "ACTIF", "ACTIF", "RESILIE"],
        "Type_Evenement": ["reel", "artificiel", "reel", "reel"],
        "Categorie": ["PRO", "PRO", "PRO", "PRO"],
        "Type_Compteur": ["ELEC", "ELEC", "ELEC", "ELEC"],
        "Num_Compteur": ["12345", "12345", "12345", "12345"],
        "Ref_Demandeur": ["REF001", "REF001", "REF001", "REF001"],
        "Id_Affaire": ["AFF001", "AFF001", "AFF001", "AFF001"],
        
        # Colonnes Avant_/Après_ pour calendrier et index
        "Avant_Id_Calendrier_Distributeur": ["CAL1", "CAL1", "CAL1", "CAL2"],
        "Après_Id_Calendrier_Distributeur": ["CAL1", "CAL1", "CAL2", "CAL2"],
        "Avant_BASE": [1000.0, 1200.0, 1500.0, 1800.0],
        "Après_BASE": [1000.0, 1200.0, 1550.0, 1800.0],  # Changement ligne 3
        "Avant_HP": [None, None, None, None],
        "Après_HP": [None, None, None, None],
        "Avant_HC": [None, None, None, None],
        "Après_HC": [None, None, None, None],
        "Avant_HPH": [None, None, None, None],
        "Après_HPH": [None, None, None, None],
        "Avant_HCH": [None, None, None, None],
        "Après_HCH": [None, None, None, None],
        "Avant_HPB": [None, None, None, None],
        "Après_HPB": [None, None, None, None],
        "Avant_HCB": [None, None, None, None],
        "Après_HCB": [None, None, None, None],
    }
    
    # Version pandas
    df_pandas = pd.DataFrame(data_communes)
    resultat_pandas = detecter_pandas(df_pandas)
    
    # Version Polars  
    df_polars = pl.LazyFrame(data_communes)
    resultat_polars = detecter_polars(df_polars).collect()
    
    # Comparaison des colonnes clés
    colonnes_a_comparer = ["impacte_abonnement", "impacte_energie"]
    
    for col in colonnes_a_comparer:
        pandas_values = resultat_pandas[col].tolist()
        polars_values = resultat_polars[col].to_list()
        
        assert pandas_values == polars_values, f"Différence détectée sur {col}:\nPandas: {pandas_values}\nPolars: {polars_values}"
    
    print(f"✅ Comparaison pandas vs polars réussie:")
    print(f"- Impacts abonnement identiques: {resultat_pandas['impacte_abonnement'].tolist()}")
    print(f"- Impacts énergie identiques: {resultat_pandas['impacte_energie'].tolist()}")


def test_comparaison_pandas_polars_cas_complexe():
    """Compare pandas vs polars sur un cas plus complexe avec plusieurs PDL."""
    # Données plus complexes avec 2 PDL
    data_complexes = {
        "Ref_Situation_Contractuelle": ["PDL1", "PDL1", "PDL1", "PDL2", "PDL2", "PDL2"],
        "Date_Evenement": pd.to_datetime([
            "2024-01-01", "2024-02-15", "2024-03-01", 
            "2024-01-15", "2024-02-01", "2024-04-15"
        ]),
        "Evenement_Declencheur": ["CFNE", "MCT", "FACTURATION", "MES", "FACTURATION", "CFNS"],
        "Puissance_Souscrite": [9.0, 12.0, 12.0, 6.0, 6.0, 6.0],
        "Formule_Tarifaire_Acheminement": [
            "BTINFMU4", "BTINFMU4", "BTINFMU4", 
            "BTINFCU4", "BTINFCU4", "BTINFCU4"
        ],
        
        # Colonnes obligatoires du schéma
        "pdl": ["PDL001", "PDL001", "PDL001", "PDL002", "PDL002", "PDL002"],
        "Segment_Clientele": ["C5", "C5", "C5", "C4", "C4", "C4"],
        "Etat_Contractuel": ["ACTIF", "ACTIF", "ACTIF", "ACTIF", "ACTIF", "RESILIE"],
        "Type_Evenement": ["reel", "reel", "artificiel", "reel", "artificiel", "reel"],
        "Categorie": ["PRO", "PRO", "PRO", "PART", "PART", "PART"],
        "Type_Compteur": ["ELEC", "ELEC", "ELEC", "ELEC", "ELEC", "ELEC"],
        "Num_Compteur": ["12345", "12345", "12345", "67890", "67890", "67890"],
        "Ref_Demandeur": ["REF001", "REF001", "REF001", "REF002", "REF002", "REF002"],
        "Id_Affaire": ["AFF001", "AFF001", "AFF001", "AFF002", "AFF002", "AFF002"],
        
        # Colonnes Avant_/Après_
        "Avant_Id_Calendrier_Distributeur": ["CAL1", "CAL1", "CAL2", "CAL3", "CAL3", "CAL3"],
        "Après_Id_Calendrier_Distributeur": ["CAL1", "CAL2", "CAL2", "CAL3", "CAL3", "CAL3"],
        "Avant_BASE": [1000.0, 1300.0, 1600.0, 2000.0, 2200.0, 2500.0],
        "Après_BASE": [1000.0, 1350.0, 1600.0, 2000.0, 2200.0, 2500.0],  # Changement PDL1 ligne 2
        "Avant_HP": [None, None, None, None, None, None],
        "Après_HP": [None, None, None, None, None, None],
        "Avant_HC": [None, None, None, None, None, None],
        "Après_HC": [None, None, None, None, None, None],
        "Avant_HPH": [None, None, None, None, None, None],
        "Après_HPH": [None, None, None, None, None, None],
        "Avant_HCH": [None, None, None, None, None, None],
        "Après_HCH": [None, None, None, None, None, None],
        "Avant_HPB": [None, None, None, None, None, None],
        "Après_HPB": [None, None, None, None, None, None],
        "Avant_HCB": [None, None, None, None, None, None],
        "Après_HCB": [None, None, None, None, None, None],
    }
    
    # Version pandas
    df_pandas = pd.DataFrame(data_complexes)
    resultat_pandas = detecter_pandas(df_pandas)
    
    # Version Polars  
    df_polars = pl.LazyFrame(data_complexes)
    resultat_polars = detecter_polars(df_polars).collect()
    
    # Comparaison
    for col in ["impacte_abonnement", "impacte_energie"]:
        pandas_values = resultat_pandas[col].tolist()
        polars_values = resultat_polars[col].to_list()
        
        assert pandas_values == polars_values, f"Différence sur {col}:\nPandas: {pandas_values}\nPolars: {polars_values}"
    
    # Vérifications spécifiques
    assert sum(resultat_pandas["impacte_abonnement"]) == sum(resultat_polars["impacte_abonnement"].to_list())
    assert sum(resultat_pandas["impacte_energie"]) == sum(resultat_polars["impacte_energie"].to_list())
    
    print(f"✅ Cas complexe validé - {len(data_complexes['Ref_Situation_Contractuelle'])} événements sur 2 PDL")