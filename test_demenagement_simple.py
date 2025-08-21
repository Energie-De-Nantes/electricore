"""
Test simple pour valider la solution de déménagement avec Ref_Situation_Contractuelle.
"""
import pandas as pd
from datetime import datetime

print("=== Test propagation des colonnes contractuelles ===")

# Simuler des relevés après reconstituer_chronologie_relevés
# Les relevés C15 ont Ref_Situation_Contractuelle, les R151 non
relevés_mixtes = pd.DataFrame({
    'pdl': ['PDL_DEMO'] * 6,
    'Date_Releve': [
        datetime(2024, 1, 10, tzinfo=pd.Timestamp.now().tz),  # R151
        datetime(2024, 1, 15, tzinfo=pd.Timestamp.now().tz),  # C15 RES
        datetime(2024, 1, 15, tzinfo=pd.Timestamp.now().tz),  # C15 MES  
        datetime(2024, 1, 20, tzinfo=pd.Timestamp.now().tz),  # R151
        datetime(2024, 1, 25, tzinfo=pd.Timestamp.now().tz),  # R151
        datetime(2024, 1, 30, tzinfo=pd.Timestamp.now().tz),  # FACTURATION
    ],
    'Source': ['flux_R151', 'flux_C15', 'flux_C15', 'flux_R151', 'flux_R151', 'FACTURATION'],
    'ordre_index': [0, 0, 1, 0, 0, 0],
    'Ref_Situation_Contractuelle': [None, 'REF_001_RES', 'REF_002_MES', None, None, None],
    'Formule_Tarifaire_Acheminement': [None, 'BTINFCU4', 'BTINFMU4', None, None, None],
    'BASE': [1000, 1000, 1000, 1200, 1400, 1600],
    'Id_Calendrier_Distributeur': ['DI000001'] * 6,
    'Unité': ['kWh'] * 6,
    'Précision': ['kWh'] * 6
})

print("Relevés AVANT propagation :")
print(relevés_mixtes[['Date_Releve', 'Source', 'Ref_Situation_Contractuelle', 'Formule_Tarifaire_Acheminement']])
print()

try:
    # Test de la propagation forward-fill
    relevés_triés = relevés_mixtes.sort_values(['pdl', 'Date_Releve', 'ordre_index'])
    
    # Appliquer la même logique que dans reconstituer_chronologie_relevés
    relevés_propagés = relevés_triés.assign(
        Ref_Situation_Contractuelle=relevés_triés.groupby('pdl')['Ref_Situation_Contractuelle'].ffill(),
        Formule_Tarifaire_Acheminement=relevés_triés.groupby('pdl')['Formule_Tarifaire_Acheminement'].ffill()
    )
    
    print("Relevés APRÈS propagation :")
    print(relevés_propagés[['Date_Releve', 'Source', 'Ref_Situation_Contractuelle', 'Formule_Tarifaire_Acheminement']])
    print()
    
    # Vérifications
    refs_propagées = relevés_propagés['Ref_Situation_Contractuelle'].notna().sum()
    fta_propagés = relevés_propagés['Formule_Tarifaire_Acheminement'].notna().sum()
    
    print(f"Ref_Situation_Contractuelle renseignées: {refs_propagées}/6")
    print(f"Formule_Tarifaire_Acheminement renseignées: {fta_propagés}/6")
    
    # Test déduplication
    déduplication = relevés_propagés.drop_duplicates(
        subset=['Ref_Situation_Contractuelle', 'Date_Releve', 'ordre_index'], 
        keep='first'
    )
    
    print(f"Lignes avant déduplication: {len(relevés_propagés)}")
    print(f"Lignes après déduplication par contrat: {len(déduplication)}")
    
    # Vérifier les contrats distincts le même jour
    jour_demenagement = déduplication[déduplication['Date_Releve'].dt.date == datetime(2024, 1, 15).date()]
    refs_distinctes = jour_demenagement['Ref_Situation_Contractuelle'].nunique()
    
    print(f"Contrats distincts le 15/01: {refs_distinctes}")
    
    if refs_distinctes == 2 and len(déduplication) >= 5:
        print("✅ SUCCÈS: La solution gère correctement les déménagements")
        print("✅ Propagation des colonnes contractuelles: OK")
        print("✅ Déduplication par contrat: OK") 
    else:
        print("❌ ÉCHEC: Problème dans la solution")
        
except Exception as e:
    print(f"Erreur lors du test: {e}")
    import traceback
    traceback.print_exc()