#!/usr/bin/env python3
"""
Vérifie l'état incrémental de chaque resource dans le pipeline.
"""

import dlt
import json
from datetime import datetime

def check_incremental_state():
    """Affiche l'état incrémental détaillé de chaque resource."""
    
    print("=" * 80)
    print("🔍 ÉTAT INCRÉMENTAL DU PIPELINE")
    print("=" * 80)
    
    # Charger le pipeline
    pipeline = dlt.pipeline('flux_enedis')
    state = pipeline.state
    
    print(f"Pipeline: {pipeline.pipeline_name}")
    print(f"Dataset: {pipeline.dataset_name}")
    print()
    
    # Explorer l'état des sources
    if 'sources' in state:
        for source_name, source_state in state['sources'].items():
            print(f"📦 Source: {source_name}")
            
            if 'resources' in source_state:
                resources = source_state['resources']
                
                # Afficher toutes les resources
                print(f"   Nombre de resources: {len(resources)}")
                
                for res_name, res_state in resources.items():
                    print(f"\n   📄 Resource: {res_name}")
                    
                    # Vérifier l'état incrémental
                    if 'incremental' in res_state:
                        for inc_name, inc_state in res_state['incremental'].items():
                            print(f"      🔄 Incrémental '{inc_name}':")
                            
                            start_value = inc_state.get('start_value')
                            end_value = inc_state.get('end_value')
                            last_value = inc_state.get('last_value')
                            
                            if start_value:
                                print(f"         Start: {start_value}")
                                # Si c'est une date, calculer l'âge
                                try:
                                    if isinstance(start_value, str) and 'T' in start_value:
                                        date_obj = datetime.fromisoformat(start_value.replace('Z', '+00:00'))
                                        age = datetime.now(date_obj.tzinfo) - date_obj
                                        print(f"         Âge: {age.days} jours")
                                except:
                                    pass
                            else:
                                print(f"         Start: None (pas encore initialisé)")
                            
                            if end_value:
                                print(f"         End: {end_value}")
                            
                            if last_value and last_value != start_value:
                                print(f"         Last: {last_value}")
                    else:
                        print(f"      ❌ Pas d'incrémental configuré")
            print()
    else:
        print("❌ Pas de sources dans l'état")
    
    # Afficher aussi l'état brut pour debug
    print("\n" + "=" * 80)
    print("🔧 ÉTAT BRUT (pour debug)")
    print("=" * 80)
    
    # Afficher seulement les resources avec leur incrémental
    if 'sources' in state:
        for source_name, source_state in state['sources'].items():
            if 'resources' in source_state:
                for res_name, res_state in source_state['resources'].items():
                    if 'incremental' in res_state:
                        print(f"\n{source_name}.{res_name}:")
                        print(json.dumps(res_state['incremental'], indent=2, default=str))

if __name__ == "__main__":
    check_incremental_state()