#!/usr/bin/env python3
"""
Diagnostic méthodique de chaque flux configuré.
Vérifie la présence de fichiers et l'état du pipeline pour chaque flux.
"""

import dlt
import yaml
from pathlib import Path
from dlt.sources.filesystem import filesystem
from datetime import datetime

def diagnostic_complet():
    """Diagnostic complet de tous les flux configurés."""
    
    print("=" * 80)
    print("🔍 DIAGNOSTIC MÉTHODIQUE DES FLUX ENEDIS")
    print("=" * 80)
    print(f"📅 Date du diagnostic: {datetime.now()}")
    print()
    
    # 1. Charger la configuration
    config_path = Path("config/flux.yaml")
    with open(config_path, 'r', encoding='utf-8') as f:
        flux_config = yaml.safe_load(f)
    
    # 2. Configuration SFTP
    sftp_config = dlt.secrets['sftp']
    sftp_url = sftp_config['url']
    print(f"🌐 SFTP: {sftp_url}")
    print()
    
    # 3. Résultats du diagnostic
    resultats = {}
    
    print("=" * 80)
    print("📊 ANALYSE PAR FLUX")
    print("=" * 80)
    
    # 4. Analyser chaque flux
    for flux_name, config in flux_config.items():
        print(f"\n📁 FLUX {flux_name}")
        print("-" * 40)
        
        zip_pattern = config['zip_pattern']
        print(f"📦 Pattern ZIP: {zip_pattern}")
        
        # Compter les tables configurées
        nb_tables = 0
        if 'xml_configs' in config:
            nb_tables += len(config['xml_configs'])
            print(f"   📄 {len(config['xml_configs'])} config(s) XML")
            for xml_config in config['xml_configs']:
                print(f"      - {xml_config['name']}")
        
        if 'csv_configs' in config:
            nb_tables += len(config['csv_configs'])
            print(f"   📊 {len(config['csv_configs'])} config(s) CSV")
            for csv_config in config['csv_configs']:
                print(f"      - {csv_config['name']}")
        
        # Chercher les fichiers
        print(f"\n🔍 Recherche de fichiers...")
        try:
            files = filesystem(
                bucket_url=sftp_url,
                file_glob=zip_pattern
            )
            
            file_count = 0
            dates = []
            for file_item in files:
                file_count += 1
                dates.append(file_item['modification_date'])
                if file_count <= 3:  # Afficher les 3 premiers
                    print(f"   ✅ {file_item['file_name'][:60]}")
                    print(f"      Date: {file_item['modification_date']}")
                if file_count >= 10:  # Limiter la recherche
                    break
            
            if file_count == 0:
                print(f"   ❌ AUCUN FICHIER TROUVÉ!")
                resultats[flux_name] = {
                    'status': 'NO_FILES',
                    'file_count': 0,
                    'table_count': nb_tables
                }
            else:
                print(f"   📊 Total: {file_count}+ fichiers trouvés")
                if dates:
                    print(f"   📅 Plus ancien: {min(dates)}")
                    print(f"   📅 Plus récent: {max(dates)}")
                
                resultats[flux_name] = {
                    'status': 'OK',
                    'file_count': file_count,
                    'table_count': nb_tables,
                    'oldest': min(dates) if dates else None,
                    'newest': max(dates) if dates else None
                }
                
        except Exception as e:
            print(f"   ❌ Erreur: {e}")
            resultats[flux_name] = {
                'status': 'ERROR',
                'error': str(e),
                'table_count': nb_tables
            }
    
    # 5. Résumé
    print("\n" + "=" * 80)
    print("📈 RÉSUMÉ DU DIAGNOSTIC")
    print("=" * 80)
    
    flux_ok = [k for k, v in resultats.items() if v['status'] == 'OK']
    flux_no_files = [k for k, v in resultats.items() if v['status'] == 'NO_FILES']
    flux_error = [k for k, v in resultats.items() if v['status'] == 'ERROR']
    
    print(f"\n✅ Flux avec fichiers: {len(flux_ok)}/{len(resultats)}")
    for flux in flux_ok:
        r = resultats[flux]
        print(f"   - {flux}: {r['file_count']}+ fichiers, {r['table_count']} table(s)")
    
    if flux_no_files:
        print(f"\n❌ Flux sans fichiers: {len(flux_no_files)}")
        for flux in flux_no_files:
            print(f"   - {flux}")
    
    if flux_error:
        print(f"\n⚠️ Flux en erreur: {len(flux_error)}")
        for flux in flux_error:
            print(f"   - {flux}: {resultats[flux]['error']}")
    
    # 6. État du pipeline
    print("\n" + "=" * 80)
    print("🔧 ÉTAT DU PIPELINE")
    print("=" * 80)
    
    try:
        pipeline = dlt.pipeline('flux_enedis')
        print(f"Dataset: {pipeline.dataset_name}")
        print(f"Destination: {pipeline.destination.destination_type}")
        
        # Tables dans la base
        import duckdb
        conn = duckdb.connect('enedis_production.duckdb')
        
        # Vérifier le schéma de production
        schema = pipeline.dataset_name
        tables = conn.execute(f"""
            SELECT table_name, COUNT(*) as nb
            FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
            AND table_name NOT LIKE '_dlt%'
            GROUP BY table_name
            ORDER BY table_name
        """).fetchall()
        
        print(f"\n📊 Tables dans {schema}:")
        for table_name, _ in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]
            print(f"   - {table_name}: {count} lignes")
            
            # Vérifier quel flux correspond
            flux_found = None
            for flux_name, config in flux_config.items():
                if 'xml_configs' in config:
                    for xml_config in config['xml_configs']:
                        if xml_config['name'] == table_name:
                            flux_found = flux_name
                            break
                if 'csv_configs' in config:
                    for csv_config in config['csv_configs']:
                        if csv_config['name'] == table_name:
                            flux_found = flux_name
                            break
                if flux_found:
                    break
            
            if flux_found:
                print(f"      (depuis flux {flux_found})")
        
    except Exception as e:
        print(f"❌ Erreur pipeline: {e}")
    
    print("\n" + "=" * 80)
    print("✅ DIAGNOSTIC TERMINÉ")
    print("=" * 80)

if __name__ == "__main__":
    diagnostic_complet()