"""
Pipeline ETL unifié pour les flux Enedis.
Combine la logique d'extraction d'electriflux avec les capacités DLT.
"""

import dlt
import yaml
from pathlib import Path
from typing import Iterator
from dlt.sources.filesystem import FileItemDict, filesystem
from xml_to_dict import xml_to_dict

# Configuration
ETL_DIR = Path(__file__).parent
CONFIG_FILE = ETL_DIR / "simple_flux.yaml"
BUCKET_URL = "file:///home/virgile/data/flux_enedis/"

# Charger la configuration YAML
with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
    FLUX_CONFIG = yaml.safe_load(f)

print(f"📋 Configuration chargée pour {len(FLUX_CONFIG)} types de flux: {list(FLUX_CONFIG.keys())}")


@dlt.transformer
def parse_flux_xml(items: Iterator[FileItemDict], flux_type: str) -> Iterator[dict]:
    """
    Transformer générique qui utilise la config YAML pour parser chaque type de flux.
    """
    config = FLUX_CONFIG[flux_type]
    count = 0
    
    print(f"🔄 Traitement flux {flux_type}...")
    
    for file_obj in items:
        try:
            with file_obj.open() as file:
                # Utiliser xml_to_dict avec la config du flux
                for record in xml_to_dict(
                    file,
                    row_level=config['row_level'],
                    metadata_fields=config.get('metadata_fields', {}),
                    data_fields=config.get('data_fields', {}),
                    nested_fields=config.get('nested_fields', [])
                ):
                    count += 1
                    
                    # Ajouter métadonnées DLT pour le suivi
                    record['_flux_type'] = flux_type
                    record['_file_name'] = file_obj['file_name']
                    record['_file_path'] = file_obj['relative_path']
                    record['_file_modified'] = file_obj['modification_date']
                    
                    # Log de progression tous les 100 enregistrements
                    if count % 100 == 0:
                        print(f"   ⏳ {flux_type}: {count} enregistrements traités")
                    
                    yield record
                    
        except Exception as e:
            print(f"❌ Erreur lors du traitement de {file_obj['file_path']}: {e}")
            continue
    
    print(f"✅ {flux_type}: {count} enregistrements au total")


@dlt.source
def flux_enedis():
    """
    Source DLT dynamique basée sur la configuration YAML.
    Crée automatiquement une ressource par type de flux configuré.
    """
    resources = []
    
    for flux_type, config in FLUX_CONFIG.items():
        print(f"🏗️  Création de la ressource pour {flux_type}")
        
        # Construire le pattern de fichiers - limiter à 1 fichier pour test initial
        if 'file_regex' in config:
            # Utiliser le regex spécifique s'il existe
            file_pattern = f"**/{config['file_regex']}"
        else:
            # Pattern par défaut basé sur le nom du flux
            file_pattern = f"{flux_type}/**/*.xml"
        
        print(f"   🔍 Pattern de fichiers: {file_pattern}")
        
        # Créer le pipeline filesystem | transformer pour ce flux
        flux_pipeline = (
            filesystem(bucket_url=BUCKET_URL, file_glob=file_pattern) 
            | parse_flux_xml(flux_type=flux_type)
        )
        
        # Le nom de la ressource est défini via with_name()
        flux_pipeline = flux_pipeline.with_name(f"flux_{flux_type.lower()}")
        
        # Déterminer la clé primaire selon le type de flux
        if flux_type in ['R15', 'R151', 'R15_ACC']:
            # Flux de relevés : PDL + Date
            primary_key = ["pdl", "Date_Releve"]
        elif flux_type in ['F12', 'F15']:
            # Flux de facturation : PDL + Facture + Element valorisé
            primary_key = ["pdl", "Num_Facture", "Id_EV"] 
        else:  # C15
            # Flux contractuels : PDL + Date événement
            primary_key = ["pdl", "Date_Evenement"]
        
        # Appliquer les hints DLT
        flux_pipeline.apply_hints(
            primary_key=primary_key,
            write_disposition="merge",  # Merge pour gérer les doublons
            incremental=dlt.sources.incremental("_file_modified")  # Incrémental sur modification
        )
        
        resources.append(flux_pipeline)
    
    return resources


def run_pipeline():
    """
    Exécute le pipeline complet et affiche les résultats.
    """
    print("🚀 Démarrage du pipeline DLT unifié...")
    
    # Créer le pipeline DLT
    pipeline = dlt.pipeline(
        pipeline_name="flux_enedis_unified",
        destination="duckdb",
        dataset_name="flux_enedis"
    )
    
    # Exécuter le chargement
    try:
        load_info = pipeline.run(flux_enedis())
        print("✅ Pipeline exécuté avec succès !")
        print(f"📊 Résultats du chargement:")
        print(load_info)
        
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution du pipeline: {e}")
        return False
    
    # Vérifier et afficher les résultats dans DuckDB
    verify_results()
    return True


def verify_results():
    """
    Vérifie et affiche le nombre d'enregistrements chargés par table.
    """
    print("\n📈 Vérification des résultats dans DuckDB...")
    
    try:
        import duckdb
        conn = duckdb.connect('flux_enedis_unified.duckdb')
        
        # Lister les tables créées
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'flux_enedis'").fetchall()
        
        if not tables:
            print("❌ Aucune table trouvée dans le schéma flux_enedis")
            return
        
        print(f"📋 Tables créées: {[t[0] for t in tables]}")
        
        # Compter les enregistrements par table
        total_records = 0
        for table_name, in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM flux_enedis.{table_name}").fetchone()[0]
                print(f"   📊 {table_name}: {count:,} enregistrements")
                total_records += count
            except Exception as e:
                print(f"   ❌ Erreur pour la table {table_name}: {e}")
        
        print(f"\n🎯 Total: {total_records:,} enregistrements chargés")
        
        # Afficher un échantillon de données
        if total_records > 0:
            print("\n🔍 Échantillon de données (première table):")
            first_table = tables[0][0]
            sample = conn.execute(f"SELECT * FROM flux_enedis.{first_table} LIMIT 3").fetchall()
            columns = [desc[0] for desc in conn.description]
            
            for row in sample:
                print(f"   {dict(zip(columns, row))}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Erreur lors de la vérification: {e}")


def count_source_files():
    """
    Compte les fichiers source disponibles par type de flux.
    """
    print("📁 Analyse des fichiers source disponibles...")
    
    base_path = Path("/home/virgile/data/flux_enedis")
    
    if not base_path.exists():
        print(f"❌ Répertoire source non trouvé: {base_path}")
        return
    
    for flux_type, config in FLUX_CONFIG.items():
        if 'file_regex' in config:
            # Utiliser le regex pour compter
            pattern = config['file_regex'].replace('\\d+', '*').replace('\\.', '.')
            files = list(base_path.glob(f"**/{pattern}"))
        else:
            # Pattern par défaut
            files = list(base_path.glob(f"{flux_type}/**/*.xml"))
        
        print(f"   📄 {flux_type}: {len(files)} fichiers trouvés")


if __name__ == "__main__":
    print("🔧 Pipeline ETL unifié pour les flux Enedis")
    print("=" * 50)
    
    # Analyser les fichiers source
    count_source_files()
    print()
    
    # Exécuter le pipeline
    success = run_pipeline()
    
    if success:
        print("\n🎉 Pipeline terminé avec succès !")
    else:
        print("\n💥 Pipeline terminé avec des erreurs")