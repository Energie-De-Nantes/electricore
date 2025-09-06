"""
Pipeline ETL SFTP avec déchiffrement AES - Version multi-ressources.
Crée une ressource DLT par type de flux pour des tables séparées.
"""

import dlt
import tempfile
import zipfile
import yaml
from pathlib import Path
from typing import Iterator
from Crypto.Cipher import AES
from dlt.sources.filesystem import filesystem, FileItemDict
from xml_to_dict import xml_to_dict

# Configuration
ETL_DIR = Path(__file__).parent
CONFIG_FILE = ETL_DIR / "simple_flux.yaml"

# Charger la configuration YAML
with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
    FLUX_CONFIG = yaml.safe_load(f)

print(f"📋 Configuration chargée pour {len(FLUX_CONFIG)} types de flux: {list(FLUX_CONFIG.keys())}")


def decrypt_file_aes(encrypted_data: bytes, key: bytes, iv: bytes) -> bytes:
    """
    Déchiffre les données avec AES-CBC.
    Compatible avec la logique electriflux existante.
    """
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted_data = cipher.decrypt(encrypted_data)
    
    # Supprimer le padding PKCS7 si présent
    padding_length = decrypted_data[-1]
    if padding_length <= 16:  # Block size AES
        decrypted_data = decrypted_data[:-padding_length]
    
    return decrypted_data


def detect_flux_type_from_zip_name(zip_name: str) -> str:
    """
    Détecte le type de flux selon le nom du fichier ZIP.
    Basé sur les conventions Enedis.
    """
    zip_name_upper = zip_name.upper()
    
    if '_C15_' in zip_name_upper or '_C12_' in zip_name_upper:
        return 'C15'  # C12 et C15 sont traités de la même façon
    elif '_R151_' in zip_name_upper:
        return 'R151'
    elif '_R15_' in zip_name_upper and '_ACC' not in zip_name_upper:
        return 'R15'
    elif '_F12_' in zip_name_upper:
        return 'F12'
    elif '_F15_' in zip_name_upper:
        return 'F15'
    elif '_ACC' in zip_name_upper or 'ACC_' in zip_name_upper:
        return 'R15_ACC'
    else:
        return 'UNKNOWN'


@dlt.transformer
def decrypt_extract_flux_specific(items: Iterator[FileItemDict], flux_type: str) -> Iterator[dict]:
    """
    Transformer spécifique à un type de flux qui :
    1. Filtre les ZIPs correspondant au flux_type
    2. Déchiffre les fichiers ZIP avec AES-CBC
    3. Extrait et traite les XMLs correspondant au flux_type
    """
    # Récupérer les clés AES depuis les secrets DLT
    try:
        aes_config = dlt.secrets['aes']
        aes_key = bytes.fromhex(aes_config['key'])
        aes_iv = bytes.fromhex(aes_config['iv'])
    except Exception as e:
        raise ValueError(f"Erreur chargement clés AES depuis secrets: {e}")
    
    # Configuration pour ce type de flux
    config = FLUX_CONFIG[flux_type]
    
    processed_zips = set()
    total_records = 0
    
    # Mode test : limiter à quelques fichiers
    items_list = list(items)
    test_limit = 5  # Par type de flux
    
    # Filtrer les ZIPs pour ce type de flux
    flux_items = []
    for item in items_list:
        zip_name = item['file_name']
        detected_type = detect_flux_type_from_zip_name(zip_name)
        if detected_type == flux_type:
            flux_items.append(item)
    
    # Limiter pour les tests
    flux_items = flux_items[:test_limit]
    
    print(f"🔍 Flux {flux_type}: {len(flux_items)} fichiers ZIP trouvés (sur {len(items_list)} total)")
    
    if not flux_items:
        print(f"   ⚠️ Aucun fichier ZIP trouvé pour le flux {flux_type}")
        return
    
    for encrypted_item in flux_items:
        zip_modified = encrypted_item['modification_date']
        zip_name = encrypted_item['file_name']
        
        # Éviter les doublons basé sur nom + date de modification
        zip_key = f"{zip_name}_{zip_modified}"
        if zip_key in processed_zips:
            continue
        processed_zips.add(zip_key)
        
        print(f"🔐 {flux_type}: Déchiffrement de {zip_name}")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            zip_records = 0
            
            try:
                # 1. Lire le fichier chiffré depuis SFTP
                with encrypted_item.open() as f:
                    encrypted_data = f.read()
                
                # 2. Déchiffrer avec AES-CBC
                decrypted_data = decrypt_file_aes(encrypted_data, aes_key, aes_iv)
                
                # 3. Sauvegarder le ZIP déchiffré temporairement
                zip_path = temp_path / f"decrypted_{zip_name}"
                if not zip_path.name.endswith('.zip'):
                    zip_path = zip_path.with_suffix('.zip')
                
                zip_path.write_bytes(decrypted_data)
                
                # 4. Extraire le ZIP
                extract_dir = temp_path / 'extracted'
                extract_dir.mkdir(exist_ok=True)
                
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                
                xml_files = list(extract_dir.glob('**/*.xml'))
                
                # 5. Traiter chaque XML avec xml_to_dict
                for xml_path in xml_files:
                    # Utiliser xml_to_dict avec la config du flux
                    xml_record_count = 0
                    for record in xml_to_dict(
                        xml_path, 
                        row_level=config['row_level'],
                        metadata_fields=config.get('metadata_fields', {}),
                        data_fields=config.get('data_fields', {}),
                        nested_fields=config.get('nested_fields', [])
                    ):
                        xml_record_count += 1
                        zip_records += 1
                        total_records += 1
                        
                        # Ajouter métadonnées pour traçabilité
                        record['_source_zip'] = zip_name
                        record['_zip_modified'] = zip_modified
                        record['_flux_type'] = flux_type
                        record['_xml_name'] = xml_path.name
                        
                        yield record
                    
                    if xml_record_count > 0:
                        print(f"   ✅ {xml_path.name}: {xml_record_count} enregistrements")
                
                if zip_records > 0:
                    print(f"📊 {flux_type} - {zip_name}: {zip_records} enregistrements")
                        
            except zipfile.BadZipFile as e:
                print(f"❌ ZIP corrompu {zip_name}: {e}")
                continue
            except Exception as e:
                print(f"❌ Erreur {zip_name}: {e}")
                continue
    
    print(f"🎯 {flux_type}: {total_records} enregistrements totaux")


@dlt.source
def sftp_flux_enedis_multi():
    """
    Source DLT multi-ressources pour flux Enedis via SFTP avec déchiffrement AES.
    
    Crée une ressource DLT par type de flux configuré.
    Chaque ressource produit une table séparée.
    """
    # URL SFTP depuis secrets
    sftp_config = dlt.secrets['sftp']
    sftp_url = sftp_config['url']
    file_pattern = sftp_config.get('file_pattern', '**/*.zip')
    
    print(f"🌐 Connexion SFTP : {sftp_url}")
    print(f"📁 Pattern de fichiers : {file_pattern}")
    
    resources = []
    
    for flux_type, config in FLUX_CONFIG.items():
        print(f"🏗️  Création de la ressource pour {flux_type}")
        
        # Source filesystem DLT pour SFTP (partagée)
        encrypted_files = filesystem(
            bucket_url=sftp_url,
            file_glob=file_pattern
        )
        
        # Pipeline de transformation spécifique au flux
        flux_records = encrypted_files | decrypt_extract_flux_specific(flux_type=flux_type)
        
        # Configuration DLT par flux
        flux_records = flux_records.with_name(f"flux_{flux_type.lower()}")
        
        # Déterminer la clé primaire selon le type de flux
        if flux_type in ['R15', 'R151', 'R15_ACC']:
            # Flux de relevés : PDL + Date
            primary_key = ["pdl", "Date_Releve", "_xml_name"]  # Ajouter xml_name pour éviter doublons
        elif flux_type in ['F12', 'F15']:
            # Flux de facturation : PDL + Facture + Element valorisé
            primary_key = ["pdl", "Num_Facture", "Id_EV"] 
        else:  # C15
            # Flux contractuels : PDL + Date événement + source pour éviter doublons
            primary_key = ["pdl", "Date_Evenement", "_xml_name"]
        
        # Appliquer les hints DLT - désactiver primary_key pour debug
        flux_records.apply_hints(
            # primary_key=primary_key,  # Désactivé temporairement pour debug
            write_disposition="replace",  # Replace pour éviter erreur primary_key
            # incremental=dlt.sources.incremental("_zip_modified")  # Désactivé temporairement
        )
        
        resources.append(flux_records)
    
    return resources


def run_sftp_multi_pipeline():
    """
    Exécute le pipeline SFTP multi-ressources.
    """
    print("🚀 Démarrage du pipeline SFTP + AES + DLT (Multi-ressources)")
    print("=" * 70)
    
    # Créer le pipeline DLT
    pipeline = dlt.pipeline(
        pipeline_name="flux_enedis_sftp_multi",
        destination="duckdb",
        dataset_name="enedis_multi"
    )
    
    try:
        # Exécuter le chargement
        load_info = pipeline.run(sftp_flux_enedis_multi())
        
        print("✅ Pipeline SFTP multi-ressources exécuté avec succès !")
        print(f"📊 Résultats du chargement:")
        print(load_info)
        
        # Vérifier les résultats
        verify_multi_results()
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution du pipeline SFTP multi: {e}")
        return False


def verify_multi_results():
    """
    Vérifie et affiche les résultats du pipeline SFTP multi-ressources.
    """
    print("\\n📈 Vérification des résultats SFTP multi dans DuckDB...")
    
    try:
        import duckdb
        conn = duckdb.connect('flux_enedis_sftp_multi.duckdb')
        
        # Lister les tables créées
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'enedis_multi'"
        ).fetchall()
        
        if not tables:
            print("❌ Aucune table trouvée dans le schéma enedis_multi")
            return
        
        print(f"📋 Tables créées: {[t[0] for t in tables]}")
        
        # Statistiques par table
        total_records = 0
        for table_name, in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM enedis_multi.{table_name}").fetchone()[0]
                print(f"   📊 {table_name}: {count:,} enregistrements")
                total_records += count
                
                # Statistiques par ZIP source pour les tables de flux
                if table_name.startswith('flux_'):
                    zip_stats = conn.execute(f"""
                        SELECT _source_zip, COUNT(*) as records 
                        FROM enedis_multi.{table_name} 
                        WHERE _source_zip IS NOT NULL
                        GROUP BY _source_zip 
                        ORDER BY records DESC 
                        LIMIT 3
                    """).fetchall()
                    
                    if zip_stats:
                        print("   📦 Top 3 ZIP sources :")
                        for zip_name, count in zip_stats:
                            print(f"      {zip_name}: {count:,} records")
                
            except Exception as e:
                print(f"   ❌ Erreur pour table {table_name}: {e}")
        
        print(f"\\n🎯 Total: {total_records:,} enregistrements chargés")
        conn.close()
        
    except Exception as e:
        print(f"❌ Erreur lors de la vérification SFTP multi: {e}")


if __name__ == "__main__":
    print("🔧 Pipeline ETL SFTP multi-ressources avec déchiffrement AES")
    print("=" * 70)
    
    # Vérifier la configuration
    try:
        secrets = dlt.secrets
        sftp_config = secrets['sftp']
        aes_config = secrets['aes']
        print("✅ Configuration SFTP et AES trouvée")
        print(f"   SFTP URL configuré: {sftp_config['url'][:20]}...")
        print(f"   Clé AES configurée: {len(aes_config['key'])} caractères")
    except Exception as e:
        print(f"❌ Configuration manquante: {e}")
        print("📝 Configurez .dlt/secrets.toml avec les sections [sftp] et [aes]")
        exit(1)
    
    # Exécuter le pipeline
    success = run_sftp_multi_pipeline()
    
    if success:
        print("\\n🎉 Pipeline SFTP multi-ressources terminé avec succès !")
    else:
        print("\\n💥 Pipeline SFTP multi-ressources terminé avec des erreurs")