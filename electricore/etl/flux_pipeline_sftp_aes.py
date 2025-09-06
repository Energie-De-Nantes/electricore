"""
Pipeline ETL complet pour flux Enedis via SFTP avec déchiffrement AES.
Combine SFTP natif DLT + déchiffrement AES + logique xml_to_dict existante.
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


def detect_flux_type(filename: str) -> str:
    """
    Détecte le type de flux selon le nom du fichier XML.
    Basé sur les conventions Enedis.
    """
    filename_upper = filename.upper()
    
    if '_C15_' in filename_upper or '_C12_' in filename_upper:
        return 'C15'  # C12 et C15 sont traités de la même façon
    elif '_R151_' in filename_upper:
        return 'R151'
    elif '_R15_' in filename_upper and '_ACC' not in filename_upper:
        return 'R15'
    elif '_F12_' in filename_upper or '_FL_' in filename_upper:
        return 'F12'
    elif '_F15_' in filename_upper:
        return 'F15'
    elif '_ACC' in filename_upper or 'ACC_' in filename_upper:
        return 'R15_ACC'
    else:
        return 'UNKNOWN'


@dlt.transformer
def decrypt_extract_process(items: Iterator[FileItemDict]) -> Iterator[dict]:
    """
    Transformer principal qui :
    1. Déchiffre les fichiers ZIP avec AES-CBC
    2. Extrait les XML du ZIP
    3. Traite chaque XML avec xml_to_dict
    4. Yield les records avec métadonnées de traçabilité
    """
    # Récupérer les clés AES depuis les secrets DLT
    try:
        aes_config = dlt.secrets['aes']
        aes_key = bytes.fromhex(aes_config['key'])
        aes_iv = bytes.fromhex(aes_config['iv'])
    except Exception as e:
        raise ValueError(f"Erreur chargement clés AES depuis secrets: {e}")
    
    processed_zips = set()
    total_records = 0
    
    # Debug: compter les fichiers trouvés
    items_list = list(items)
    print(f"🔍 Trouvé {len(items_list)} fichiers sur SFTP avec pattern **/*.zip")
    
    # Limite pour test - traiter seulement les 10 premiers
    test_limit = 10
    items_list = items_list[:test_limit]
    print(f"🧪 Mode test: traitement des {len(items_list)} premiers fichiers")
    
    for encrypted_item in items_list:
        zip_modified = encrypted_item['modification_date']
        zip_name = encrypted_item['file_name']
        
        # Éviter les doublons basé sur nom + date de modification
        zip_key = f"{zip_name}_{zip_modified}"
        if zip_key in processed_zips:
            print(f"⏭️ ZIP déjà traité : {zip_name}")
            continue
        processed_zips.add(zip_key)
        
        print(f"🔐 Déchiffrement AES de {zip_name}")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            zip_records = 0
            
            try:
                # 1. Lire le fichier chiffré depuis SFTP
                with encrypted_item.open() as f:
                    encrypted_data = f.read()
                
                print(f"📥 Téléchargé {len(encrypted_data):,} bytes")
                
                # 2. Déchiffrer avec AES-CBC
                decrypted_data = decrypt_file_aes(encrypted_data, aes_key, aes_iv)
                print(f"🔓 Déchiffré {len(decrypted_data):,} bytes")
                
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
                print(f"📦 {len(xml_files)} fichiers XML extraits de {zip_name}")
                
                # 5. Traiter chaque XML avec xml_to_dict
                for xml_path in xml_files:
                    flux_type = detect_flux_type(xml_path.name)
                    
                    if flux_type == 'UNKNOWN':
                        print(f"⚠️ Type de flux inconnu pour : {xml_path.name}")
                        continue
                    
                    if flux_type not in FLUX_CONFIG:
                        print(f"⚠️ Configuration manquante pour flux {flux_type}")
                        continue
                    
                    config = FLUX_CONFIG[flux_type]
                    
                    # Utiliser xml_to_dict existant
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
                        
                        # Ajouter métadonnées pour traçabilité et incrémental
                        record['_source_zip'] = zip_name
                        record['_zip_modified'] = zip_modified
                        record['_flux_type'] = flux_type
                        record['_xml_name'] = xml_path.name
                        
                        yield record
                    
                    if xml_record_count > 0:
                        print(f"   ✅ {xml_path.name} ({flux_type}): {xml_record_count} enregistrements")
                
                print(f"📊 ZIP {zip_name}: {zip_records} enregistrements au total")
                        
            except zipfile.BadZipFile as e:
                print(f"❌ Fichier ZIP corrompu {zip_name}: {e}")
                continue
            except Exception as e:
                print(f"❌ Erreur traitement {zip_name}: {e}")
                continue
    
    print(f"🎯 Pipeline terminé: {total_records} enregistrements totaux")


@dlt.source
def sftp_flux_enedis_aes():
    """
    Source DLT pour flux Enedis via SFTP avec déchiffrement AES.
    
    Pipeline complet :
    SFTP → ZIP chiffrés → Déchiffrement AES → Extraction → XML → DLT
    """
    
    # URL SFTP depuis secrets
    sftp_config = dlt.secrets['sftp']
    sftp_url = sftp_config['url']
    file_pattern = sftp_config.get('file_pattern', '**/*.zip')
    
    print(f"🌐 Connexion SFTP : {sftp_url}")
    print(f"📁 Pattern de fichiers : {file_pattern}")
    
    # Source filesystem DLT pour SFTP
    encrypted_files = filesystem(
        bucket_url=sftp_url,
        file_glob=file_pattern
    )
    
    # Pipeline de transformation
    flux_records = encrypted_files | decrypt_extract_process()
    
    # Configuration DLT
    flux_records = flux_records.with_name("flux_enedis_sftp")
    flux_records.apply_hints(
        primary_key=["_xml_name", "_source_zip"],  # Évite les doublons
        write_disposition="merge",  # Merge pour gérer les reprises
        incremental=dlt.sources.incremental("_zip_modified")  # Incrémental sur date ZIP
    )
    
    return flux_records


def run_sftp_pipeline():
    """
    Exécute le pipeline SFTP complet.
    """
    print("🚀 Démarrage du pipeline SFTP + AES + DLT")
    print("=" * 60)
    
    # Créer le pipeline DLT
    pipeline = dlt.pipeline(
        pipeline_name="flux_enedis_sftp",
        destination="duckdb",
        dataset_name="enedis_data"
    )
    
    try:
        # Exécuter le chargement
        load_info = pipeline.run(sftp_flux_enedis_aes())
        
        print("✅ Pipeline SFTP exécuté avec succès !")
        print(f"📊 Résultats du chargement:")
        print(load_info)
        
        # Vérifier les résultats
        verify_sftp_results()
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution du pipeline SFTP: {e}")
        return False


def verify_sftp_results():
    """
    Vérifie et affiche les résultats du pipeline SFTP.
    """
    print("\n📈 Vérification des résultats SFTP dans DuckDB...")
    
    try:
        import duckdb
        conn = duckdb.connect('flux_enedis_sftp.duckdb')
        
        # Lister les tables créées
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'enedis_data'"
        ).fetchall()
        
        if not tables:
            print("❌ Aucune table trouvée dans le schéma enedis_data")
            return
        
        print(f"📋 Tables créées: {[t[0] for t in tables]}")
        
        # Statistiques par table
        for table_name, in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM enedis_data.{table_name}").fetchone()[0]
                print(f"   📊 {table_name}: {count:,} enregistrements")
                
                # Statistiques par ZIP source
                if table_name.startswith('flux_enedis'):
                    zip_stats = conn.execute(f"""
                        SELECT _source_zip, COUNT(*) as records 
                        FROM enedis_data.{table_name} 
                        WHERE _source_zip IS NOT NULL
                        GROUP BY _source_zip 
                        ORDER BY records DESC 
                        LIMIT 5
                    """).fetchall()
                    
                    if zip_stats:
                        print("   📦 Top 5 ZIP sources :")
                        for zip_name, count in zip_stats:
                            print(f"      {zip_name}: {count:,} records")
                
            except Exception as e:
                print(f"   ❌ Erreur pour table {table_name}: {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Erreur lors de la vérification SFTP: {e}")


if __name__ == "__main__":
    print("🔧 Pipeline ETL SFTP avec déchiffrement AES pour les flux Enedis")
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
        print("📁 Fichier attendu: .dlt/secrets.toml")
        exit(1)
    
    # Exécuter le pipeline
    success = run_sftp_pipeline()
    
    if success:
        print("\n🎉 Pipeline SFTP terminé avec succès !")
    else:
        print("\n💥 Pipeline SFTP terminé avec des erreurs")