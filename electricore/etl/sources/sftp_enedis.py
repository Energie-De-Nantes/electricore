"""
Source DLT pour les flux Enedis via SFTP avec déchiffrement AES.
Utilise les fonctions pures du module lib/ pour le traitement.
"""

import dlt
from typing import Iterator
from dlt.sources.filesystem import filesystem, read_csv

# Imports des fonctions pures depuis lib/
from lib.crypto import load_aes_credentials, decrypt_file_aes
from lib.processors import read_sftp_file
from lib.transformers import extract_files_from_zip
from lib.xml_parser import match_xml_pattern, xml_to_dict_from_bytes


def create_xml_resource(flux_type: str, xml_config: dict, zip_pattern: str, sftp_url: str, aes_key: bytes, aes_iv: bytes):
    """
    Crée une resource DLT qui traite directement un type XML spécifique.
    Pattern DLT recommandé : 1 resource = 1 table
    """
    @dlt.resource(
        name=xml_config['name'],
        write_disposition="append"
    )
    def xml_resource():
        # Source filesystem DLT pour SFTP avec incrémental
        # Nom unique pour chaque resource pour éviter les conflits d'état
        filesystem_name = f"filesystem_{flux_type.lower()}_{xml_config['name']}"
        print(f"🔍 Recherche fichiers pour {xml_config['name']}: {sftp_url} avec pattern {zip_pattern}")
        
        encrypted_files = filesystem(
            bucket_url=sftp_url,
            file_glob=zip_pattern
        ).with_name(filesystem_name)
        
        # Appliquer l'incrémental sur la date de modification
        encrypted_files.apply_hints(
            incremental=dlt.sources.incremental("modification_date")
        )
        
        # Pour chaque fichier ZIP
        for encrypted_item in encrypted_files:
            try:
                # Déchiffrer et extraire XMLs
                encrypted_data = read_sftp_file(encrypted_item)
                decrypted_data = decrypt_file_aes(encrypted_data, aes_key, aes_iv)
                xml_files = extract_files_from_zip(decrypted_data, '.xml')
                
                # Traiter chaque XML avec la configuration spécifique
                for xml_name, xml_content in xml_files:
                    # Filtrer par file_regex si spécifié
                    if not match_xml_pattern(xml_name, xml_config.get('file_regex')):
                        continue
                    
                    # Traiter XML avec la configuration spécifique
                    for record in xml_to_dict_from_bytes(
                        xml_content,
                        row_level=xml_config['row_level'],
                        metadata_fields=xml_config.get('metadata_fields', {}),
                        data_fields=xml_config.get('data_fields', {}),
                        nested_fields=xml_config.get('nested_fields', [])
                    ):
                        # Enrichir avec métadonnées de traçabilité
                        record.update({
                            '_source_zip': encrypted_item['file_name'],
                            '_flux_type': flux_type,
                            '_xml_name': xml_name,
                            'modification_date': encrypted_item['modification_date']
                        })
                        yield record
                        
            except Exception as e:
                print(f"❌ Erreur traitement {encrypted_item['file_name']}: {e}")
                continue
    
    return xml_resource


def create_csv_resource(flux_type: str, csv_config: dict, zip_pattern: str, sftp_url: str, aes_key: bytes, aes_iv: bytes):
    """
    Crée une resource DLT qui traite les fichiers CSV depuis des ZIP chiffrés.
    Pattern DLT recommandé : 1 resource = 1 table
    """
    @dlt.resource(
        name=csv_config['name'],
        write_disposition="append"
    )
    def csv_resource():
        # Source filesystem DLT pour SFTP avec incrémental
        filesystem_name = f"filesystem_{flux_type.lower()}_{csv_config['name']}"
        print(f"🔍 Recherche fichiers CSV pour {csv_config['name']}: {sftp_url} avec pattern {zip_pattern}")
        
        encrypted_files = filesystem(
            bucket_url=sftp_url,
            file_glob=zip_pattern
        ).with_name(filesystem_name)
        
        # Appliquer l'incrémental sur la date de modification
        encrypted_files.apply_hints(
            incremental=dlt.sources.incremental("modification_date")
        )
        
        # Pour chaque fichier ZIP
        for encrypted_item in encrypted_files:
            try:
                # Déchiffrer et extraire CSVs
                encrypted_data = read_sftp_file(encrypted_item)
                decrypted_data = decrypt_file_aes(encrypted_data, aes_key, aes_iv)
                csv_files = extract_files_from_zip(decrypted_data, '.csv')
                
                # Traiter chaque CSV avec pandas et read_csv de DLT
                for csv_name, csv_content in csv_files:
                    # Filtrer par file_regex si spécifié
                    if csv_config.get('file_regex'):
                        import fnmatch
                        if not fnmatch.fnmatch(csv_name, csv_config['file_regex']):
                            continue
                    
                    # Utiliser pandas pour lire le CSV
                    import pandas as pd
                    import io
                    
                    df = pd.read_csv(
                        io.BytesIO(csv_content),
                        delimiter=csv_config.get('delimiter', ','),
                        encoding=csv_config.get('encoding', 'utf-8')
                    )
                    
                    # Convertir en records et enrichir avec métadonnées
                    for record in df.to_dict(orient='records'):
                        record.update({
                            '_source_zip': encrypted_item['file_name'],
                            '_flux_type': flux_type,
                            '_csv_name': csv_name,
                            'modification_date': encrypted_item['modification_date']
                        })
                        yield record
                        
            except Exception as e:
                print(f"❌ Erreur traitement CSV {encrypted_item['file_name']}: {e}")
                continue
    
    return csv_resource


@dlt.source(name="flux_enedis")
def sftp_flux_enedis_multi(flux_config: dict):
    """
    Source DLT multi-ressources pour flux Enedis via SFTP avec déchiffrement AES.
    
    Version refactorisée avec fonctions pures et injection des clés AES.
    Crée une ressource DLT par type de flux configuré.
    Chaque ressource produit une table séparée.
    
    Args:
        flux_config: Configuration des flux depuis config/settings.py
    """
    # URL SFTP depuis secrets
    sftp_config = dlt.secrets['sftp']
    sftp_url = sftp_config['url']
    file_pattern = sftp_config.get('file_pattern', '**/*.zip')
    
    print(f"🌐 Connexion SFTP : {sftp_url}")
    print(f"📁 Pattern de fichiers : {file_pattern}")
    
    # Charger les clés AES une seule fois (optimisation)
    aes_key, aes_iv = load_aes_credentials()
    print(f"🔐 Clés AES chargées: {len(aes_key)} bytes")
    
    # Créer une resource DLT pour chaque xml_config (pattern recommandé)
    print("=" * 80)
    print("🚀 CRÉATION DES RESSOURCES DLT")
    print("=" * 80)
    
    for flux_type, flux_config_data in flux_config.items():
        zip_pattern = flux_config_data['zip_pattern']
        
        # Gérer xml_configs s'ils existent
        if 'xml_configs' in flux_config_data:
            xml_configs = flux_config_data['xml_configs']
            print(f"\n🏗️  FLUX {flux_type}: {len(xml_configs)} config(s) XML")
            print(f"   📁 Zip pattern: {zip_pattern}")
            
            table_names = [xml_config['name'] for xml_config in xml_configs]
            print(f"   📊 Tables XML cibles: {table_names}")
            
            try:
                for xml_config in xml_configs:
                    xml_resource_factory = create_xml_resource(flux_type, xml_config, zip_pattern, sftp_url, aes_key, aes_iv)
                    xml_resource = xml_resource_factory()
                    print(f"   ✅ Resource XML {xml_config['name']} créée")
                    yield xml_resource
            except Exception as e:
                print(f"   ❌ ERREUR création flux XML {flux_type}: {e}")
                raise
        
        # Gérer csv_configs s'ils existent
        if 'csv_configs' in flux_config_data:
            csv_configs = flux_config_data['csv_configs']
            print(f"\n🏗️  FLUX {flux_type}: {len(csv_configs)} config(s) CSV")
            print(f"   📁 Zip pattern: {zip_pattern}")
            
            table_names = [csv_config['name'] for csv_config in csv_configs]
            print(f"   📊 Tables CSV cibles: {table_names}")
            
            try:
                for csv_config in csv_configs:
                    csv_resource_factory = create_csv_resource(flux_type, csv_config, zip_pattern, sftp_url, aes_key, aes_iv)
                    csv_resource = csv_resource_factory()
                    print(f"   ✅ Resource CSV {csv_config['name']} créée")
                    yield csv_resource
            except Exception as e:
                print(f"   ❌ ERREUR création flux CSV {flux_type}: {e}")
                raise
    
    print("\n" + "=" * 80)
    print("✅ TOUTES LES RESSOURCES CRÉÉES")
    print("=" * 80)