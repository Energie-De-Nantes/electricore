"""
Orchestrateur fonctionnel pour le traitement des flux.
Coordonne cryptographie, parsing et transformation sans dépendance DLT.
"""

import zipfile
from typing import Iterator
from dlt.sources.filesystem import FileItemDict

from lib.crypto import decrypt_file_aes
from lib.transformers import extract_files_from_zip, enrich_record
from lib.xml_parser import match_xml_pattern, xml_to_dict_from_bytes


def read_sftp_file(encrypted_item: FileItemDict) -> bytes:
    """
    Lit le contenu d'un fichier depuis SFTP.
    
    Args:
        encrypted_item: Item FileItemDict de DLT
    
    Returns:
        bytes: Contenu du fichier
    """
    with encrypted_item.open() as f:
        return f.read()


def process_flux_items(
    items: Iterator[FileItemDict],
    flux_type: str,
    xml_config: dict,
    aes_key: bytes,
    aes_iv: bytes
) -> Iterator[dict]:
    """
    Orchestrateur principal refactorisé avec fonctions pures.
    
    Args:
        items: Itérateur des fichiers SFTP
        flux_type: Type de flux à traiter
        xml_config: Configuration XML spécifique 
        aes_key: Clé AES (injectée)
        aes_iv: IV AES (injecté)
    
    Yields:
        dict: Enregistrements enrichis avec métadonnées
    """
    total_records = 0
    
    for encrypted_item in items:
        zip_records = 0
        
        try:
            # 1. Lecture et déchiffrement
            encrypted_data = read_sftp_file(encrypted_item)
            decrypted_data = decrypt_file_aes(encrypted_data, aes_key, aes_iv)
            
            # 2. Extraction des XMLs en mémoire
            xml_files = extract_files_from_zip(decrypted_data, '.xml')
            
            # 3. Traitement de chaque XML
            for xml_name, xml_content in xml_files:
                # Filtrer par file_regex de la xml_config
                if not match_xml_pattern(xml_name, xml_config.get('file_regex')):
                    continue
                
                xml_record_count = 0
                # Parser avec lxml directement depuis bytes
                for record in xml_to_dict_from_bytes(
                    xml_content,
                    row_level=xml_config['row_level'],
                    metadata_fields=xml_config.get('metadata_fields', {}),
                    data_fields=xml_config.get('data_fields', {}),
                    nested_fields=xml_config.get('nested_fields', [])
                ):
                    # Enrichir avec métadonnées de traçabilité
                    enriched_record = enrich_record(
                        record,
                        encrypted_item['file_name'],
                        encrypted_item['modification_date'],
                        flux_type,
                        xml_name
                    )
                    
                    xml_record_count += 1
                    zip_records += 1
                    total_records += 1
                    
                    yield enriched_record

        except zipfile.BadZipFile as e:
            print(f"❌ ZIP corrompu {encrypted_item['file_name']}: {e}")
            continue
        except Exception as e:
            print(f"❌ Erreur {encrypted_item['file_name']}: {e}")
            continue
    
    # Log final
    print(f"🎯 {flux_type}: {total_records} enregistrements totaux")