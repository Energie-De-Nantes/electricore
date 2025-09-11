"""
Transformateurs de données purs pour les flux Enedis.
Fonctions de transformation, enrichissement et extraction.
"""

import zipfile
import io
from typing import Iterator
from lib.xml_parser import xml_to_dict_from_bytes


def extract_files_from_zip(zip_data: bytes, file_extension: str = '.xml') -> list[tuple[str, bytes]]:
    """
    Extrait les fichiers d'une extension donnée d'un ZIP.
    
    Args:
        zip_data: Contenu du fichier ZIP
        file_extension: Extension des fichiers à extraire (ex: '.xml', '.csv')
    
    Returns:
        List[Tuple[str, bytes]]: Liste de (nom_fichier, contenu)
    
    Raises:
        zipfile.BadZipFile: Si le ZIP est corrompu
    """
    files = []
    
    with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as zip_ref:
        for file_info in zip_ref.filelist:
            if file_info.filename.lower().endswith(file_extension.lower()) and not file_info.is_dir():
                try:
                    content = zip_ref.read(file_info.filename)
                    files.append((file_info.filename, content))
                except Exception as e:
                    print(f"⚠️ Erreur lecture {file_info.filename}: {e}")
                    continue
    
    return files


def process_xml_content(
    xml_content: bytes,
    xml_name: str,
    flux_config: dict
) -> Iterator[dict]:
    """
    Transforme le contenu XML en enregistrements selon la configuration.
    
    Args:
        xml_content: Contenu binaire du fichier XML
        xml_name: Nom du fichier XML
        flux_config: Configuration du flux
    
    Yields:
        dict: Enregistrements extraits du XML
    """
    for record in xml_to_dict_from_bytes(
        xml_content,
        row_level=flux_config['row_level'],
        metadata_fields=flux_config.get('metadata_fields', {}),
        data_fields=flux_config.get('data_fields', {}),
        nested_fields=flux_config.get('nested_fields', [])
    ):
        yield record


def enrich_record(
    record: dict,
    zip_name: str,
    zip_modified: str,
    flux_type: str,
    xml_name: str
) -> dict:
    """
    Ajoute les métadonnées de traçabilité à un enregistrement.
    
    Args:
        record: Enregistrement à enrichir
        zip_name: Nom du fichier ZIP source
        zip_modified: Date de modification du ZIP
        flux_type: Type de flux
        xml_name: Nom du fichier XML source
    
    Returns:
        dict: Nouvel enregistrement enrichi (copie)
    """
    enriched = record.copy()
    enriched.update({
        '_source_zip': zip_name,
        '_flux_type': flux_type,
        '_xml_name': xml_name,
        'modification_date': zip_modified  # Pour l'incrémental DLT et traçabilité
    })
    return enriched