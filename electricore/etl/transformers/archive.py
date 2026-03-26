"""
Transformer DLT pour l'extraction de fichiers depuis des archives ZIP.
Inclut les fonctions pures d'extraction et le transformer DLT.
"""

import dlt
import logging
import zipfile
import io
from typing import Iterator, Optional
import fnmatch

logger = logging.getLogger(__name__)


# =============================================================================
# FONCTIONS PURES D'EXTRACTION ZIP
# =============================================================================

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
                    logger.warning("Erreur lecture %s: %s", file_info.filename, e)
                    continue
    
    return files


# Import de la fonction de matching depuis parsers
from electricore.etl.transformers.parsers import match_xml_pattern


# =============================================================================
# TRANSFORMER DLT
# =============================================================================


def _unzip_transformer_base(
    decrypted_file: dict,
    file_extension: str,
    file_regex: Optional[str]
) -> Iterator[dict]:
    """
    Fonction de base pour extraire les fichiers d'archives ZIP déchiffrées.
    
    Args:
        decrypted_file: Fichier déchiffré du transformer crypto
        file_extension: Extension des fichiers à extraire (ex: '.xml', '.csv')
        file_regex: Pattern optionnel pour filtrer les noms de fichiers
    
    Yields:
        dict: {
            'source_zip': str,
            'modification_date': datetime,
            'extracted_file_name': str,
            'extracted_content': bytes,
            'file_size': int
        }
    """
    zip_name = decrypted_file['file_name']
    zip_modified = decrypted_file['modification_date']
    decrypted_content = decrypted_file['decrypted_content']
    
    try:
        # print(f"📁 Extraction ZIP: {zip_name}")
        
        # Extraire les fichiers de l'extension souhaitée
        extracted_files = extract_files_from_zip(decrypted_content, file_extension)
        
        for file_name, file_content in extracted_files:
            # Filtrer par regex si spécifié
            if file_regex and not match_xml_pattern(file_name, file_regex):
                # print(f"⏭️  Ignoré (regex): {file_name}")
                continue
            
            # Yield le fichier extrait avec métadonnées
            yield {
                'source_zip': zip_name,
                'modification_date': zip_modified,
                'extracted_file_name': file_name,
                'extracted_content': file_content,
                'file_size': len(file_content)
            }
            
            # print(f"✅ Extrait: {file_name} ({len(file_content)} bytes)")
        
        if not extracted_files:
            logger.warning("Aucun fichier %s trouvé dans %s", file_extension, zip_name)

    except Exception as e:
        logger.error("Erreur extraction %s: %s", zip_name, e)
        return


def create_unzip_transformer(
    file_extension: str = '.xml',
    file_regex: Optional[str] = None
):
    """
    Factory pour créer un transformer d'extraction ZIP configuré.
    
    Args:
        file_extension: Extension des fichiers à extraire
        file_regex: Pattern optionnel pour filtrer les noms
    
    Returns:
        Transformer DLT configuré
    """
    @dlt.transformer
    def configured_unzip_transformer(decrypted_file: dict) -> Iterator[dict]:
        return _unzip_transformer_base(decrypted_file, file_extension, file_regex)
    
    return configured_unzip_transformer


# Transformers pré-configurés pour les cas courants
unzip_xml_transformer = create_unzip_transformer('.xml')
unzip_csv_transformer = create_unzip_transformer('.csv')