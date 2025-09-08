"""
Helpers de logging pour les opérations ETL.
Fonctions pures pour le logging structuré des traitements.
"""

import zipfile


def log_processing_info(flux_type: str, zip_name: str, record_count: int):
    """
    Log les informations de traitement d'un fichier ZIP.
    
    Args:
        flux_type: Type de flux traité
        zip_name: Nom du fichier ZIP
        record_count: Nombre d'enregistrements traités (peut être 0 en début)
    """
    print(f"🔐 {flux_type}: Déchiffrement de {zip_name}")
    if record_count > 0:
        # Log détaillé uniquement si demandé
        pass


def log_xml_info(xml_name: str, record_count: int):
    """
    Log les informations de traitement d'un fichier XML.
    
    Args:
        xml_name: Nom du fichier XML
        record_count: Nombre d'enregistrements extraits
    """
    if record_count > 0:
        print(f"   ✅ {xml_name}: {record_count} enregistrements")


def log_error(zip_name: str, error: Exception):
    """
    Log les erreurs de traitement de manière structurée.
    
    Args:
        zip_name: Nom du fichier ayant causé l'erreur
        error: Exception capturée
    """
    if isinstance(error, zipfile.BadZipFile):
        print(f"❌ ZIP corrompu {zip_name}: {error}")
    else:
        print(f"❌ Erreur {zip_name}: {error}")