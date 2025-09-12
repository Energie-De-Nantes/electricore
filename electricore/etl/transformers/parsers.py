"""
Transformers DLT pour le parsing XML et CSV des flux Enedis.
Utilise les fonctions pures pour le traitement des données.
"""

import dlt
import io
from typing import Iterator, Dict, Any, Optional, List
import polars as pl

# Import des fonctions pures
from lib.xml_parser import xml_to_dict_from_bytes


def _xml_parser_transformer_base(
    extracted_file: dict,
    row_level: str,
    metadata_fields: Dict[str, str],
    data_fields: Dict[str, str],
    nested_fields: List[Dict[str, Any]],
    flux_type: str
) -> Iterator[dict]:
    """
    Fonction de base pour parser les fichiers XML extraits.
    
    Args:
        extracted_file: Fichier extrait du transformer archive
        row_level: Niveau XPath pour les enregistrements
        metadata_fields: Champs de métadonnées XML
        data_fields: Champs de données XML
        nested_fields: Configuration des champs imbriqués
        flux_type: Type de flux pour traçabilité
    
    Yields:
        dict: Enregistrements parsés avec métadonnées de traçabilité
    """
    
    zip_name = extracted_file['source_zip']
    zip_modified = extracted_file['modification_date']
    xml_name = extracted_file['extracted_file_name']
    xml_content = extracted_file['extracted_content']
    
    try:
        print(f"🔍 Parsing XML: {xml_name}")
        
        # Parser le XML avec la configuration
        records_count = 0
        for record in xml_to_dict_from_bytes(
            xml_content,
            row_level=row_level,
            metadata_fields=metadata_fields,
            data_fields=data_fields,
            nested_fields=nested_fields
        ):
            # Enrichir avec métadonnées de traçabilité
            enriched_record = record.copy()
            enriched_record.update({
                '_source_zip': zip_name,
                '_flux_type': flux_type,
                '_xml_name': xml_name,
                'modification_date': zip_modified
            })
            
            yield enriched_record
            records_count += 1
        
        print(f"✅ Parsé: {records_count} enregistrements depuis {xml_name}")
        
    except Exception as e:
        print(f"❌ Erreur parsing XML {xml_name}: {e}")
        return


def _csv_parser_transformer_base(
    extracted_file: dict,
    delimiter: str,
    encoding: str,
    flux_type: str,
    column_mapping: Dict[str, str]
) -> Iterator[dict]:
    """
    Fonction de base pour parser les fichiers CSV avec Polars.
    
    Args:
        extracted_file: Fichier extrait du transformer archive
        delimiter: Délimiteur CSV
        encoding: Encodage du fichier
        flux_type: Type de flux pour traçabilité
        column_mapping: Mapping des colonnes (français → snake_case)
    
    Yields:
        dict: Lignes CSV parsées avec métadonnées
    """
    
    zip_name = extracted_file['source_zip']
    zip_modified = extracted_file['modification_date']
    csv_name = extracted_file['extracted_file_name']
    csv_content = extracted_file['extracted_content']
    
    try:
        print(f"📊 Parsing CSV: {csv_name}")
        
        # Décoder le contenu CSV
        csv_text = csv_content.decode(encoding)
        
        # Parser avec Polars (plus performant)
        df = pl.read_csv(
            io.StringIO(csv_text),
            separator=delimiter,
            encoding=encoding,
            null_values=['null', '', 'NULL', 'None'],
            ignore_errors=True,
            infer_schema_length=10000
        )
        
        # Appliquer le mapping des colonnes si fourni
        if column_mapping:
            df = df.rename(column_mapping)
        
        # Ajouter métadonnées de traçabilité
        df_with_meta = df.with_columns([
            pl.lit(zip_modified).alias('modification_date'),
            pl.lit(zip_name).alias('_source_zip'),
            pl.lit(flux_type).alias('_flux_type'),
            pl.lit(csv_name).alias('_csv_name')
        ])
        
        # Yield chaque ligne comme dictionnaire
        records_count = 0
        for row_dict in df_with_meta.to_dicts():
            yield row_dict
            records_count += 1
        
        print(f"✅ Parsé: {records_count} lignes depuis {csv_name}")
        
    except Exception as e:
        print(f"❌ Erreur parsing CSV {csv_name}: {e}")
        return


def create_xml_parser_transformer(
    row_level: str,
    metadata_fields: Dict[str, str] = None,
    data_fields: Dict[str, str] = None,
    nested_fields: List[Dict[str, Any]] = None,
    flux_type: str = "unknown"
):
    """
    Factory pour créer un transformer de parsing XML configuré.
    
    Args:
        row_level: Niveau XPath pour les enregistrements
        metadata_fields: Champs de métadonnées XML
        data_fields: Champs de données XML
        nested_fields: Configuration des champs imbriqués
        flux_type: Type de flux
    
    Returns:
        Transformer DLT configuré
    """
    @dlt.transformer
    def configured_xml_parser(extracted_file: dict) -> Iterator[dict]:
        return _xml_parser_transformer_base(
            extracted_file, row_level, metadata_fields or {}, 
            data_fields or {}, nested_fields or [], flux_type
        )
    
    return configured_xml_parser


def create_csv_parser_transformer(
    delimiter: str = ',',
    encoding: str = 'utf-8',
    flux_type: str = "unknown",
    column_mapping: Dict[str, str] = None
):
    """
    Factory pour créer un transformer de parsing CSV configuré.
    
    Args:
        delimiter: Délimiteur CSV
        encoding: Encodage du fichier
        flux_type: Type de flux
        column_mapping: Mapping des colonnes
    
    Returns:
        Transformer DLT configuré
    """
    @dlt.transformer
    def configured_csv_parser(extracted_file: dict) -> Iterator[dict]:
        return _csv_parser_transformer_base(
            extracted_file, delimiter, encoding, flux_type, column_mapping or {}
        )
    
    return configured_csv_parser


# Column mapping pour R64 (réutilisé depuis l'ancien code)
R64_COLUMN_MAPPING = {
    'Identifiant PRM': 'id_prm',
    'Date de début': 'date_debut', 
    'Date de fin': 'date_fin',
    'Grandeur physique': 'grandeur_physique',
    'Grandeur metier': 'grandeur_metier',
    'Etape metier': 'etape_metier',
    'Unite': 'unite',
    'Horodate': 'horodate',
    'Contexte de relève': 'contexte_releve',
    'Type de releve': 'type_releve',
    'Motif de relève': 'motif_releve',
    'Grille': 'grille',
    'Identifiant calendrier': 'id_calendrier',
    'Libellé calendrier': 'libelle_calendrier',
    'Identifiant classe temporelle': 'id_classe_temporelle',
    'Libellé classe temporelle': 'libelle_classe_temporelle',
    'Cadran': 'cadran',
    'Valeur': 'valeur',
    'Indice de vraisemblance': 'indice_vraisemblance'
}

# Transformer pré-configuré pour R64
csv_r64_parser_transformer = create_csv_parser_transformer(
    delimiter=';',
    encoding='utf-8',
    flux_type='R64',
    column_mapping=R64_COLUMN_MAPPING
)