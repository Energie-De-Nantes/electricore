"""
Transformers DLT pour le parsing XML et CSV des flux Enedis.
Inclut les fonctions pures de parsing et les transformers DLT.
"""

import dlt
import io
import re
import fnmatch
from typing import Iterator, Dict, Any, Optional, List
import polars as pl
from lxml import etree


# =============================================================================
# FONCTIONS PURES DE PARSING XML
# =============================================================================

def match_xml_pattern(xml_name: str, pattern: str | None) -> bool:
    """
    Vérifie si un nom de fichier XML correspond au pattern (wildcard ou regex).
    
    Args:
        xml_name: Nom du fichier XML
        pattern: Pattern wildcard (*,?) ou regex, ou None
    
    Returns:
        bool: True si le fichier match (ou si pas de pattern)
    """
    if pattern is None:
        return True  # Pas de pattern = accepte tout
    
    try:
        # Si le pattern contient des wildcards (* ou ?), utiliser fnmatch
        if '*' in pattern or '?' in pattern:
            return fnmatch.fnmatch(xml_name, pattern)
        else:
            # Sinon, traiter comme regex
            return bool(re.search(pattern, xml_name))
    except re.error:
        # En cas d'erreur regex, essayer comme wildcard en fallback
        try:
            return fnmatch.fnmatch(xml_name, pattern)
        except Exception:
            print(f"⚠️ Pattern invalide '{pattern}' pour {xml_name}")
            return False


def xml_to_dict_from_bytes(
    xml_bytes: bytes,
    row_level: str,
    metadata_fields: dict = None,
    data_fields: dict = None,
    nested_fields: list = None
) -> Iterator[dict]:
    """
    Version lxml de xml_to_dict qui parse directement des bytes - SANS écriture disque.
    
    Args:
        xml_bytes: Contenu XML en bytes
        row_level: XPath pour les lignes
        metadata_fields: Champs de métadonnées
        data_fields: Champs de données
        nested_fields: Champs imbriqués
    
    Yields:
        dict: Enregistrements extraits
    """
    # Initialiser les paramètres par défaut
    metadata_fields = metadata_fields or {}
    data_fields = data_fields or {}
    nested_fields = nested_fields or []
    
    # Parser directement depuis bytes avec lxml - très efficace !
    root = etree.fromstring(xml_bytes)

    # Extraire les métadonnées une seule fois avec XPath
    meta: dict[str, str] = {}
    for field_name, field_xpath in metadata_fields.items():
        elements = root.xpath(field_xpath)
        if elements and hasattr(elements[0], 'text') and elements[0].text:
            meta[field_name] = elements[0].text

    # Parcourir chaque ligne avec XPath (plus puissant qu'ElementTree)
    for row in root.xpath(row_level):
        # Extraire les champs de données principaux avec XPath relatif
        row_data: dict[str, Any] = {}
        
        for field_name, field_xpath in data_fields.items():
            elements = row.xpath(field_xpath)
            if elements and hasattr(elements[0], 'text') and elements[0].text:
                row_data[field_name] = elements[0].text
        
        # Extraire les champs imbriqués avec conditions (logique identique à xml_to_dict)
        for nested in nested_fields:
            prefix = nested.get('prefix', '')
            child_path = nested['child_path']
            id_field = nested['id_field'] 
            value_field = nested['value_field']
            conditions = nested.get('conditions', [])
            additional_fields = nested.get('additional_fields', {})

            # Parcourir les éléments enfants avec XPath
            for nr in row.xpath(child_path):
                # Vérifier toutes les conditions
                all_conditions_met = True
                
                for cond in conditions:
                    cond_xpath = cond['xpath']
                    cond_value = cond['value']
                    cond_elements = nr.xpath(cond_xpath)
                    
                    if not cond_elements or not hasattr(cond_elements[0], 'text') or cond_elements[0].text != cond_value:
                        all_conditions_met = False
                        break
                
                # Si toutes les conditions sont remplies
                if all_conditions_met:
                    key_elements = nr.xpath(id_field)
                    value_elements = nr.xpath(value_field)
                    
                    if (key_elements and value_elements and 
                        hasattr(key_elements[0], 'text') and hasattr(value_elements[0], 'text') and
                        key_elements[0].text and value_elements[0].text):
                        
                        # Ajouter la valeur principale avec préfixe
                        field_key = f"{prefix}{key_elements[0].text}"
                        row_data[field_key] = value_elements[0].text
                        
                        # Ajouter les champs additionnels
                        for add_field_name, add_field_xpath in additional_fields.items():
                            add_field_key = f"{prefix}{add_field_name}"
                            
                            # Éviter d'écraser si déjà présent
                            if add_field_key not in row_data:
                                add_elements = nr.xpath(add_field_xpath)
                                if (add_elements and hasattr(add_elements[0], 'text') and 
                                    add_elements[0].text):
                                    row_data[add_field_key] = add_elements[0].text
        
        # Fusionner métadonnées et données de ligne
        final_record = {**row_data, **meta}
        
        yield final_record


# =============================================================================
# TRANSFORMERS DLT
# =============================================================================


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
        # print(f"🔍 Parsing XML: {xml_name}")
        
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
        
        # print(f"✅ Parsé: {records_count} enregistrements depuis {xml_name}")
        
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