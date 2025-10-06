"""
Transformers DLT pour le parsing XML et CSV des flux Enedis.
Inclut les fonctions pures de parsing et les transformers DLT.
"""

import dlt
import io
import re
import json
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

# =============================================================================
# TRANSFORMER JSON DLT
# =============================================================================

def _json_parser_transformer_base(
    extracted_file: dict,
    record_path: str,
    metadata_fields: Dict[str, str],
    data_fields: Dict[str, str],
    nested_fields: List[Dict[str, Any]],
    flux_type: str
) -> Iterator[dict]:
    """
    Fonction de base pour parser les fichiers JSON.
    Parallèle à _xml_parser_transformer_base.

    Args:
        extracted_file: Fichier extrait du transformer archive
        record_path: Chemin vers les enregistrements
        metadata_fields: Champs de métadonnées
        data_fields: Champs de données principaux
        nested_fields: Configuration des champs imbriqués
        flux_type: Type de flux pour traçabilité

    Yields:
        dict: Enregistrements parsés avec métadonnées de traçabilité
    """
    # Gérer les deux cas : JSON extrait de ZIP ou JSON direct du SFTP
    if 'source_zip' in extracted_file:
        # Cas 1: JSON extrait d'un ZIP
        zip_name = extracted_file['source_zip']
        zip_modified = extracted_file['modification_date']
        json_name = extracted_file['extracted_file_name']
        json_content = extracted_file['extracted_content']
    else:
        # Cas 2: JSON décrypté du SFTP (R64)
        zip_name = extracted_file['file_name']  # Nom du fichier JSON
        zip_modified = extracted_file['modification_date']
        json_name = extracted_file['file_name']
        json_content = extracted_file['decrypted_content']

    try:
        print(f"📄 Parsing JSON: {json_name}")

        records_count = 0

        # Parser le JSON et extraire les enregistrements
        for record in json_to_dict_from_bytes(
            json_content, record_path, metadata_fields, data_fields, nested_fields
        ):
            # Enrichir avec métadonnées de traçabilité DLT
            enriched_record = {
                **record,
                'modification_date': zip_modified,
                '_source_zip': zip_name,
                '_flux_type': flux_type,
                '_json_name': json_name
            }

            yield enriched_record
            records_count += 1

        print(f"✅ Parsé: {records_count} enregistrements depuis {json_name}")

    except Exception as e:
        print(f"❌ Erreur parsing JSON {json_name}: {e}")
        return


def create_json_parser_transformer(
    record_path: str,
    metadata_fields: Dict[str, str] = None,
    data_fields: Dict[str, str] = None,
    nested_fields: List[Dict[str, Any]] = None,
    flux_type: str = "unknown"
):
    """
    Factory pour créer un transformer de parsing JSON configuré.
    Parallèle à create_xml_parser_transformer.

    Args:
        record_path: Chemin vers les enregistrements dans le JSON
        metadata_fields: Champs de métadonnées JSON
        data_fields: Champs de données JSON
        nested_fields: Configuration des champs imbriqués
        flux_type: Type de flux

    Returns:
        Transformer DLT configuré
    """
    @dlt.transformer
    def configured_json_parser(extracted_file: dict) -> Iterator[dict]:
        return _json_parser_transformer_base(
            extracted_file, record_path, metadata_fields or {},
            data_fields or {}, nested_fields or [], flux_type
        )

    return configured_json_parser


# =============================================================================
# TRANSFORMER JSON R64 SPÉCIALISÉ - FORMAT WIDE
# =============================================================================

def extract_header_metadata(data: dict) -> dict:
    """
    Extrait les métadonnées du header JSON R64.

    Args:
        data: Données JSON R64 complètes

    Returns:
        dict: Métadonnées du header
    """
    header = data.get('header', {})
    return {
        'id_demande': header.get('idDemande'),
        'si_demandeur': header.get('siDemandeur'),
        'code_flux': header.get('codeFlux'),
        'format': header.get('format')
    }


def is_valid_calendrier(calendrier: dict) -> bool:
    """
    Vérifie si le calendrier est un calendrier distributeur valide.

    Args:
        calendrier: Données du calendrier

    Returns:
        bool: True si calendrier distributeur valide
    """
    id_calendrier = calendrier.get('idCalendrier')
    libelle_calendrier = calendrier.get('libelleCalendrier', '').lower()

    # Filtrer par ID calendrier ou par libellé distributeur
    valid_ids = {'DI000001', 'DI000002', 'DI000003'}
    return (id_calendrier in valid_ids or 'distributeur' in libelle_calendrier)


def is_valid_data_point(point: dict) -> bool:
    """
    Vérifie si un point de données est valide (iv == 0).

    Args:
        point: Point de données avec 'd', 'v', 'iv'

    Returns:
        bool: True si point valide
    """
    return (
        point.get('d') and
        point.get('v') is not None and
        point.get('iv') == 0
    )


def should_process_grandeur(grandeur: dict) -> bool:
    """
    Vérifie si une grandeur doit être traitée (CONS + EA).

    Args:
        grandeur: Données de grandeur

    Returns:
        bool: True si grandeur à traiter
    """
    return (
        grandeur.get('grandeurMetier') == 'CONS' and
        grandeur.get('grandeurPhysique') == 'EA'
    )


def build_base_record(mesure: dict, contexte: dict, grandeur: dict, header_meta: dict) -> dict:
    """
    Construit l'enregistrement de base avec toutes les métadonnées.

    Args:
        mesure: Données de mesure (PDL)
        contexte: Données de contexte
        grandeur: Données de grandeur
        header_meta: Métadonnées du header

    Returns:
        dict: Enregistrement de base avec métadonnées
    """
    periode = mesure.get('periode', {})

    return {
        # Métadonnées de base
        'pdl': mesure.get('idPrm'),

        # Métadonnées contexte
        'etape_metier': contexte.get('etapeMetier'),
        'contexte_releve': contexte.get('contexteReleve'),
        'type_releve': contexte.get('typeReleve'),

        # Métadonnées grandeur
        'grandeur_physique': grandeur.get('grandeurPhysique'),
        'grandeur_metier': grandeur.get('grandeurMetier'),
        'unite': grandeur.get('unite'),

        # Métadonnées header
        **header_meta
    }


def collect_timeseries_data(mesure: dict, base_record: dict) -> dict:
    """
    Collecte toutes les données de timeseries d'une mesure en format wide.

    Args:
        mesure: Données de mesure complète
        base_record: Enregistrement de base avec métadonnées

    Returns:
        dict: Données par date avec colonnes de cadrans
    """
    values_by_date = {}

    for contexte in mesure.get('contexte', []):
        for grandeur in contexte.get('grandeur', []):
            if not should_process_grandeur(grandeur):
                continue

            for calendrier in grandeur.get('calendrier', []):
                if not is_valid_calendrier(calendrier):
                    continue

                # Traiter les classes temporelles
                for classe in calendrier.get('classeTemporelle', []):
                    id_classe = classe.get('idClasseTemporelle')
                    if not id_classe:
                        continue

                    col_name = id_classe.lower()

                    # Traiter chaque point de données
                    for point in classe.get('valeur', []):
                        if not is_valid_data_point(point):
                            continue

                        date_str = point.get('d')
                        valeur = point.get('v')

                        # Initialiser l'enregistrement pour cette date
                        if date_str not in values_by_date:
                            values_by_date[date_str] = {
                                **base_record,
                                'date_releve': date_str
                            }

                        # Ajouter la valeur pour cette classe
                        values_by_date[date_str][col_name] = valeur

    return values_by_date


def process_single_mesure(mesure: dict, header_meta: dict) -> Iterator[dict]:
    """
    Traite une mesure unique et génère les enregistrements wide.

    Args:
        mesure: Données d'une mesure (PDL)
        header_meta: Métadonnées du header

    Yields:
        dict: Enregistrements wide par date
    """
    pdl = mesure.get('idPrm')
    print(f"📊 Traitement PDL {pdl} - {len(mesure.get('contexte', []))} contexte(s)")

    # Pour trouver le premier contexte/grandeur valide pour les métadonnées
    base_record = None

    for contexte in mesure.get('contexte', []):
        for grandeur in contexte.get('grandeur', []):
            if should_process_grandeur(grandeur):
                base_record = build_base_record(mesure, contexte, grandeur, header_meta)
                break
        if base_record:
            break

    if not base_record:
        print(f"⚠️ Aucune grandeur CONS/EA trouvée pour PDL {pdl}")
        return

    # Collecter toutes les données timeseries
    values_by_date = collect_timeseries_data(mesure, base_record)

    # Générer les enregistrements
    for date_str, row_data in values_by_date.items():
        yield row_data


def r64_timeseries_to_wide_format(
    json_bytes: bytes,
    flux_type: str
) -> Iterator[Dict[str, Any]]:
    """
    Transforme les timeseries R64 en format WIDE pour cohérence avec R15/R151.

    Chaque enregistrement de sortie = 1 PDL + 1 date + tous les cadrans.

    Args:
        json_bytes: Contenu JSON R64 en bytes
        flux_type: Type de flux pour traçabilité

    Yields:
        dict: Enregistrements en format wide par date
    """
    try:
        # Parser JSON R64
        data = json.loads(json_bytes.decode('utf-8'))

        # Extraire métadonnées du header
        header_metadata = extract_header_metadata(data)

        # Traiter chaque mesure (PDL)
        total_records = 0
        for mesure in data.get('mesures', []):
            for record in process_single_mesure(mesure, header_metadata):
                yield record
                total_records += 1

        print(f"✅ R64 transformé en format WIDE: {total_records} enregistrements")

    except json.JSONDecodeError as e:
        print(f"❌ Erreur parsing JSON R64: {e}")
        return
    except Exception as e:
        print(f"❌ Erreur transformation R64: {e}")
        return


def _json_r64_transformer_base(
    extracted_file: dict,
    flux_type: str
) -> Iterator[dict]:
    """
    Transformer de base pour R64 avec gestion des timeseries en format WIDE.

    Args:
        extracted_file: Fichier extrait du transformer archive
        flux_type: Type de flux pour traçabilité

    Yields:
        dict: Enregistrements R64 en format wide avec métadonnées DLT
    """
    # Gérer les deux cas : JSON extrait de ZIP ou JSON direct du SFTP
    if 'source_zip' in extracted_file:
        # Cas 1: JSON extrait d'un ZIP
        zip_name = extracted_file['source_zip']
        zip_modified = extracted_file['modification_date']
        json_name = extracted_file['extracted_file_name']
        json_content = extracted_file['extracted_content']
    else:
        # Cas 2: JSON décrypté du SFTP
        zip_name = extracted_file['file_name']
        zip_modified = extracted_file['modification_date']
        json_name = extracted_file['file_name']
        json_content = extracted_file['decrypted_content']

    try:
        print(f"📄 Parsing JSON R64: {json_name}")

        records_count = 0

        # Transformer les timeseries R64 en format WIDE
        for record in r64_timeseries_to_wide_format(json_content, flux_type):
            # Enrichir avec métadonnées de traçabilité DLT
            enriched_record = {
                **record,
                'modification_date': zip_modified,
                '_source_zip': zip_name,
                '_flux_type': flux_type,
                '_json_name': json_name
            }

            yield enriched_record
            records_count += 1

        print(f"✅ R64 parsé: {records_count} enregistrements WIDE depuis {json_name}")

    except Exception as e:
        print(f"❌ Erreur parsing JSON R64 {json_name}: {e}")
        return


def create_json_r64_transformer(flux_type: str = "R64"):
    """
    Factory pour créer un transformer R64 spécialisé avec format WIDE.

    Args:
        flux_type: Type de flux (R64)

    Returns:
        Transformer DLT configuré pour R64
    """
    @dlt.transformer
    def configured_r64_parser(extracted_file: dict) -> Iterator[dict]:
        return _json_r64_transformer_base(extracted_file, flux_type)

    return configured_r64_parser


