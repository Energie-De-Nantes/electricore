"""
Fonctions pures de parsing XML avec lxml pour les flux Enedis.
Transforme les fichiers XML en dictionnaires Python selon la configuration.
"""

import re
import fnmatch
from typing import Iterator, Any
from lxml import etree


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