"""
Module d'extraction XML vers dictionnaire Python.
Adaptation de la logique electriflux sans dépendance à pandas.
"""

from pathlib import Path
from typing import Iterator, Dict, Any, Union
import xml.etree.ElementTree as ET


def xml_to_dict(xml_source: Union[Path, str], 
                row_level: str, 
                metadata_fields: Dict[str, str] = None, 
                data_fields: Dict[str, str] = None,
                nested_fields: list = None) -> Iterator[Dict[str, Any]]:
    """
    Convert XML structure to dictionary iterator, handling nested structures and conditions.
    
    Compatible avec DLT : yield des dict directement sans passer par pandas.
    Basé sur la logique electriflux xml_to_dataframe.

    Parameters:
        xml_source: Path vers fichier XML ou file-like object (pour DLT FileItemDict.open())
        row_level: XPath string définissant le niveau des lignes dans le XML
        metadata_fields: Dict des champs de métadonnées (nom -> xpath)
        data_fields: Dict des champs de données (nom -> xpath)  
        nested_fields: Liste de dicts définissant l'extraction de champs imbriqués

    Yields:
        Dict: Dictionnaire représentant une ligne de données
    """
    # Initialiser les paramètres par défaut
    metadata_fields = metadata_fields or {}
    data_fields = data_fields or {}
    nested_fields = nested_fields or []
    
    # Parser le XML - gérer Path ou file-like object
    if isinstance(xml_source, (str, Path)):
        tree = ET.parse(xml_source)
    else:
        # File-like object (cas DLT)
        tree = ET.parse(xml_source)
    
    root = tree.getroot()

    # Extraire les métadonnées une seule fois
    meta: Dict[str, str] = {}
    for field_name, field_xpath in metadata_fields.items():
        field_elem = root.find(field_xpath)
        if field_elem is not None and field_elem.text:
            meta[field_name] = field_elem.text

    # Parcourir chaque ligne et yield un dict
    for row in root.findall(row_level):
        # Extraire les champs de données principaux
        row_data: Dict[str, Any] = {}
        
        for field_name, field_xpath in data_fields.items():
            elem = row.find(field_xpath)
            if elem is not None and elem.text:
                row_data[field_name] = elem.text
        
        # Extraire les champs imbriqués avec conditions
        for nested in nested_fields:
            prefix = nested.get('prefix', '')
            child_path = nested['child_path']
            id_field = nested['id_field'] 
            value_field = nested['value_field']
            conditions = nested.get('conditions', [])
            additional_fields = nested.get('additional_fields', {})

            # Parcourir les éléments enfants
            for nr in row.findall(child_path):
                # Vérifier toutes les conditions
                all_conditions_met = True
                
                for cond in conditions:
                    cond_xpath = cond['xpath']
                    cond_value = cond['value']
                    cond_elem = nr.find(cond_xpath)
                    
                    if cond_elem is None or cond_elem.text != cond_value:
                        all_conditions_met = False
                        break
                
                # Si toutes les conditions sont remplies
                if all_conditions_met:
                    key_elem = nr.find(id_field)
                    value_elem = nr.find(value_field)
                    
                    if key_elem is not None and value_elem is not None:
                        # Ajouter la valeur principale avec préfixe
                        field_key = f"{prefix}{key_elem.text}"
                        row_data[field_key] = value_elem.text
                        
                        # Ajouter les champs additionnels
                        for add_field_name, add_field_xpath in additional_fields.items():
                            add_field_key = f"{prefix}{add_field_name}"
                            
                            # Éviter d'écraser si déjà présent
                            if add_field_key not in row_data:
                                add_elem = nr.find(add_field_xpath)
                                if add_elem is not None and add_elem.text:
                                    row_data[add_field_key] = add_elem.text
        
        # Fusionner métadonnées et données de ligne
        final_record = {**row_data, **meta}
        
        yield final_record


def test_xml_to_dict():
    """Test simple de la fonction avec un XML fictif."""
    
    # Créer un XML de test
    test_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <root>
        <En_Tete_Flux>
            <Unite_Mesure_Index>kWh</Unite_Mesure_Index>
        </En_Tete_Flux>
        <PRM>
            <Id_PRM>12345678901234</Id_PRM>
            <Donnees_Releve>
                <Date_Releve>2024-01-15</Date_Releve>
                <Type_Compteur>LINKY</Type_Compteur>
                <Classe_Temporelle_Distributeur>
                    <Id_Classe_Temporelle>HP</Id_Classe_Temporelle>
                    <Valeur>1500</Valeur>
                </Classe_Temporelle_Distributeur>
                <Classe_Temporelle_Distributeur>
                    <Id_Classe_Temporelle>HC</Id_Classe_Temporelle>
                    <Valeur>800</Valeur>
                </Classe_Temporelle_Distributeur>
            </Donnees_Releve>
        </PRM>
    </root>
    """
    
    # Sauvegarder temporairement
    test_file = Path("test.xml")
    test_file.write_text(test_xml)
    
    try:
        # Config de test (style R15)
        metadata_fields = {"Unité": "En_Tete_Flux/Unite_Mesure_Index"}
        data_fields = {
            "pdl": "Id_PRM",
            "Date_Releve": "Donnees_Releve/Date_Releve", 
            "Type_Compteur": "Donnees_Releve/Type_Compteur"
        }
        nested_fields = [{
            "prefix": "",
            "child_path": "Donnees_Releve/Classe_Temporelle_Distributeur",
            "id_field": "Id_Classe_Temporelle",
            "value_field": "Valeur"
        }]
        
        # Test de la fonction
        records = list(xml_to_dict(
            test_file,
            row_level=".//PRM",
            metadata_fields=metadata_fields,
            data_fields=data_fields,
            nested_fields=nested_fields
        ))
        
        print("✅ Test réussi !")
        print(f"Nombre d'enregistrements: {len(records)}")
        print(f"Premier enregistrement: {records[0]}")
        
    finally:
        # Nettoyer le fichier de test
        test_file.unlink(missing_ok=True)


if __name__ == "__main__":
    test_xml_to_dict()