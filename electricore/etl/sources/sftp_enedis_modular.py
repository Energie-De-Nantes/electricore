"""
Source DLT modulaire avec architecture transformer pour les flux Enedis.
Démonstration avec R151 - Architecture avec chaînage.
"""

import dlt
from typing import Iterator

# Imports des modules créés
from sources.base import create_sftp_resource
from transformers.crypto import create_decrypt_transformer
from transformers.archive import create_unzip_transformer
from transformers.parsers import create_xml_parser_transformer


@dlt.source(name="flux_enedis_modular")
def sftp_flux_enedis_modular(flux_config: dict):
    """
    Source DLT modulaire avec architecture transformer.
    
    Démonstration sur R151 du chaînage :
    SFTP → Decrypt → Unzip → XML Parse → Table
    
    Args:
        flux_config: Configuration des flux depuis config/settings.py
    """
    # Configuration SFTP depuis secrets
    sftp_config = dlt.secrets['sftp']
    sftp_url = sftp_config['url']
    
    print("=" * 80)
    print("🚀 ARCHITECTURE MODULAIRE - CHAÎNAGE DE TRANSFORMERS")
    print("=" * 80)
    
    # Test avec R151 uniquement pour valider l'architecture
    if 'R151' not in flux_config:
        print("⚠️  Configuration R151 non trouvée - test impossible")
        return
    
    r151_config = flux_config['R151']
    zip_pattern = r151_config['zip_pattern']
    xml_configs = r151_config['xml_configs']
    
    print(f"🏗️  TEST FLUX R151 avec architecture modulaire")
    print(f"   📁 Zip pattern: {zip_pattern}")
    print(f"   📊 {len(xml_configs)} config(s) XML")
    
    # Créer les transformers une seule fois (optimisation)
    decrypt_transformer = create_decrypt_transformer()
    unzip_xml_transformer = create_unzip_transformer('.xml')
    
    for xml_config in xml_configs:
        table_name = xml_config['name']
        file_regex = xml_config.get('file_regex')
        
        print(f"\n🔧 Construction pipeline: {table_name}")
        print(f"   🗂️  File regex: {file_regex}")
        print(f"   📄 Row level: {xml_config['row_level']}")
        
        # 1. Resource SFTP de base
        sftp_resource = create_sftp_resource(
            flux_type="R151",
            zip_pattern=zip_pattern, 
            sftp_url=sftp_url
        )
        
        # 2. Transformer de parsing XML configuré
        xml_parser = create_xml_parser_transformer(
            row_level=xml_config['row_level'],
            metadata_fields=xml_config.get('metadata_fields', {}),
            data_fields=xml_config.get('data_fields', {}),
            nested_fields=xml_config.get('nested_fields', []),
            flux_type="R151"
        )
        
        # 3. Transformer d'extraction ZIP configuré avec regex
        unzip_configured = create_unzip_transformer('.xml', file_regex)
        
        # 4. 🎯 CHAÎNAGE DES TRANSFORMERS
        modular_pipeline = (
            sftp_resource |          # SFTP files
            decrypt_transformer |    # AES decrypt  
            unzip_configured |       # ZIP extract + filter
            xml_parser              # XML parse
        ).with_name(table_name)
        
        # 5. Configuration DLT
        modular_pipeline.apply_hints(
            write_disposition="append"
        )
        
        print(f"   ✅ Pipeline créé: SFTP → Decrypt → Unzip → Parse → {table_name}")
        yield modular_pipeline
    
    print("\n" + "=" * 80)
    print("✅ ARCHITECTURE MODULAIRE PRÊTE")
    print("   🔗 Chaînage: SFTP | decrypt | unzip | parse")
    print("   🧪 Testable: Chaque transformer isolé")
    print("   🔄 Réutilisable: Transformers communs à tous les flux")
    print("=" * 80)


# Version simplifiée pour test rapide d'un seul flux R151
@dlt.source(name="test_r151_modular") 
def test_r151_modular():
    """
    Source de test simple pour valider l'architecture modulaire sur R151.
    """
    # Configuration R151 en dur pour le test
    sftp_config = dlt.secrets['sftp']
    sftp_url = sftp_config['url']
    
    # Configuration R151 basique
    r151_xml_config = {
        'name': 'flux_r151_test',
        'file_regex': '*.xml',
        'row_level': './/PRM',
        'metadata_fields': {
            'unite': 'En_Tete_Flux/Unite_Mesure_Index'
        },
        'data_fields': {
            'date_releve': 'Donnees_Releve/Date_Releve',
            'pdl': 'Id_PRM',
            'id_calendrier_fournisseur': 'Donnees_Releve/Id_Calendrier_Fournisseur',
            'id_affaire': 'Donnees_Releve/Id_Affaire'
        },
        'nested_fields': [
            {
                'prefix': '',
                'child_path': 'Donnees_Releve/Classe_Temporelle_Distributeur',
                'id_field': 'Id_Classe_Temporelle',
                'value_field': 'Valeur'
            }
        ]
    }
    
    print("🧪 TEST SIMPLE R151 - Architecture modulaire")
    
    # Construction du pipeline modulaire
    sftp_resource = create_sftp_resource("R151", "**/*_R151_*.zip", sftp_url)
    decrypt_transformer = create_decrypt_transformer()
    unzip_transformer = create_unzip_transformer('.xml', '*.xml')
    xml_parser = create_xml_parser_transformer(
        row_level=r151_xml_config['row_level'],
        metadata_fields=r151_xml_config['metadata_fields'],
        data_fields=r151_xml_config['data_fields'],
        nested_fields=r151_xml_config['nested_fields'],
        flux_type="R151"
    )
    
    # Chaînage des transformers
    test_pipeline = (
        sftp_resource |
        decrypt_transformer |
        unzip_transformer |
        xml_parser
    ).with_name(r151_xml_config['name'])
    
    test_pipeline.apply_hints(write_disposition="append")
    
    print("✅ Pipeline de test R151 créé avec chaînage modulaire")
    
    yield test_pipeline