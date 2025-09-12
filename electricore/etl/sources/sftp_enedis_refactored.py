"""
Source DLT refactorée avec architecture modulaire complète.
Migration de tous les flux vers l'architecture transformer.
"""

import dlt
from typing import Iterator

# Imports des modules modulaires
from sources.base import create_sftp_resource
from transformers.crypto import create_decrypt_transformer
from transformers.archive import create_unzip_transformer
from transformers.parsers import (
    create_xml_parser_transformer,
    create_csv_parser_transformer,
    R64_COLUMN_MAPPING
)


@dlt.source(name="flux_enedis_refactored")
def sftp_flux_enedis_refactored(flux_config: dict):
    """
    Source DLT refactorée avec architecture modulaire pour tous les flux Enedis.
    
    Architecture unifiée :
    - XML: SFTP → Decrypt → Unzip → XML Parse → Table
    - CSV: SFTP → Decrypt → Unzip → CSV Parse → Table
    
    Args:
        flux_config: Configuration des flux depuis config/settings.py
    """
    # Configuration SFTP depuis secrets
    sftp_config = dlt.secrets['sftp']
    sftp_url = sftp_config['url']
    
    print("=" * 80)
    print("🚀 ARCHITECTURE REFACTORÉE - TOUS LES FLUX")
    print("=" * 80)
    print(f"🌐 SFTP: {sftp_url}")
    
    # Créer les transformers communs une seule fois (optimisation)
    decrypt_transformer = create_decrypt_transformer()
    
    # Traiter chaque type de flux
    for flux_type, flux_config_data in flux_config.items():
        zip_pattern = flux_config_data['zip_pattern']
        
        print(f"\n🏗️  FLUX {flux_type}")
        print(f"   📁 Pattern: {zip_pattern}")
        
        # === FLUX XML ===
        if 'xml_configs' in flux_config_data:
            xml_configs = flux_config_data['xml_configs']
            print(f"   📄 {len(xml_configs)} config(s) XML")
            
            for xml_config in xml_configs:
                table_name = xml_config['name']
                file_regex = xml_config.get('file_regex', '*.xml')
                
                print(f"   🔧 Pipeline XML: {table_name}")
                
                # 1. Resource SFTP
                sftp_resource = create_sftp_resource(flux_type, zip_pattern, sftp_url)
                
                # 2. Transformer unzip configuré pour ce flux
                unzip_transformer = create_unzip_transformer('.xml', file_regex)
                
                # 3. Transformer XML parser configuré
                xml_parser = create_xml_parser_transformer(
                    row_level=xml_config['row_level'],
                    metadata_fields=xml_config.get('metadata_fields', {}),
                    data_fields=xml_config.get('data_fields', {}),
                    nested_fields=xml_config.get('nested_fields', []),
                    flux_type=flux_type
                )
                
                # 4. 🎯 CHAÎNAGE MODULAIRE
                xml_pipeline = (
                    sftp_resource |
                    decrypt_transformer |
                    unzip_transformer |
                    xml_parser
                ).with_name(table_name)
                
                # 5. Configuration DLT
                xml_pipeline.apply_hints(write_disposition="append")
                
                print(f"   ✅ {table_name}: SFTP | decrypt | unzip | parse")
                yield xml_pipeline
        
        # === FLUX CSV ===
        if 'csv_configs' in flux_config_data:
            csv_configs = flux_config_data['csv_configs']
            print(f"   📊 {len(csv_configs)} config(s) CSV")
            
            for csv_config in csv_configs:
                table_name = csv_config['name']
                file_regex = csv_config.get('file_regex', '*.csv')
                delimiter = csv_config.get('delimiter', ',')
                encoding = csv_config.get('encoding', 'utf-8')
                primary_key = csv_config.get('primary_key', [])
                
                print(f"   🔧 Pipeline CSV: {table_name}")
                
                # 1. Resource SFTP
                sftp_resource = create_sftp_resource(flux_type, zip_pattern, sftp_url)
                
                # 2. Transformer unzip configuré
                unzip_transformer = create_unzip_transformer('.csv', file_regex)
                
                # 3. Transformer CSV parser configuré
                # Utiliser le mapping R64 si c'est du R64
                column_mapping = R64_COLUMN_MAPPING if flux_type == 'R64' else None
                
                csv_parser = create_csv_parser_transformer(
                    delimiter=delimiter,
                    encoding=encoding,
                    flux_type=flux_type,
                    column_mapping=column_mapping
                )
                
                # 4. 🎯 CHAÎNAGE MODULAIRE
                csv_pipeline = (
                    sftp_resource |
                    decrypt_transformer |
                    unzip_transformer |
                    csv_parser
                ).with_name(table_name)
                
                # 5. Configuration DLT avec déduplication si clé primaire
                if primary_key:
                    csv_pipeline.apply_hints(
                        primary_key=primary_key,
                        write_disposition="merge"
                    )
                else:
                    csv_pipeline.apply_hints(write_disposition="append")
                
                print(f"   ✅ {table_name}: SFTP | decrypt | unzip | parse")
                if primary_key:
                    print(f"   🔑 Clé primaire: {primary_key}")
                
                yield csv_pipeline
    
    print("\n" + "=" * 80)
    print("✅ ARCHITECTURE REFACTORÉE COMPLÈTE")
    print("   🔗 Chaînage unifié pour tous les flux")
    print("   🧪 Chaque transformer testable isolément")
    print("   🔄 Transformers réutilisés entre flux")
    print("   ⚡ Optimisé: clés AES chargées une seule fois")
    print("=" * 80)


# Version de migration douce - garde l'ancienne interface
@dlt.source(name="flux_enedis")
def sftp_flux_enedis_multi_v2(flux_config: dict):
    """
    Version de migration - utilise la nouvelle architecture
    mais garde le même nom de source pour compatibilité.
    """
    print("🔄 MIGRATION: Utilisation de l'architecture refactorée")
    return sftp_flux_enedis_refactored(flux_config)