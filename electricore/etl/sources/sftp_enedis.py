"""
Source DLT pour les flux Enedis via SFTP avec architecture modulaire.
Utilise le chaînage de transformers DLT pour une architecture propre.
"""

import dlt
import re
from typing import Iterator
from dlt.sources.filesystem import filesystem

def mask_password_in_url(url: str) -> str:
    """
    Masque le mot de passe dans une URL SFTP pour les logs.

    Args:
        url: URL SFTP avec mot de passe

    Returns:
        URL avec mot de passe masqué

    Example:
        >>> mask_password_in_url("sftp://user:pass@host:22/path")
        "sftp://user:****@host:22/path"
    """
    # Pattern pour capturer: protocol://user:password@host
    pattern = r'(sftp://[^:]+:)[^@]+(@.+)'
    replacement = r'\1****\2'
    return re.sub(pattern, replacement, url)


# Imports des transformers modulaires
from electricore.etl.transformers.crypto import create_decrypt_transformer
from electricore.etl.transformers.archive import create_unzip_transformer
from electricore.etl.transformers.parsers import (
    create_xml_parser_transformer,
    create_csv_parser_transformer,
    create_json_parser_transformer,
    create_json_r64_transformer
)


def create_sftp_resource(flux_type: str, table_name: str, file_pattern: str, sftp_url: str, max_files: int = None):
    """
    Crée une resource SFTP réutilisable avec limitation optionnelle.

    Args:
        flux_type: Type de flux (R15, C15, etc.)
        table_name: Nom de la table cible (pour un état incrémental unique)
        file_pattern: Pattern pour les fichiers (ZIP ou JSON directs)
        sftp_url: URL du serveur SFTP
        max_files: Nombre max de fichiers à traiter
    """
    @dlt.resource(
        name=f"sftp_files_{table_name}",  # Nom unique par table pour état incrémental indépendant
        write_disposition="append"
    )
    def sftp_files_resource():
        print(f"🔍 SFTP {flux_type}: {mask_password_in_url(sftp_url)} avec pattern {file_pattern}")

        files = filesystem(
            bucket_url=sftp_url,
            file_glob=file_pattern
        ).with_name(f"filesystem_{table_name}")  # Nom unique pour état incrémental indépendant

        # Appliquer l'incrémental sur la date de modification
        files.apply_hints(
            incremental=dlt.sources.incremental("modification_date")
        )

        # Limiter le nombre de fichiers si spécifié
        file_count = 0
        for file_item in files:
            if max_files and file_count >= max_files:
                print(f"🔄 Limitation atteinte: {max_files} fichiers traités")
                break
            file_count += 1
            yield file_item

    return sftp_files_resource


@dlt.source(name="flux_enedis")
def flux_enedis(flux_config: dict, max_files: int = None):
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
    print(f"🌐 SFTP: {mask_password_in_url(sftp_url)}")
    
    # Créer les transformers communs une seule fois (optimisation)
    decrypt_transformer = create_decrypt_transformer()
    
    # Traiter chaque type de flux
    for flux_type, flux_config_data in flux_config.items():
        file_pattern = flux_config_data['file_pattern']

        print(f"\n🏗️  FLUX {flux_type}")
        print(f"   📁 Pattern: {file_pattern}")

        # === FLUX XML ===
        if 'xml_configs' in flux_config_data:
            xml_configs = flux_config_data['xml_configs']
            print(f"   📄 {len(xml_configs)} config(s) XML")

            for xml_config in xml_configs:
                table_name = xml_config['name']
                file_regex = xml_config.get('file_regex', '*.xml')

                print(f"   🔧 Pipeline XML: {table_name}")

                # 1. Resource SFTP
                sftp_resource = create_sftp_resource(flux_type, table_name, file_pattern, sftp_url, max_files)
                
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
                sftp_resource = create_sftp_resource(flux_type, table_name, file_pattern, sftp_url, max_files)
                
                # 2. Transformer unzip configuré
                unzip_transformer = create_unzip_transformer('.csv', file_regex)
                
                # 3. Transformer CSV parser configuré
                column_mapping = csv_config.get('column_mapping', {})
                
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

        # === FLUX JSON ===
        if 'json_configs' in flux_config_data:
            json_configs = flux_config_data['json_configs']
            print(f"   🔧 {len(json_configs)} config(s) JSON")

            for json_config in json_configs:
                table_name = json_config['name']
                file_regex = json_config.get('file_regex', '*.json')
                transformer_type = json_config.get('transformer_type', 'standard')
                primary_key = json_config.get('primary_key', [])

                print(f"   🔧 Pipeline JSON: {table_name} (type: {transformer_type})")

                # 1. Resource SFTP
                sftp_resource = create_sftp_resource(flux_type, table_name, file_pattern, sftp_url, max_files)

                # 2. Transformer unzip configuré
                unzip_transformer = create_unzip_transformer('.json', file_regex)

                # 3. Choisir le transformer JSON selon le type
                if transformer_type == 'r64_timeseries':
                    # Transformer R64 spécialisé pour timeseries en format WIDE
                    json_parser = create_json_r64_transformer(flux_type=flux_type)
                    print(f"   📊 Utilisation transformer R64 spécialisé (format WIDE)")
                else:
                    # Transformer JSON générique
                    record_path = json_config['record_path']
                    json_parser = create_json_parser_transformer(
                        record_path=record_path,
                        metadata_fields=json_config.get('metadata_fields', {}),
                        data_fields=json_config.get('data_fields', {}),
                        nested_fields=json_config.get('nested_fields', []),
                        flux_type=flux_type
                    )
                    print(f"   📄 Utilisation transformer JSON standard")

                # 4. 🎯 CHAÎNAGE COMPLET JSON
                json_pipeline = (
                    sftp_resource |
                    decrypt_transformer |
                    unzip_transformer |
                    json_parser
                ).with_name(table_name)

                # 5. Configuration DLT avec déduplication si clé primaire
                if primary_key:
                    json_pipeline.apply_hints(
                        primary_key=primary_key,
                        write_disposition="merge"
                    )
                else:
                    json_pipeline.apply_hints(write_disposition="append")

                print(f"   ✅ {table_name}: SFTP | decrypt | unzip | parse")
                if primary_key:
                    print(f"   🔑 Clé primaire: {primary_key}")

                yield json_pipeline

    print("\n" + "=" * 80)
    print("✅ ARCHITECTURE REFACTORÉE COMPLÈTE")
    print("   🔗 Chaînage unifié pour tous les flux")
    print("   🧪 Chaque transformer testable isolément")
    print("   🔄 Transformers réutilisés entre flux")
    print("   ⚡ Optimisé: clés AES chargées une seule fois")
    print("=" * 80)


