"""
Source DLT pour les flux Enedis via SFTP avec déchiffrement AES.
Utilise les fonctions pures du module lib/ pour le traitement.
"""

import dlt
import io
from typing import Iterator
from dlt.sources.filesystem import filesystem
from dlt.common.storages.fsspec_filesystem import FileItemDict

# Imports des fonctions pures depuis transformers/
from transformers.crypto import load_aes_credentials, decrypt_file_aes, read_sftp_file
from transformers.archive import extract_files_from_zip
from transformers.parsers import match_xml_pattern, xml_to_dict_from_bytes


def create_xml_resource(flux_type: str, xml_config: dict, zip_pattern: str, sftp_url: str, aes_key: bytes, aes_iv: bytes, max_files: int = None):
    """
    Crée une resource DLT qui traite directement un type XML spécifique.
    Pattern DLT recommandé : 1 resource = 1 table
    """
    @dlt.resource(
        name=xml_config['name'],
        write_disposition="append"
    )
    def xml_resource():
        # Source filesystem DLT pour SFTP avec incrémental
        # Nom unique pour chaque resource pour éviter les conflits d'état
        filesystem_name = f"filesystem_{xml_config['name']}"
        print(f"🔍 Recherche fichiers pour {xml_config['name']}: {sftp_url} avec pattern {zip_pattern}")
        
        encrypted_files = filesystem(
            bucket_url=sftp_url,
            file_glob=zip_pattern
        ).with_name(filesystem_name)
        
        # Appliquer l'incrémental sur la date de modification
        encrypted_files.apply_hints(
            incremental=dlt.sources.incremental("modification_date")
        )
        
        # Pour chaque fichier ZIP (limité si max_files spécifié)
        file_count = 0
        for encrypted_item in encrypted_files:
            if max_files and file_count >= max_files:
                print(f"🔄 Limitation atteinte: {max_files} fichiers traités")
                break
            file_count += 1
            try:
                # Déchiffrer et extraire XMLs
                encrypted_data = read_sftp_file(encrypted_item)
                decrypted_data = decrypt_file_aes(encrypted_data, aes_key, aes_iv)
                xml_files = extract_files_from_zip(decrypted_data, '.xml')
                
                # Traiter chaque XML avec la configuration spécifique
                for xml_name, xml_content in xml_files:
                    # Filtrer par file_regex si spécifié
                    if not match_xml_pattern(xml_name, xml_config.get('file_regex')):
                        continue
                    
                    # Traiter XML avec la configuration spécifique
                    for record in xml_to_dict_from_bytes(
                        xml_content,
                        row_level=xml_config['row_level'],
                        metadata_fields=xml_config.get('metadata_fields', {}),
                        data_fields=xml_config.get('data_fields', {}),
                        nested_fields=xml_config.get('nested_fields', [])
                    ):
                        # Enrichir avec métadonnées de traçabilité
                        record.update({
                            '_source_zip': encrypted_item['file_name'],
                            '_flux_type': flux_type,
                            '_xml_name': xml_name,
                            'modification_date': encrypted_item['modification_date']
                        })
                        yield record
                        
            except Exception as e:
                print(f"❌ Erreur traitement {encrypted_item['file_name']}: {e}")
                continue
    
    return xml_resource


def create_csv_transformer(flux_type: str, csv_config: dict, aes_key: bytes, aes_iv: bytes, max_files: int = None):
    """
    Crée un transformer DLT pour déchiffrer et parser les CSV avec Polars.
    Cohérent avec l'architecture Polars du projet.
    """
    
    # Column mapping for R64 CSV files (French to snake_case)
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
    
    @dlt.transformer
    def decrypt_and_extract_csv(items: Iterator[FileItemDict]) -> Iterator[dict]:
        """Déchiffre les ZIP, extrait et parse les CSV avec Polars (plus performant).
        Cohérent avec l'architecture Polars du projet."""
        import polars as pl
        
        file_count = 0
        for encrypted_item in items:
            if max_files and file_count >= max_files:
                print(f"🔄 Limitation CSV atteinte: {max_files} fichiers traités")
                break
            file_count += 1
            try:
                # Déchiffrer et extraire
                encrypted_data = read_sftp_file(encrypted_item)
                decrypted_data = decrypt_file_aes(encrypted_data, aes_key, aes_iv)
                csv_files = extract_files_from_zip(decrypted_data, '.csv')
                
                # Parser chaque CSV avec Polars
                for csv_name, csv_content in csv_files:
                    # Filtrer par file_regex si spécifié
                    if csv_config.get('file_regex'):
                        import fnmatch
                        if not fnmatch.fnmatch(csv_name, csv_config['file_regex']):
                            continue
                    
                    # Parser le CSV avec Polars (plus performant)
                    csv_text = csv_content.decode(csv_config.get('encoding', 'utf-8'))
                    
                    try:
                        df = pl.read_csv(
                            io.StringIO(csv_text),
                            separator=csv_config.get('delimiter', ','),
                            encoding=csv_config.get('encoding', 'utf-8'),
                            # Options pour gérer les valeurs nulles/manquantes
                            null_values=['null', '', 'NULL', 'None'],
                            ignore_errors=True,
                            infer_schema_length=10000
                        )
                        
                        # Apply column renaming for R64 files
                        if flux_type == 'R64' and csv_config.get('name') == 'flux_r64':
                            # Rename columns from French to snake_case
                            df = df.rename(R64_COLUMN_MAPPING)
                        
                        # Ajouter métadonnées de traçabilité
                        df_with_meta = df.with_columns([
                            pl.lit(encrypted_item['modification_date']).alias('modification_date'),
                            pl.lit(encrypted_item['file_name']).alias('_source_zip'),
                            pl.lit(flux_type).alias('_flux_type'),
                            pl.lit(csv_name).alias('_csv_name')
                        ])
                        
                        # Yield chaque ligne comme dictionnaire
                        for row_dict in df_with_meta.to_dicts():
                            yield row_dict
                            
                    except Exception as parse_error:
                        print(f"❌ Erreur parsing Polars {csv_name}: {parse_error}")
                        continue
                    
            except Exception as e:
                print(f"❌ Erreur traitement {encrypted_item['file_name']}: {e}")
                continue
    
    # Retourner seulement le transformer
    return decrypt_and_extract_csv


@dlt.source(name="flux_enedis")
def sftp_flux_enedis_multi(flux_config: dict, max_files: int = None):
    """
    Source DLT multi-ressources pour flux Enedis via SFTP avec déchiffrement AES.
    
    Version refactorisée avec fonctions pures et injection des clés AES.
    Crée une ressource DLT par type de flux configuré.
    Chaque ressource produit une table séparée.
    
    Args:
        flux_config: Configuration des flux depuis config/settings.py
        max_files: Nombre maximal de fichiers à traiter par resource (pour tests rapides)
    """
    # URL SFTP depuis secrets
    sftp_config = dlt.secrets['sftp']
    sftp_url = sftp_config['url']
    file_pattern = sftp_config.get('file_pattern', '**/*.zip')
    
    print(f"🌐 Connexion SFTP : {sftp_url}")
    print(f"📁 Pattern de fichiers : {file_pattern}")
    
    # Charger les clés AES une seule fois (optimisation)
    aes_key, aes_iv = load_aes_credentials()
    print(f"🔐 Clés AES chargées: {len(aes_key)} bytes")
    
    # Créer une resource DLT pour chaque xml_config (pattern recommandé)
    print("=" * 80)
    print("🚀 CRÉATION DES RESSOURCES DLT")
    print("=" * 80)
    
    for flux_type, flux_config_data in flux_config.items():
        zip_pattern = flux_config_data['zip_pattern']
        
        # Gérer xml_configs s'ils existent
        if 'xml_configs' in flux_config_data:
            xml_configs = flux_config_data['xml_configs']
            print(f"\n🏗️  FLUX {flux_type}: {len(xml_configs)} config(s) XML")
            print(f"   📁 Zip pattern: {zip_pattern}")
            
            table_names = [xml_config['name'] for xml_config in xml_configs]
            print(f"   📊 Tables XML cibles: {table_names}")
            
            try:
                for xml_config in xml_configs:
                    xml_resource_factory = create_xml_resource(flux_type, xml_config, zip_pattern, sftp_url, aes_key, aes_iv, max_files)
                    xml_resource = xml_resource_factory()
                    print(f"   ✅ Resource XML {xml_config['name']} créée{f' (max {max_files} fichiers)' if max_files else ''}")
                    yield xml_resource
            except Exception as e:
                print(f"   ❌ ERREUR création flux XML {flux_type}: {e}")
                raise
        
        # Gérer csv_configs s'ils existent
        if 'csv_configs' in flux_config_data:
            csv_configs = flux_config_data['csv_configs']
            print(f"\n🏗️  FLUX {flux_type}: {len(csv_configs)} config(s) CSV")
            print(f"   📁 Zip pattern: {zip_pattern}")
            
            table_names = [csv_config['name'] for csv_config in csv_configs]
            print(f"   📊 Tables CSV cibles: {table_names}")
            
            try:
                for csv_config in csv_configs:
                    # Créer le filesystem source
                    filesystem_name = f"filesystem_{flux_type.lower()}_{csv_config['name']}"
                    encrypted_files = filesystem(
                        bucket_url=sftp_url,
                        file_glob=zip_pattern
                    ).with_name(filesystem_name)
                    
                    # Appliquer l'incrémental sur les fichiers
                    encrypted_files.apply_hints(
                        incremental=dlt.sources.incremental("modification_date")
                    )
                    
                    # Créer le transformer
                    csv_transformer = create_csv_transformer(flux_type, csv_config, aes_key, aes_iv, max_files)
                    
                    # Pipeline simplifié: filesystem -> decrypt et parse directement
                    csv_pipeline = (
                        encrypted_files | 
                        csv_transformer
                    ).with_name(csv_config['name'])
                    
                    # Appliquer la déduplication si des clés primaires sont spécifiées
                    if csv_config.get('primary_key'):
                        csv_pipeline.apply_hints(
                            primary_key=csv_config['primary_key'],
                            write_disposition="merge"
                        )
                    else:
                        csv_pipeline.apply_hints(
                            write_disposition="append"
                        )
                    
                    print(f"   ✅ Pipeline CSV {csv_config['name']} créé avec Polars{f' (max {max_files} fichiers)' if max_files else ''}")
                    yield csv_pipeline
            except Exception as e:
                print(f"   ❌ ERREUR création flux CSV {flux_type}: {e}")
                raise
    
    print("\n" + "=" * 80)
    print("✅ TOUTES LES RESSOURCES CRÉÉES")
    print("=" * 80)