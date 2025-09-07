"""
Pipeline ETL SFTP avec déchiffrement AES - Version multi-ressources.
Crée une ressource DLT par type de flux pour des tables séparées.
"""

import dlt
import tempfile
import zipfile
import yaml
from pathlib import Path
from typing import Iterator
from Crypto.Cipher import AES
from dlt.sources.filesystem import filesystem, FileItemDict
from xml_to_dict import xml_to_dict

# Configuration
ETL_DIR = Path(__file__).parent
CONFIG_FILE = ETL_DIR / "simple_flux.yaml"

# Charger la configuration YAML
with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
    FLUX_CONFIG = yaml.safe_load(f)

print(f"📋 Configuration chargée pour {len(FLUX_CONFIG)} types de flux: {list(FLUX_CONFIG.keys())}")


def load_aes_credentials() -> tuple[bytes, bytes]:
    """
    Charge les clés AES depuis les secrets DLT.
    
    Returns:
        Tuple[bytes, bytes]: (aes_key, aes_iv)
    
    Raises:
        ValueError: Si les clés AES ne peuvent pas être chargées
    """
    try:
        aes_config = dlt.secrets['aes']
        aes_key = bytes.fromhex(aes_config['key'])
        aes_iv = bytes.fromhex(aes_config['iv'])
        return aes_key, aes_iv
    except Exception as e:
        raise ValueError(f"Erreur chargement clés AES depuis secrets: {e}")


def should_process_zip(zip_name: str, flux_type: str) -> bool:
    """
    Détermine si un ZIP doit être traité pour ce type de flux.
    
    Args:
        zip_name: Nom du fichier ZIP
        flux_type: Type de flux attendu (C15, R15, etc.)
    
    Returns:
        bool: True si le ZIP correspond au type de flux
    """
    detected_type = detect_flux_type_from_zip_name(zip_name)
    return detected_type == flux_type


def create_zip_key(zip_name: str, modified_date: str) -> str:
    """
    Crée une clé unique pour éviter les doublons.
    
    Args:
        zip_name: Nom du fichier ZIP
        modified_date: Date de modification du fichier
    
    Returns:
        str: Clé unique basée sur nom + date de modification
    """
    return f"{zip_name}_{modified_date}"


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
    
    import re
    import fnmatch
    
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


def should_process_xml(xml_name: str, flux_config: dict) -> bool:
    """
    Détermine si un XML doit être traité selon la configuration.
    
    Args:
        xml_name: Nom du fichier XML
        flux_config: Configuration du flux
    
    Returns:
        bool: True si le XML doit être traité
    """
    file_regex = flux_config.get('file_regex')
    return match_xml_pattern(xml_name, file_regex)


def decrypt_file_aes(encrypted_data: bytes, key: bytes, iv: bytes) -> bytes:
    """
    Déchiffre les données avec AES-CBC.
    Compatible avec la logique electriflux existante.
    """
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted_data = cipher.decrypt(encrypted_data)
    
    # Supprimer le padding PKCS7 si présent
    padding_length = decrypted_data[-1]
    if padding_length <= 16:  # Block size AES
        decrypted_data = decrypted_data[:-padding_length]
    
    return decrypted_data


def detect_flux_type_from_zip_name(zip_name: str) -> str:
    """
    Détecte le type de flux selon le nom du fichier ZIP.
    Basé sur les conventions Enedis.
    """
    zip_name_upper = zip_name.upper()
    
    if '_C15_' in zip_name_upper or '_C12_' in zip_name_upper:
        return 'C15'  # C12 et C15 sont traités de la même façon
    elif '_R151_' in zip_name_upper:
        return 'R151'
    elif '_R15_' in zip_name_upper and '_ACC' not in zip_name_upper:
        return 'R15'
    elif '_F12_' in zip_name_upper:
        return 'F12'
    elif '_F15_' in zip_name_upper:
        return 'F15'
    elif '_ACC' in zip_name_upper or 'ACC_' in zip_name_upper:
        return 'R15_ACC'
    else:
        return 'UNKNOWN'


def extract_xml_files_from_zip(zip_data: bytes) -> list[tuple[str, bytes]]:
    """
    Extrait les fichiers XML d'un ZIP en mémoire.
    
    Args:
        zip_data: Contenu du fichier ZIP
    
    Returns:
        List[Tuple[str, bytes]]: Liste de (nom_fichier, contenu_xml)
    
    Raises:
        zipfile.BadZipFile: Si le ZIP est corrompu
    """
    import io
    import zipfile
    
    xml_files = []
    
    with zipfile.ZipFile(io.BytesIO(zip_data), 'r') as zip_ref:
        for file_info in zip_ref.filelist:
            if file_info.filename.lower().endswith('.xml') and not file_info.is_dir():
                try:
                    xml_content = zip_ref.read(file_info.filename)
                    xml_files.append((file_info.filename, xml_content))
                except Exception as e:
                    print(f"⚠️ Erreur lecture XML {file_info.filename}: {e}")
                    continue
    
    return xml_files


def process_xml_content(
    xml_content: bytes,
    xml_name: str,
    flux_config: dict
) -> Iterator[dict]:
    """
    Transforme le contenu XML en enregistrements selon la configuration.
    
    Args:
        xml_content: Contenu binaire du fichier XML
        xml_name: Nom du fichier XML
        flux_config: Configuration du flux
    
    Yields:
        dict: Enregistrements extraits du XML
    """
    import io
    
    # Créer un objet file-like en mémoire
    xml_file = io.BytesIO(xml_content)
    
    # Utiliser xml_to_dict avec l'objet file-like
    # Note: xml_to_dict doit être modifié pour accepter un file-like object
    # ou on peut créer une version xml_to_dict_from_bytes
    for record in xml_to_dict_from_bytes(
        xml_content,
        row_level=flux_config['row_level'],
        metadata_fields=flux_config.get('metadata_fields', {}),
        data_fields=flux_config.get('data_fields', {}),
        nested_fields=flux_config.get('nested_fields', [])
    ):
        yield record


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
    from lxml import etree
    from typing import Any
    
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


def enrich_record(
    record: dict,
    zip_name: str,
    zip_modified: str,
    flux_type: str,
    xml_name: str
) -> dict:
    """
    Ajoute les métadonnées de traçabilité à un enregistrement.
    
    Args:
        record: Enregistrement à enrichir
        zip_name: Nom du fichier ZIP source
        zip_modified: Date de modification du ZIP
        flux_type: Type de flux
        xml_name: Nom du fichier XML source
    
    Returns:
        dict: Nouvel enregistrement enrichi (copie)
    """
    enriched = record.copy()
    enriched.update({
        '_source_zip': zip_name,
        '_zip_modified': zip_modified,
        '_flux_type': flux_type,
        '_xml_name': xml_name
    })
    return enriched


def read_sftp_file(encrypted_item) -> bytes:
    """
    Lit le contenu d'un fichier depuis SFTP.
    
    Args:
        encrypted_item: Item FileItemDict de DLT
    
    Returns:
        bytes: Contenu du fichier
    """
    with encrypted_item.open() as f:
        return f.read()


def log_processing_info(flux_type: str, zip_name: str, record_count: int):
    """Log les informations de traitement."""
    print(f"🔐 {flux_type}: Déchiffrement de {zip_name}")
    if record_count > 0:
        print(f"📊 {flux_type} - {zip_name}: {record_count} enregistrements")


def log_xml_info(xml_name: str, record_count: int):
    """Log les informations de traitement XML."""
    if record_count > 0:
        print(f"   ✅ {xml_name}: {record_count} enregistrements")


def log_error(zip_name: str, error: Exception):
    """Log les erreurs de traitement."""
    if isinstance(error, zipfile.BadZipFile):
        print(f"❌ ZIP corrompu {zip_name}: {error}")
    else:
        print(f"❌ Erreur {zip_name}: {error}")


def process_flux_items(
    items: Iterator[FileItemDict],
    flux_type: str,
    flux_config: dict,
    aes_key: bytes,
    aes_iv: bytes
) -> Iterator[dict]:
    """
    Orchestrateur principal refactorisé avec fonctions pures.
    
    Args:
        items: Itérateur des fichiers SFTP
        flux_type: Type de flux à traiter
        flux_config: Configuration du flux
        aes_key: Clé AES (injectée)
        aes_iv: IV AES (injecté)
    
    Yields:
        dict: Enregistrements enrichis avec métadonnées
    """
    processed_zips = set()
    total_records = 0
    
    for encrypted_item in items:
        # 1. Filtrage par type de flux
        if not should_process_zip(encrypted_item['file_name'], flux_type):
            continue
        
        # 2. Déduplication
        zip_key = create_zip_key(encrypted_item['file_name'], encrypted_item['modification_date'])
        if zip_key in processed_zips:
            continue
        processed_zips.add(zip_key)
        
        # Log de début de traitement
        log_processing_info(flux_type, encrypted_item['file_name'], 0)
        zip_records = 0
        
        try:
            # 3. Lecture et déchiffrement
            encrypted_data = read_sftp_file(encrypted_item)
            decrypted_data = decrypt_file_aes(encrypted_data, aes_key, aes_iv)
            
            # 4. Extraction des XMLs en mémoire
            xml_files = extract_xml_files_from_zip(decrypted_data)
            
            # 5. Traitement de chaque XML
            for xml_name, xml_content in xml_files:
                # Filtrer par regex si configuré
                if not should_process_xml(xml_name, flux_config):
                    continue
                
                xml_record_count = 0
                # Parser avec lxml directement depuis bytes
                for record in xml_to_dict_from_bytes(
                    xml_content,
                    row_level=flux_config['row_level'],
                    metadata_fields=flux_config.get('metadata_fields', {}),
                    data_fields=flux_config.get('data_fields', {}),
                    nested_fields=flux_config.get('nested_fields', [])
                ):
                    # Enrichir avec métadonnées de traçabilité
                    enriched_record = enrich_record(
                        record,
                        encrypted_item['file_name'],
                        encrypted_item['modification_date'],
                        flux_type,
                        xml_name
                    )
                    
                    xml_record_count += 1
                    zip_records += 1
                    total_records += 1
                    
                    yield enriched_record
                
                # Log du XML traité
                log_xml_info(xml_name, xml_record_count)
            
            # Log du ZIP traité
            if zip_records > 0:
                print(f"📊 {flux_type} - {encrypted_item['file_name']}: {zip_records} enregistrements")
                
        except zipfile.BadZipFile as e:
            log_error(encrypted_item['file_name'], e)
            continue
        except Exception as e:
            log_error(encrypted_item['file_name'], e)
            continue
    
    # Log final
    print(f"🎯 {flux_type}: {total_records} enregistrements totaux")


# Fonction de compatibilité pour l'ancien nom
def decrypt_extract_flux_logic(items: Iterator[FileItemDict], flux_type: str) -> Iterator[dict]:
    """
    Fonction de compatibilité - utilise l'orchestrateur refactorisé.
    
    DEPRECATED: Utiliser process_flux_items() directement avec injection des clés AES.
    """
    # Charger les clés AES (approche legacy)
    aes_key, aes_iv = load_aes_credentials()
    flux_config = FLUX_CONFIG[flux_type]
    
    return process_flux_items(items, flux_type, flux_config, aes_key, aes_iv)


@dlt.source
def sftp_flux_enedis_multi():
    """
    Source DLT multi-ressources pour flux Enedis via SFTP avec déchiffrement AES.
    
    Version refactorisée avec fonctions pures et injection des clés AES.
    Crée une ressource DLT par type de flux configuré.
    Chaque ressource produit une table séparée.
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
    
    def get_flux_resource(flux_type: str, flux_config: dict):
        """
        Générateur optimisé pour un type de flux spécifique.
        Les clés AES sont injectées (chargées une seule fois).
        """
        # Source filesystem DLT pour SFTP
        encrypted_files = filesystem(
            bucket_url=sftp_url,
            file_glob=file_pattern
        )
        
        # Utiliser l'orchestrateur refactorisé avec injection des dépendances
        for record in process_flux_items(
            encrypted_files, 
            flux_type, 
            flux_config, 
            aes_key, 
            aes_iv
        ):
            yield record
    
    # Créer une ressource pour chaque type de flux (pattern de la doc officielle)
    for flux_type, config in FLUX_CONFIG.items():
        print(f"🏗️  Création de la ressource pour {flux_type}")
        
        resource_name = f"flux_{flux_type.lower()}"
        
        # Utiliser dlt.resource() avec les dépendances injectées
        resource = dlt.resource(
            get_flux_resource(flux_type, config), 
            name=resource_name
        )
        
        # Appliquer l'incrémental pour éviter les doublons
        resource.apply_hints(
            incremental=dlt.sources.incremental("_zip_modified")
        )
        
        yield resource


def run_sftp_multi_pipeline():
    """
    Exécute le pipeline SFTP multi-ressources.
    """
    print("🚀 Démarrage du pipeline SFTP + AES + DLT (Multi-ressources)")
    print("=" * 70)
    
    # Créer le pipeline DLT
    pipeline = dlt.pipeline(
        pipeline_name="flux_enedis_sftp_multi",
        destination="duckdb",
        dataset_name="enedis_multi"
    )
    
    try:
        # Exécuter le chargement
        load_info = pipeline.run(sftp_flux_enedis_multi())
        
        print("✅ Pipeline SFTP multi-ressources exécuté avec succès !")
        print(f"📊 Résultats du chargement:")
        print(load_info)
        
        # Vérifier les résultats
        verify_multi_results()
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de l'exécution du pipeline SFTP multi: {e}")
        return False


def verify_multi_results():
    """
    Vérifie et affiche les résultats du pipeline SFTP multi-ressources.
    """
    print("\\n📈 Vérification des résultats SFTP multi dans DuckDB...")
    
    try:
        import duckdb
        conn = duckdb.connect('flux_enedis_sftp_multi.duckdb')
        
        # Lister les tables créées
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'enedis_multi'"
        ).fetchall()
        
        if not tables:
            print("❌ Aucune table trouvée dans le schéma enedis_multi")
            return
        
        print(f"📋 Tables créées: {[t[0] for t in tables]}")
        
        # Statistiques par table
        total_records = 0
        for table_name, in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM enedis_multi.{table_name}").fetchone()[0]
                print(f"   📊 {table_name}: {count:,} enregistrements")
                total_records += count
                
                # Statistiques par ZIP source pour les tables de flux
                if table_name.startswith('flux_'):
                    zip_stats = conn.execute(f"""
                        SELECT _source_zip, COUNT(*) as records 
                        FROM enedis_multi.{table_name} 
                        WHERE _source_zip IS NOT NULL
                        GROUP BY _source_zip 
                        ORDER BY records DESC 
                        LIMIT 3
                    """).fetchall()
                    
                    if zip_stats:
                        print("   📦 Top 3 ZIP sources :")
                        for zip_name, count in zip_stats:
                            print(f"      {zip_name}: {count:,} records")
                
            except Exception as e:
                print(f"   ❌ Erreur pour table {table_name}: {e}")
        
        print(f"\\n🎯 Total: {total_records:,} enregistrements chargés")
        conn.close()
        
    except Exception as e:
        print(f"❌ Erreur lors de la vérification SFTP multi: {e}")


if __name__ == "__main__":
    print("🔧 Pipeline ETL SFTP multi-ressources avec déchiffrement AES")
    print("=" * 70)
    
    # Vérifier la configuration
    try:
        secrets = dlt.secrets
        sftp_config = secrets['sftp']
        aes_config = secrets['aes']
        print("✅ Configuration SFTP et AES trouvée")
        print(f"   SFTP URL configuré: {sftp_config['url'][:20]}...")
        print(f"   Clé AES configurée: {len(aes_config['key'])} caractères")
    except Exception as e:
        print(f"❌ Configuration manquante: {e}")
        print("📝 Configurez .dlt/secrets.toml avec les sections [sftp] et [aes]")
        exit(1)
    
    # Exécuter le pipeline
    success = run_sftp_multi_pipeline()
    
    if success:
        print("\\n🎉 Pipeline SFTP multi-ressources terminé avec succès !")
    else:
        print("\\n💥 Pipeline SFTP multi-ressources terminé avec des erreurs")