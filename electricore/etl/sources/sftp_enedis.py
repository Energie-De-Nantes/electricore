"""
Source DLT pour les flux Enedis via SFTP avec déchiffrement AES.
Utilise les fonctions pures du module lib/ pour le traitement.
"""

import dlt
from typing import Iterator
from dlt.sources.filesystem import filesystem

# Imports des fonctions pures depuis lib/
from lib.crypto import load_aes_credentials
from lib.processors import process_flux_items


def get_xml_config_resource(flux_type: str, xml_config: dict, zip_pattern: str, sftp_url: str, aes_key: bytes, aes_iv: bytes):
    """
    Générateur optimisé pour une config XML spécifique.
    Les clés AES sont injectées (chargées une seule fois).
    Incrémental appliqué au niveau filesystem pour éviter de télécharger les anciens fichiers.
    """
    # Source filesystem DLT pour SFTP avec incrémental et pattern spécifique
    print(f"🔍 Recherche fichiers pour {xml_config['name']}: {sftp_url} avec pattern {zip_pattern}")
    encrypted_files = filesystem(
        bucket_url=sftp_url,
        file_glob=zip_pattern  # Utiliser le zip_pattern du flux
    ).with_name(f"filesystem_{flux_type.lower()}")  # Nom unique par flux
    
    # Appliquer l'incrémental sur la date de modification du fichier filesystem
    encrypted_files.apply_hints(
        incremental=dlt.sources.incremental("modification_date")
    )
    
    # Utiliser l'orchestrateur refactorisé avec injection des dépendances
    for record in process_flux_items(
        encrypted_files, 
        flux_type, 
        xml_config,  # Passer la xml_config spécifique 
        aes_key, 
        aes_iv
    ):
        yield record


@dlt.source(name="flux_enedis")
def sftp_flux_enedis_multi(flux_config: dict):
    """
    Source DLT multi-ressources pour flux Enedis via SFTP avec déchiffrement AES.
    
    Version refactorisée avec fonctions pures et injection des clés AES.
    Crée une ressource DLT par type de flux configuré.
    Chaque ressource produit une table séparée.
    
    Args:
        flux_config: Configuration des flux depuis config/settings.py
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
    
    # Créer une ressource pour chaque xml_config (nouvelle architecture)
    print("=" * 80)
    print("🚀 CRÉATION DES RESSOURCES DLT")
    print("=" * 80)
    
    for flux_type, flux_config_data in flux_config.items():
        zip_pattern = flux_config_data['zip_pattern']
        xml_configs = flux_config_data['xml_configs']
        
        print(f"\n🏗️  FLUX {flux_type}: {len(xml_configs)} config(s) XML")
        print(f"   📁 Zip pattern: {zip_pattern}")
        
        for xml_config in xml_configs:
            config_name = xml_config['name']
            print(f"   📄 Création ressource: {config_name} (file_regex: {xml_config.get('file_regex', '*')})")
            
            try:
                # Utiliser dlt.resource() avec les dépendances injectées
                resource = dlt.resource(
                    get_xml_config_resource(flux_type, xml_config, zip_pattern, sftp_url, aes_key, aes_iv), 
                    name=config_name,  # Utiliser le nom de la config XML comme nom de table
                    write_disposition="append"  # Données stateless : append seulement
                )
                
                print(f"   ✅ Ressource {config_name} créée avec succès")
                yield resource
                
            except Exception as e:
                print(f"   ❌ ERREUR création ressource {config_name}: {e}")
                raise
    
    print("\n" + "=" * 80)
    print("✅ TOUTES LES RESSOURCES CRÉÉES")
    print("=" * 80)