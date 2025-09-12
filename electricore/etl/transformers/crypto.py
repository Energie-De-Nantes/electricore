"""
Transformer DLT pour le déchiffrement AES des fichiers Enedis.
Réutilisable pour tous les flux chiffrés.
"""

import dlt
from typing import Iterator
from dlt.common.storages.fsspec_filesystem import FileItemDict

# Import des fonctions pures
from lib.crypto import load_aes_credentials, decrypt_file_aes
from lib.processors import read_sftp_file


def _decrypt_aes_transformer_base(
    encrypted_file: FileItemDict,
    aes_key: bytes,
    aes_iv: bytes
) -> Iterator[dict]:
    """
    Fonction de base pour déchiffrer les fichiers AES depuis SFTP.
    
    Args:
        encrypted_file: Fichier chiffré depuis une resource SFTP
        aes_key: Clé AES
        aes_iv: IV AES
    
    Yields:
        dict: {
            'file_name': str,
            'modification_date': datetime,
            'decrypted_content': bytes,
            'original_size': int,
            'decrypted_size': int
        }
    """
    try:
        print(f"🔓 Déchiffrement: {encrypted_file['file_name']}")
        
        # Lire le fichier chiffré depuis SFTP
        encrypted_data = read_sftp_file(encrypted_file)
        original_size = len(encrypted_data)
        
        # Déchiffrer avec AES
        decrypted_data = decrypt_file_aes(encrypted_data, aes_key, aes_iv)
        decrypted_size = len(decrypted_data)
        
        # Yield les données déchiffrées avec métadonnées
        yield {
            'file_name': encrypted_file['file_name'],
            'modification_date': encrypted_file['modification_date'],
            'decrypted_content': decrypted_data,
            'original_size': original_size,
            'decrypted_size': decrypted_size
        }
        
        print(f"✅ Déchiffré: {original_size} → {decrypted_size} bytes")
        
    except Exception as e:
        print(f"❌ Erreur déchiffrement {encrypted_file['file_name']}: {e}")
        return


def create_decrypt_transformer(aes_key: bytes = None, aes_iv: bytes = None):
    """
    Factory pour créer un transformer de déchiffrement avec clés pré-chargées.
    
    Args:
        aes_key: Clé AES (optionnel)
        aes_iv: IV AES (optionnel)
    
    Returns:
        Transformer configuré
    """
    # Charger les clés une seule fois si non fournies
    if aes_key is None or aes_iv is None:
        aes_key, aes_iv = load_aes_credentials()
        print(f"🔐 Clés AES chargées dans factory: {len(aes_key)} bytes")
    
    @dlt.transformer
    def configured_decrypt_transformer(encrypted_file: FileItemDict) -> Iterator[dict]:
        return _decrypt_aes_transformer_base(encrypted_file, aes_key, aes_iv)
    
    return configured_decrypt_transformer