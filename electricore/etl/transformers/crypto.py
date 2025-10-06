"""
Transformer DLT pour le déchiffrement AES des fichiers Enedis.
Inclut les fonctions pures de cryptographie et le transformer DLT.
"""

import dlt
from typing import Iterator
from dlt.common.storages.fsspec_filesystem import FileItemDict
from Crypto.Cipher import AES


# =============================================================================
# FONCTIONS PURES DE CRYPTOGRAPHIE
# =============================================================================

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


def decrypt_file_aes(encrypted_data: bytes, key: bytes, iv: bytes) -> bytes:
    """
    Déchiffre les données avec AES-CBC.
    Compatible avec la logique electriflux existante.
    
    Args:
        encrypted_data: Données chiffrées à déchiffrer
        key: Clé AES
        iv: Vecteur d'initialisation
    
    Returns:
        bytes: Données déchiffrées
    """
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted_data = cipher.decrypt(encrypted_data)
    
    # Supprimer le padding PKCS7 si présent
    padding_length = decrypted_data[-1]
    if padding_length <= 16:  # Block size AES
        decrypted_data = decrypted_data[:-padding_length]
    
    return decrypted_data


def read_sftp_file(encrypted_item: FileItemDict) -> bytes:
    """
    Lit le contenu d'un fichier depuis SFTP.
    
    Args:
        encrypted_item: Item FileItemDict de DLT
    
    Returns:
        bytes: Contenu du fichier
    """
    with encrypted_item.open() as f:
        return f.read()


# =============================================================================
# TRANSFORMER DLT
# =============================================================================


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
        # print(f"🔓 Déchiffrement: {encrypted_file['file_name']}")
        
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
        
        # print(f"✅ Déchiffré: {original_size} → {decrypted_size} bytes")
        
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