"""
Fonctions pures de cryptographie AES pour le déchiffrement des fichiers Enedis.
Compatible avec la logique electriflux existante.
"""

import dlt
from Crypto.Cipher import AES


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