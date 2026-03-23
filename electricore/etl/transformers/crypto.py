"""
Transformer DLT pour le déchiffrement AES des fichiers Enedis.
Inclut les fonctions pures de cryptographie et le transformer DLT.

Gestion de la rotation de clés :
  Les clés AES Enedis sont rotées périodiquement. Le format secrets.toml supporte
  plusieurs clés simultanément pour couvrir la période de transition.

  Format recommandé (v2) :
      [aes.current]
      key = "nouvelle_clé_hex"
      iv  = "nouvel_iv_hex"

      [aes.previous]           # optionnel, gardé pendant ~4 semaines après rotation
      key = "ancienne_clé_hex"
      iv  = "ancien_iv_hex"

  Format hérité (v1, toujours supporté) :
      [aes]
      key = "clé_hex"
      iv  = "iv_hex"

  Lors du déchiffrement, la clé `current` est essayée en premier. Si elle échoue,
  `previous` est tentée. Un log indique quelle clé a fonctionné pour faciliter
  le diagnostic lors d'une transition.
"""

import dlt
from typing import Iterator
from dlt.common.storages.fsspec_filesystem import FileItemDict
from Crypto.Cipher import AES


# Magic bytes d'un fichier ZIP (PK local file header)
_ZIP_MAGIC = b'PK\x03\x04'


# =============================================================================
# FONCTIONS PURES DE CRYPTOGRAPHIE
# =============================================================================

def decrypt_file_aes(encrypted_data: bytes, key: bytes, iv: bytes) -> bytes:
    """
    Déchiffre des données AES-128-CBC et valide le résultat.

    La validation échoue tôt (avant l'étape unzip) si la clé est incorrecte,
    en vérifiant :
      1. Le padding PKCS7 complet (les N derniers octets valent tous N)
      2. Les magic bytes ZIP (PK\\x03\\x04) — tous les fichiers Enedis sont des ZIPs

    Args:
        encrypted_data: Données chiffrées (doit être un multiple de 16 octets)
        key: Clé AES (16 octets pour AES-128)
        iv: Vecteur d'initialisation (16 octets)

    Returns:
        bytes: Données déchiffrées, padding retiré

    Raises:
        ValueError: Si le padding PKCS7 est invalide ou si le résultat n'est pas un ZIP
                    (indique une clé ou IV incorrects)
    """
    cipher = AES.new(key, AES.MODE_CBC, iv)
    raw = cipher.decrypt(encrypted_data)

    # Validation PKCS7 complète : les N derniers octets doivent tous valoir N
    n = raw[-1]
    if not (1 <= n <= 16) or any(b != n for b in raw[-n:]):
        raise ValueError(
            f"Clé AES incorrecte : padding PKCS7 invalide (dernier octet = {n})"
        )
    decrypted = raw[:-n]

    # Vérification magic bytes ZIP — tous les fichiers Enedis sont des archives ZIP
    if len(decrypted) < 4 or decrypted[:4] != _ZIP_MAGIC:
        magic = decrypted[:4].hex() if len(decrypted) >= 4 else decrypted.hex()
        raise ValueError(
            f"Clé AES incorrecte : résultat non-ZIP (magic bytes = {magic})"
        )

    return decrypted


def load_aes_key_chain() -> list[tuple[str, bytes, bytes]]:
    """
    Charge la chaîne de clés AES depuis les secrets DLT.

    Supporte deux formats de secrets.toml :
      - v2 (recommandé) : [aes.current] + [aes.previous] optionnel
      - v1 (hérité)     : [aes] avec key/iv directs

    Returns:
        Liste de tuples (nom, clé, iv) dans l'ordre de tentative.
        La clé `current` (ou la clé unique en v1) est toujours en premier.

    Raises:
        ValueError: Si aucune clé valide ne peut être chargée
    """
    try:
        aes_config = dlt.secrets['aes']
    except Exception as e:
        raise ValueError(f"Section [aes] absente des secrets DLT : {e}")

    chain: list[tuple[str, bytes, bytes]] = []

    # Format v2 : sous-sections current / previous
    if 'current' in aes_config:
        try:
            key = bytes.fromhex(aes_config['current']['key'])
            iv = bytes.fromhex(aes_config['current']['iv'])
            chain.append(('current', key, iv))
        except Exception as e:
            raise ValueError(f"Erreur chargement [aes.current] : {e}")

        if 'previous' in aes_config:
            try:
                key = bytes.fromhex(aes_config['previous']['key'])
                iv = bytes.fromhex(aes_config['previous']['iv'])
                chain.append(('previous', key, iv))
            except Exception as e:
                raise ValueError(f"Erreur chargement [aes.previous] : {e}")

        return chain

    # Format v1 hérité : clé/iv directement dans [aes]
    if 'key' in aes_config and 'iv' in aes_config:
        try:
            key = bytes.fromhex(aes_config['key'])
            iv = bytes.fromhex(aes_config['iv'])
            chain.append(('legacy', key, iv))
        except Exception as e:
            raise ValueError(f"Erreur chargement [aes] (format v1) : {e}")
        return chain

    raise ValueError(
        "Format [aes] invalide dans secrets.toml. "
        "Attendu : [aes.current] avec key/iv, ou [aes] avec key/iv."
    )


def load_aes_credentials() -> tuple[bytes, bytes]:
    """
    Charge la clé AES principale (compatibilité ascendante).

    Préférer load_aes_key_chain() pour la gestion de la rotation de clés.

    Returns:
        Tuple (aes_key, aes_iv) de la première clé disponible

    Raises:
        ValueError: Si les clés ne peuvent pas être chargées
    """
    chain = load_aes_key_chain()
    _, key, iv = chain[0]
    return key, iv


def decrypt_with_key_chain(
    encrypted_data: bytes,
    key_chain: list[tuple[str, bytes, bytes]],
) -> tuple[bytes, str]:
    """
    Tente le déchiffrement avec chaque clé dans l'ordre.

    Args:
        encrypted_data: Données chiffrées
        key_chain: Liste de (nom, clé, iv) à essayer dans l'ordre

    Returns:
        Tuple (données_déchiffrées, nom_clé_utilisée)

    Raises:
        ValueError: Si toutes les clés échouent, avec le détail de chaque erreur
    """
    errors: list[str] = []
    for name, key, iv in key_chain:
        try:
            return decrypt_file_aes(encrypted_data, key, iv), name
        except ValueError as e:
            errors.append(f"  [{name}] {e}")

    raise ValueError(
        f"Échec déchiffrement avec {len(key_chain)} clé(s) :\n" + "\n".join(errors)
    )


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
    key_chain: list[tuple[str, bytes, bytes]],
) -> Iterator[dict]:
    """
    Fonction de base pour déchiffrer les fichiers AES depuis SFTP.

    Tente chaque clé de key_chain dans l'ordre. Si toutes échouent,
    le fichier est ignoré (yield rien) et une erreur est loguée.
    Le fichier sera re-tenté au prochain run DLT.

    Args:
        encrypted_file: Fichier chiffré depuis une resource SFTP
        key_chain: Chaîne de clés [(nom, clé, iv), ...] à essayer

    Yields:
        dict: {
            'file_name': str,
            'modification_date': datetime,
            'decrypted_content': bytes,
            'original_size': int,
            'decrypted_size': int,
            'key_used': str,
        }
    """
    try:
        encrypted_data = read_sftp_file(encrypted_file)
        original_size = len(encrypted_data)

        decrypted_data, key_used = decrypt_with_key_chain(encrypted_data, key_chain)
        decrypted_size = len(decrypted_data)

        if len(key_chain) > 1:
            print(f"✅ Déchiffré avec clé '{key_used}' : {encrypted_file['file_name']}")

        yield {
            'file_name': encrypted_file['file_name'],
            'modification_date': encrypted_file['modification_date'],
            'decrypted_content': decrypted_data,
            'original_size': original_size,
            'decrypted_size': decrypted_size,
            'key_used': key_used,
        }

    except Exception as e:
        print(f"❌ Erreur déchiffrement {encrypted_file['file_name']}: {e}")
        return


def create_decrypt_transformer(aes_key: bytes = None, aes_iv: bytes = None):
    """
    Factory pour créer un transformer de déchiffrement avec chaîne de clés pré-chargée.

    Les clés sont chargées une seule fois à la création du transformer (pas à chaque
    fichier). Si aes_key et aes_iv sont fournis, ils sont utilisés directement comme
    clé unique (compatibilité ascendante).

    Args:
        aes_key: Clé AES explicite (optionnel, pour les tests)
        aes_iv: IV AES explicite (optionnel, pour les tests)

    Returns:
        Transformer DLT configuré
    """
    if aes_key is not None and aes_iv is not None:
        key_chain = [('explicit', aes_key, aes_iv)]
    else:
        key_chain = load_aes_key_chain()
        nb = len(key_chain)
        names = [name for name, _, _ in key_chain]
        print(f"🔐 {nb} clé(s) AES chargée(s) : {names}")

    @dlt.transformer
    def configured_decrypt_transformer(encrypted_file: FileItemDict) -> Iterator[dict]:
        return _decrypt_aes_transformer_base(encrypted_file, key_chain)

    return configured_decrypt_transformer
