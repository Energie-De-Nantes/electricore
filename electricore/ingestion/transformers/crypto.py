"""
Transformer DLT pour le déchiffrement AES des fichiers Enedis.
Inclut les fonctions pures de cryptographie et le transformer DLT.

Trousseau de clés N-clés (ADR-0037, registre runtime #141) :
  Les clés AES Enedis sont rotées périodiquement et évoluent en longueur (la
  bascule AES-128 → AES-256 du 8-9 juin 2026). Le registre runtime expose un
  trousseau de taille arbitraire, alimenté par des variables d'environnement
  nommées (`.env` compris) :

      AES__TROUSSEAU__aes256_2026__KEY=…    # 32 octets hex (AES-256)
      AES__TROUSSEAU__aes256_2026__IV=…
      AES__TROUSSEAU__aes128_2024__KEY=…    # 16 octets hex (AES-128)
      AES__TROUSSEAU__aes128_2024__IV=…

  La sélection se fait **par essai** : chaque clé du trousseau est tentée dans un
  ordre indifférent ; le déchiffrement est son propre oracle (padding PKCS7 +
  magic bytes ZIP). Aucune date ni protocole ne pilote la sélection. Le `<label>`
  parlant remonte dans les logs pour faciliter le diagnostic.
"""

import logging
from collections.abc import Iterator
from dataclasses import dataclass

import dlt
from Crypto.Cipher import AES
from dlt.common.storages.fsspec_filesystem import FileItemDict

from electricore.config import runtime

logger = logging.getLogger(__name__)


# Magic bytes d'un fichier ZIP (PK local file header)
_ZIP_MAGIC = b"PK\x03\x04"


@dataclass
class StatsDechiffrement:
    """Compteur de déchiffrement d'un flux (resource) — escalade per-flux (ADR-0037).

    Le transformer incrémente `succes`/`echecs` au fil des fichiers ; le runner
    agrège ensuite par flux et décide si l'absence totale de succès (`flux_aveugle`)
    doit faire échouer le job.
    """

    succes: int = 0
    echecs: int = 0

    def flux_aveugle(self) -> bool:
        """Flux ayant des fichiers mais aucun déchiffrement réussi (≥ 1 échec) → clé manquante.

        Un échec isolé noyé dans des succès (fichier corrompu) n'est PAS aveugle : il est
        toléré. Un flux sans aucun fichier (succès comme échec à 0) ne l'est pas non plus.
        """
        return self.succes == 0 and self.echecs > 0


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
        raise ValueError(f"Clé AES incorrecte : padding PKCS7 invalide (dernier octet = {n})")
    decrypted = raw[:-n]

    # Vérification magic bytes ZIP — tous les fichiers Enedis sont des archives ZIP
    if len(decrypted) < 4 or decrypted[:4] != _ZIP_MAGIC:
        magic = decrypted[:4].hex() if len(decrypted) >= 4 else decrypted.hex()
        raise ValueError(f"Clé AES incorrecte : résultat non-ZIP (magic bytes = {magic})")

    return decrypted


def load_aes_key_chain() -> list[tuple[str, bytes, bytes]]:
    """
    Charge le trousseau de clés AES depuis le registre runtime (#141, ADR-0037).

    Le trousseau nommé (`AES__TROUSSEAU__<label>__KEY/IV`) et le parsing
    hexadécimal vivent dans le domaine `aes` du registre.

    Returns:
        Liste de tuples (label, clé, iv) — ordre indifférent (sélection par essai).

    Raises:
        ConfigurationManquante: Si le trousseau est vide.
        ValueError: Si une clé/IV n'est pas un hexadécimal valide.
    """
    return runtime.aes().chaine()


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

    raise ValueError(f"Échec déchiffrement avec {len(key_chain)} clé(s) :\n" + "\n".join(errors))


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
    stats: StatsDechiffrement,
) -> Iterator[dict]:
    """
    Fonction de base pour déchiffrer les fichiers AES depuis SFTP.

    Tente chaque clé de key_chain par essai. Fin du fail silencieux (ADR-0037) :
    un échec de déchiffrement est **compté** dans `stats`, tracé en warn-log, et le
    fichier est sauté (pas de poison-pill : un fichier corrompu isolé ne fait pas
    tomber tout le flux). Le runner agrège ensuite `stats` par flux et décide si
    l'absence totale de succès doit faire échouer le job.

    Args:
        encrypted_file: Fichier chiffré depuis une resource SFTP
        key_chain: Chaîne de clés [(label, clé, iv), ...] à essayer
        stats: Compteur succès/échec du flux courant (muté en place)

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
    encrypted_data = read_sftp_file(encrypted_file)
    original_size = len(encrypted_data)

    try:
        decrypted_data, key_used = decrypt_with_key_chain(encrypted_data, key_chain)
    except ValueError as e:
        stats.echecs += 1
        logger.warning("Échec déchiffrement %s : %s", encrypted_file["file_name"], e)
        return

    stats.succes += 1
    if len(key_chain) > 1:
        logger.debug("Déchiffré avec clé '%s' : %s", key_used, encrypted_file["file_name"])

    yield {
        "file_name": encrypted_file["file_name"],
        "modification_date": encrypted_file["modification_date"],
        "decrypted_content": decrypted_data,
        "original_size": original_size,
        "decrypted_size": len(decrypted_data),
        "key_used": key_used,
    }


def create_decrypt_transformer(
    aes_key: bytes = None,
    aes_iv: bytes = None,
    *,
    key_chain: list[tuple[str, bytes, bytes]] | None = None,
    stats: StatsDechiffrement | None = None,
):
    """
    Factory pour créer un transformer de déchiffrement avec trousseau pré-chargé.

    Le trousseau est résolu une seule fois à la création du transformer (pas à chaque
    fichier). Précédence : `key_chain` explicite > `aes_key`/`aes_iv` (clé unique, tests)
    > trousseau du registre runtime.

    Args:
        aes_key: Clé AES explicite (optionnel, pour les tests)
        aes_iv: IV AES explicite (optionnel, pour les tests)
        key_chain: Trousseau déjà résolu [(label, clé, iv), …] — partagé entre flux par la source
        stats: Compteur succès/échec du flux courant (escalade per-flux, ADR-0037)

    Returns:
        Transformer DLT configuré
    """
    if key_chain is None:
        if aes_key is not None and aes_iv is not None:
            key_chain = [("explicit", aes_key, aes_iv)]
        else:
            key_chain = load_aes_key_chain()
            names = [name for name, _, _ in key_chain]
            logger.info("%d clé(s) AES chargée(s) : %s", len(key_chain), names)

    stats = stats if stats is not None else StatsDechiffrement()

    @dlt.transformer
    def configured_decrypt_transformer(encrypted_file: FileItemDict) -> Iterator[dict]:
        return _decrypt_aes_transformer_base(encrypted_file, key_chain, stats)

    return configured_decrypt_transformer
