"""
Transformer DLT pour le déchiffrement AES des fichiers Enedis.
Inclut les fonctions pures de cryptographie et le transformer DLT.

Trousseau de clés N-clés (ADR-0037, registre runtime #141) :
  Les clés AES Enedis sont rotées périodiquement et évoluent en longueur (la
  bascule AES-128 → AES-256 du 8-9 juin 2026). Le registre runtime expose un
  trousseau de taille arbitraire, alimenté par des variables d'environnement
  nommées (`.env` compris) :

      AES__TROUSSEAU__aes256_2026__KEY=…    # 32 octets hex (AES-256), SANS __IV
      AES__TROUSSEAU__aes128_2024__KEY=…    # 16 octets hex (AES-128)
      AES__TROUSSEAU__aes128_2024__IV=…

  L'IV (`__IV`) est **optionnel** et détermine le schéma de déchiffrement (ADR-0040) :
  présent ⇒ schéma **IV-fixe** (l'IV est en config, AES-128 legacy) ; absent ⇒ schéma
  **IV-préfixé** (l'IV est les 16 premiers octets de chaque fichier, AES-256).

  La sélection de la clé se fait **par essai** : chaque clé du trousseau est tentée dans
  un ordre indifférent ; le déchiffrement est son propre oracle (padding PKCS7 +
  magic bytes ZIP). Aucune date ni protocole ne pilote la sélection. Le `<label>`
  parlant remonte dans les logs pour faciliter le diagnostic.
"""

import logging
from collections.abc import Iterator

import dlt
from Crypto.Cipher import AES
from dlt.common.storages.fsspec_filesystem import FileItemDict

from electricore.config import runtime

# L'étage decrypt applique la discipline de chaîne (`etape_chaine`) et compte dans
# `StatsChaine` ; les deux vivent dans `transformers/chaine.py`, au-dessus des étages (#479).
from electricore.ingestion.transformers.chaine import StatsChaine, etape_chaine

logger = logging.getLogger(__name__)


# Magic bytes d'un fichier ZIP (PK local file header)
_ZIP_MAGIC = b"PK\x03\x04"


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


def decrypt_with_key_chain(
    encrypted_data: bytes,
    key_chain: list[tuple[str, bytes, bytes | None]],
) -> tuple[bytes, str]:
    """
    Tente le déchiffrement avec chaque clé dans l'ordre, en routant par schéma.

    L'IV de l'entrée détermine le **schéma de déchiffrement** (ADR-0040) :
      - IV présent → schéma **IV-fixe** (AES-128 legacy) : l'IV est en config et tout
        le fichier est le ciphertext ;
      - IV `None` → schéma **IV-préfixé** (AES-256) : l'IV est les 16 premiers octets
        du fichier (en clair, frais par fichier), le reste est le ciphertext.

    Args:
        encrypted_data: Données chiffrées
        key_chain: Liste de (nom, clé, iv|None) à essayer dans l'ordre

    Returns:
        Tuple (données_déchiffrées, nom_clé_utilisée)

    Raises:
        ValueError: Si toutes les clés échouent, avec le détail de chaque erreur
    """
    errors: list[str] = []
    for name, key, iv in key_chain:
        try:
            if iv is None:
                # Schéma IV-préfixé : détache l'IV de la tête du fichier (ADR-0040).
                return decrypt_file_aes(encrypted_data[16:], key, encrypted_data[:16]), name
            return decrypt_file_aes(encrypted_data, key, iv), name
        except ValueError as e:
            errors.append(f"  [{name}] {e}")

    raise ValueError(f"Échec déchiffrement avec {len(key_chain)} clé(s) :\n" + "\n".join(errors))


# =============================================================================
# TRANSFORMER DLT
# =============================================================================


@etape_chaine(
    succes="dechiffres",
    echec="echecs_dechiffrement",
    libelle="déchiffrement",
    cle_item="file_name",
    capture=ValueError,
)
def _decrypt(encrypted_file: FileItemDict, key_chain: list[tuple[str, bytes, bytes]]) -> Iterator[dict]:
    """Étage decrypt (discipline) : fichier chiffré SFTP → document déchiffré.

    Ne déclare que **son travail** (lire, tenter chaque clé par essai, émettre le document) :
    un échec de toutes les clés **lève** une `ValueError`. La discipline « attraper → compter
    → continuer » (capture **resserrée** à `ValueError`, comptage `dechiffres`/
    `echecs_dechiffrement`, non-propagation) vit dans `etape_chaine`. Le pré-compte du zip
    entrant (`fichiers`) n'est PAS de la discipline (ni succès ni échec) : il vit dans
    `_decrypt_aes_transformer_base`, hors du combinateur (symétrie à deux compteurs).

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
    # IO non gardée : une erreur de lecture n'est pas une ValueError → propage
    # (hors discipline ADR-0037).
    with encrypted_file.open() as f:
        encrypted_data = f.read()
    original_size = len(encrypted_data)
    decrypted_data, key_used = decrypt_with_key_chain(encrypted_data, key_chain)  # toutes clés KO → lève
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


def _decrypt_aes_transformer_base(
    encrypted_file: FileItemDict,
    key_chain: list[tuple[str, bytes, bytes]],
    stats: StatsChaine,
) -> Iterator[dict]:
    """Étage decrypt complet : pré-compte le zip entrant (`fichiers`) puis applique la discipline.

    `stats.fichiers` est le compteur d'**entrée** de decrypt — inconditionnel par zip reçu, ni
    succès ni échec : il vit donc HORS du combinateur (qui reste symétrique à deux compteurs sur
    les trois étages). Ce pré-compte précède la lecture SFTP de `_decrypt` ; un fichier dont la
    lecture échouerait est tout de même compté `fichiers` (l'erreur IO propage telle quelle, hors
    discipline ADR-0037). Seam de test sans dlt : la factory `@dlt.transformer` ci-dessous l'appelle.
    """
    stats.fichiers += 1
    yield from _decrypt(encrypted_file, key_chain, stats)  # stats : dernier positionnel (injecté)


def create_decrypt_transformer(
    *,
    key_chain: list[tuple[str, bytes, bytes]],
    stats: StatsChaine | None = None,
):
    """
    Factory pour créer un transformer de déchiffrement avec trousseau pré-chargé.

    Le trousseau (résolu une fois par la source via `load_aes_key_chain`, partagé
    entre flux) et le compteur de chaîne du flux courant sont injectés à la création
    du transformer, pas à chaque fichier.

    Args:
        key_chain: Trousseau déjà résolu [(label, clé, iv), …] — partagé entre flux par la source
        stats: Compteur de chaîne du flux courant (escalade per-flux, ADR-0037)

    Returns:
        Transformer DLT configuré
    """
    stats = stats if stats is not None else StatsChaine()

    @dlt.transformer
    def configured_decrypt_transformer(encrypted_file: FileItemDict) -> Iterator[dict]:
        return _decrypt_aes_transformer_base(encrypted_file, key_chain, stats)

    return configured_decrypt_transformer
