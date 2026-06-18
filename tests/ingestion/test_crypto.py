"""
Tests unitaires pour electricore/ingestion/transformers/crypto.py.

Couvre :
  - decrypt_file_aes() : validation PKCS7 stricte + magic bytes ZIP
  - decrypt_with_key_chain() : cascade de clés, fallback, échec total
  - load_aes_key_chain() : format v1 (plat) et v2 (current/previous)

Nécessite l'extra [ingestion] : uv sync --extra ingestion
"""

import io
import zipfile

import pytest

# Skip tout le fichier si PyCryptodome n'est pas installé
pytest.importorskip("Crypto", reason="Nécessite l'extra [ingestion] : uv sync --extra ingestion")
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from electricore.config import runtime
from electricore.ingestion.transformers.crypto import (
    StatsDechiffrement,
    _decrypt_aes_transformer_base,
    decrypt_file_aes,
    decrypt_with_key_chain,
    load_aes_key_chain,
)

# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture(autouse=True)
def _isoler_env_aes(monkeypatch):
    """Isole les tests crypto : .env du dépôt neutralisé, cache des accessors
    runtime vidé (#141). Les labels du trousseau (ADR-0037) étant arbitraires, on
    neutralise la source `.env` plutôt que d'énumérer des noms de variables."""
    monkeypatch.setattr(runtime, "FICHIER_ENV", None)
    runtime.vider_cache()
    yield
    runtime.vider_cache()


@pytest.fixture
def zip_bytes() -> bytes:
    """Contenu d'un fichier ZIP valide minimal (commence par PK\\x03\\x04)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("test.xml", "<data>test</data>")
    return buf.getvalue()


@pytest.fixture
def aes_key() -> bytes:
    return bytes.fromhex("0102030405060708090a0b0c0d0e0f10")  # 16 bytes AES-128


@pytest.fixture
def aes_iv() -> bytes:
    return bytes.fromhex("1112131415161718191a1b1c1d1e1f20")  # 16 bytes


@pytest.fixture
def encrypted_zip(zip_bytes, aes_key, aes_iv) -> bytes:
    """ZIP chiffré avec AES-128-CBC + padding PKCS7."""
    cipher = AES.new(aes_key, AES.MODE_CBC, aes_iv)
    return cipher.encrypt(pad(zip_bytes, AES.block_size))


# =============================================================================
# TESTS decrypt_file_aes
# =============================================================================


@pytest.mark.unit
def test_decrypt_correct_key(encrypted_zip, zip_bytes, aes_key, aes_iv):
    """Round-trip : chiffrement puis déchiffrement retrouve l'original."""
    result = decrypt_file_aes(encrypted_zip, aes_key, aes_iv)
    assert result == zip_bytes


@pytest.mark.unit
def test_decrypt_wrong_key_raises(encrypted_zip, aes_iv):
    """Mauvaise clé → ValueError (padding ou magic bytes invalide)."""
    wrong_key = bytes(16)  # Clé nulle, très différente de la vraie
    with pytest.raises(ValueError):
        decrypt_file_aes(encrypted_zip, wrong_key, aes_iv)


@pytest.mark.unit
def test_decrypt_wrong_iv_raises(encrypted_zip, aes_key):
    """Mauvais IV → résultat incorrect → ValueError."""
    wrong_iv = bytes(16)
    with pytest.raises(ValueError):
        decrypt_file_aes(encrypted_zip, aes_key, wrong_iv)


@pytest.mark.unit
def test_pkcs7_full_validation_invalid_padding():
    """
    Padding partiellement valide (dernier octet = 3, mais bytes avant != 3)
    doit lever ValueError avec 'padding PKCS7 invalide'.

    Vérifie que la validation est complète (tous les N octets, pas juste le dernier).
    """
    # Dernier octet = 3, mais les 3 octets de padding ne sont pas tous 0x03
    bad_data = b"\x00" * 12 + b"\x01\x02\x03\x03"  # [12]=1, [13]=2 → invalide
    assert len(bad_data) == 16

    key = bytes(range(16))
    iv = bytes(range(16, 32))
    cipher = AES.new(key, AES.MODE_CBC, iv)
    ciphertext = cipher.encrypt(bad_data)  # Chiffre les mauvais padding bytes directement

    with pytest.raises(ValueError, match="padding PKCS7 invalide"):
        decrypt_file_aes(ciphertext, key, iv)


@pytest.mark.unit
def test_non_zip_content_raises(aes_key, aes_iv):
    """Déchiffrement valide (bon padding) mais pas un ZIP → ValueError 'non-ZIP'."""
    not_a_zip = b"not a zip file at all !!! 12345"  # 30 bytes, pas de magic PK\x03\x04
    cipher = AES.new(aes_key, AES.MODE_CBC, aes_iv)
    ciphertext = cipher.encrypt(pad(not_a_zip, AES.block_size))

    with pytest.raises(ValueError, match="non-ZIP"):
        decrypt_file_aes(ciphertext, aes_key, aes_iv)


# =============================================================================
# TESTS decrypt_with_key_chain
# =============================================================================


@pytest.mark.unit
def test_decrypt_with_key_chain_first_key(encrypted_zip, zip_bytes, aes_key, aes_iv):
    """Première clé correcte → succès immédiat, retourne le bon nom."""
    chain = [
        ("current", aes_key, aes_iv),
        ("previous", bytes(16), aes_iv),  # Mauvaise clé, ne doit pas être essayée
    ]
    result, key_used = decrypt_with_key_chain(encrypted_zip, chain)
    assert result == zip_bytes
    assert key_used == "current"


@pytest.mark.unit
def test_decrypt_with_key_chain_fallback(encrypted_zip, zip_bytes, aes_key, aes_iv):
    """Première clé incorrecte, deuxième correcte → succès avec 'previous'."""
    chain = [
        ("current", bytes(16), aes_iv),  # Mauvaise clé
        ("previous", aes_key, aes_iv),  # Bonne clé
    ]
    result, key_used = decrypt_with_key_chain(encrypted_zip, chain)
    assert result == zip_bytes
    assert key_used == "previous"


@pytest.mark.unit
def test_decrypt_with_key_chain_all_fail(encrypted_zip, aes_iv):
    """Toutes les clés incorrectes → ValueError listant chaque échec."""
    chain = [
        ("current", bytes(16), aes_iv),
        ("previous", bytes([0xFF] * 16), aes_iv),
    ]
    with pytest.raises(ValueError) as exc_info:
        decrypt_with_key_chain(encrypted_zip, chain)

    error_msg = str(exc_info.value)
    assert "2 clé(s)" in error_msg
    assert "[current]" in error_msg
    assert "[previous]" in error_msg


@pytest.mark.unit
def test_decrypt_with_key_chain_single_key(encrypted_zip, zip_bytes, aes_key, aes_iv):
    """Chaîne d'une seule clé correcte → succès."""
    chain = [("legacy", aes_key, aes_iv)]
    result, key_used = decrypt_with_key_chain(encrypted_zip, chain)
    assert result == zip_bytes
    assert key_used == "legacy"


@pytest.mark.unit
def test_trousseau_mixe_aes128_et_aes256_dechiffre_par_essai(zip_bytes, aes_key, aes_iv):
    """Corpus mêlant AES-128 et AES-256 : chaque fichier déchiffre par essai (ADR-0037, #352).

    Le même trousseau (ordre indifférent) déchiffre les deux ciphers ; AES.new auto-sélectionne
    la longueur de clé. Le label de la clé qui déchiffre est retourné (→ logs).
    """
    cle_256 = bytes.fromhex("ee" * 32)  # 32 octets → AES-256
    iv_256 = bytes.fromhex("ff" * 16)
    chiffre_128 = AES.new(aes_key, AES.MODE_CBC, aes_iv).encrypt(pad(zip_bytes, AES.block_size))
    chiffre_256 = AES.new(cle_256, AES.MODE_CBC, iv_256).encrypt(pad(zip_bytes, AES.block_size))

    # Ordre indifférent : la clé AES-256 est en tête, l'AES-128 ensuite.
    trousseau = [("aes256_2026", cle_256, iv_256), ("aes128_2024", aes_key, aes_iv)]

    contenu_128, label_128 = decrypt_with_key_chain(chiffre_128, trousseau)
    contenu_256, label_256 = decrypt_with_key_chain(chiffre_256, trousseau)

    assert contenu_128 == zip_bytes and label_128 == "aes128_2024"
    assert contenu_256 == zip_bytes and label_256 == "aes256_2026"


# =============================================================================
# TESTS load_aes_key_chain (registre runtime — #141, ADR-0025 ; trousseau ADR-0037)
# =============================================================================


@pytest.mark.unit
def test_load_key_chain_trousseau_une_cle(aes_key, aes_iv, monkeypatch):
    """Un trousseau d'une clé → une entrée portant son label."""
    monkeypatch.setenv("AES__TROUSSEAU__aes128_2024__KEY", aes_key.hex())
    monkeypatch.setenv("AES__TROUSSEAU__aes128_2024__IV", aes_iv.hex())
    runtime.vider_cache()

    chain = load_aes_key_chain()

    assert chain == [("aes128_2024", aes_key, aes_iv)]


@pytest.mark.unit
def test_load_key_chain_trousseau_n_cles(aes_key, aes_iv, monkeypatch):
    """Plusieurs labels → autant d'entrées (ordre indifférent), labels préservés."""
    old_key = bytes([0xFF] * 16)
    old_iv = bytes([0xFE] * 16)
    monkeypatch.setenv("AES__TROUSSEAU__aes256_2026__KEY", aes_key.hex())
    monkeypatch.setenv("AES__TROUSSEAU__aes256_2026__IV", aes_iv.hex())
    monkeypatch.setenv("AES__TROUSSEAU__aes128_2024__KEY", old_key.hex())
    monkeypatch.setenv("AES__TROUSSEAU__aes128_2024__IV", old_iv.hex())
    runtime.vider_cache()

    chain = load_aes_key_chain()

    assert set(chain) == {("aes256_2026", aes_key, aes_iv), ("aes128_2024", old_key, old_iv)}


@pytest.mark.unit
def test_load_key_chain_trousseau_vide_leve_configuration_manquante():
    """Aucune variable AES__TROUSSEAU__* → ConfigurationManquante nommant le format attendu."""
    with pytest.raises(runtime.ConfigurationManquante, match="AES__TROUSSEAU__<label>__KEY"):
        load_aes_key_chain()


# =============================================================================
# TESTS escalade per-flux : comptage du déchiffrement (ADR-0037, #353)
# =============================================================================


class _FauxFichierSftp(dict):
    """Double minimal d'un FileItemDict : accès par clé + .open() sur des octets."""

    def __init__(self, file_name: str, data: bytes):
        super().__init__(file_name=file_name, modification_date=None)
        self._data = data

    def open(self):  # noqa: A003 — mime l'API FileItemDict
        return io.BytesIO(self._data)


@pytest.mark.unit
def test_transformer_compte_un_succes_et_yield(encrypted_zip, aes_key, aes_iv):
    """Un fichier déchiffrable : compté en succès, document brut produit."""
    stats = StatsDechiffrement()
    item = _FauxFichierSftp("a_R64_x.zip", encrypted_zip)

    produits = list(_decrypt_aes_transformer_base(item, [("aes128_2024", aes_key, aes_iv)], stats))

    assert stats.succes == 1 and stats.echecs == 0
    assert len(produits) == 1 and produits[0]["key_used"] == "aes128_2024"


@pytest.mark.unit
def test_transformer_compte_un_echec_et_ne_yield_pas(encrypted_zip, aes_iv, caplog):
    """Fin du fail silencieux (ADR-0037) : un échec est compté, warn-loggé, le fichier
    est sauté — pas d'exception propagée (pas de poison-pill)."""
    stats = StatsDechiffrement()
    item = _FauxFichierSftp("corrompu.zip", encrypted_zip)
    mauvaise_chaine = [("aes256_2026", bytes(16), aes_iv)]  # clé nulle → échec

    with caplog.at_level("WARNING"):
        produits = list(_decrypt_aes_transformer_base(item, mauvaise_chaine, stats))

    assert produits == []
    assert stats.succes == 0 and stats.echecs == 1
    assert "corrompu.zip" in caplog.text


@pytest.mark.unit
def test_flux_aveugle_seulement_si_zero_succes_et_au_moins_un_echec():
    """flux_aveugle() : True ssi des échecs existent sans aucun succès (clé manquante)."""
    assert StatsDechiffrement(succes=0, echecs=2).flux_aveugle() is True
    assert StatsDechiffrement(succes=3, echecs=1).flux_aveugle() is False  # échec isolé toléré
    assert StatsDechiffrement(succes=5, echecs=0).flux_aveugle() is False
    assert StatsDechiffrement(succes=0, echecs=0).flux_aveugle() is False  # flux sans fichier
