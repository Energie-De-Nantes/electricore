"""
Tests unitaires pour electricore/etl/transformers/crypto.py.

Couvre :
  - decrypt_file_aes() : validation PKCS7 stricte + magic bytes ZIP
  - decrypt_with_key_chain() : cascade de clés, fallback, échec total
  - load_aes_key_chain() : format v1 (plat) et v2 (current/previous)

Nécessite l'extra [etl] : uv sync --extra etl
"""
import io
import zipfile
from unittest.mock import patch

import pytest

# Skip tout le fichier si PyCryptodome n'est pas installé
pytest.importorskip("Crypto", reason="Nécessite l'extra [etl] : uv sync --extra etl")
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from electricore.etl.transformers.crypto import (
    decrypt_file_aes,
    decrypt_with_key_chain,
    load_aes_key_chain,
)


# =============================================================================
# FIXTURES
# =============================================================================


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
        ("previous", aes_key, aes_iv),   # Bonne clé
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


# =============================================================================
# TESTS load_aes_key_chain (mock dlt.secrets)
# =============================================================================


@pytest.mark.unit
def test_load_key_chain_v1_format(aes_key, aes_iv):
    """Format v1 [aes] plat → une clé nommée 'legacy'."""
    mock_aes = {"key": aes_key.hex(), "iv": aes_iv.hex()}

    with patch("electricore.etl.transformers.crypto.dlt") as mock_dlt:
        mock_dlt.secrets = {"aes": mock_aes}
        chain = load_aes_key_chain()

    assert len(chain) == 1
    name, key, iv = chain[0]
    assert name == "legacy"
    assert key == aes_key
    assert iv == aes_iv


@pytest.mark.unit
def test_load_key_chain_v2_current_only(aes_key, aes_iv):
    """Format v2 [aes.current] sans previous → une clé nommée 'current'."""
    mock_aes = {"current": {"key": aes_key.hex(), "iv": aes_iv.hex()}}

    with patch("electricore.etl.transformers.crypto.dlt") as mock_dlt:
        mock_dlt.secrets = {"aes": mock_aes}
        chain = load_aes_key_chain()

    assert len(chain) == 1
    assert chain[0][0] == "current"
    assert chain[0][1] == aes_key
    assert chain[0][2] == aes_iv


@pytest.mark.unit
def test_load_key_chain_v2_with_previous(aes_key, aes_iv):
    """Format v2 [aes.current] + [aes.previous] → deux clés, current en premier."""
    old_key = bytes([0xFF] * 16)
    old_iv = bytes([0xFE] * 16)

    mock_aes = {
        "current": {"key": aes_key.hex(), "iv": aes_iv.hex()},
        "previous": {"key": old_key.hex(), "iv": old_iv.hex()},
    }

    with patch("electricore.etl.transformers.crypto.dlt") as mock_dlt:
        mock_dlt.secrets = {"aes": mock_aes}
        chain = load_aes_key_chain()

    assert len(chain) == 2
    assert chain[0][0] == "current"
    assert chain[1][0] == "previous"
    assert chain[0][1] == aes_key
    assert chain[1][1] == old_key


@pytest.mark.unit
def test_load_key_chain_invalid_format():
    """Format [aes] sans key/iv ni sous-sections → ValueError."""
    mock_aes = {"unknown_field": "something"}

    with patch("electricore.etl.transformers.crypto.dlt") as mock_dlt:
        mock_dlt.secrets = {"aes": mock_aes}
        with pytest.raises(ValueError, match="Format \\[aes\\] invalide"):
            load_aes_key_chain()
