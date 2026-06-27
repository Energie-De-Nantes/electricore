"""Escalade d'échec de déchiffrement per-flux (ADR-0037, #353).

Bout-en-bout sur un bucket `file://` : la source `flux_enedis_brut` agrège les
succès/échecs de déchiffrement **par flux**. Deux cas couverts dans un même run :

- **Flux entier KO** (C15) : tous les fichiers indéchiffrables → flux aveugle →
  `flux_sans_dechiffrement` le remonte → le runner ferait échouer le job → alerte bot.
- **Fichier isolé KO** (R64) : un fichier corrompu noyé dans des succès → toléré,
  compté, le bon fichier landé quand même (pas de poison-pill) → flux NON aveugle.
"""

import io
import os
import time
import zipfile
from pathlib import Path

import dlt
import pytest

pytest.importorskip("Crypto", reason="Nécessite l'extra [ingestion] : uv sync --extra ingestion")
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from electricore.config import runtime
from electricore.ingestion.sources.sftp_enedis_brut import flux_enedis_brut
from electricore.ingestion.transformers.crypto import StatsChaine

_KEY = bytes.fromhex("00112233445566778899aabbccddeeff")  # 16 octets (AES-128)
_IV = bytes.fromhex("ffeeddccbbaa99887766554433221100")

_CONFIG = {
    "C15": {"file_pattern": "**/*_C15_*.zip", "format": "xml", "file_regex": "*_C15_*.xml"},
    "R64": {"file_pattern": "**/R63_R64_R65_R66_R67_C68/*_R64_*.zip", "format": "json", "file_regex": "*.JSON"},
}

_R64_JSON = (Path(__file__).parents[1] / "fixtures" / "flux" / "r64.json").read_bytes()


def _zip_clair(nom_interne: str, contenu: bytes) -> bytes:
    """ZIP en clair (non chiffré) contenant `nom_interne` — pour tester l'étage unzip seul."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(nom_interne, contenu)
    return buf.getvalue()


def _zip_chiffre(nom_interne: str, contenu: bytes) -> bytes:
    """ZIP contenant `nom_interne`, chiffré AES-128-CBC + PKCS7 avec la clé du trousseau."""
    return AES.new(_KEY, AES.MODE_CBC, _IV).encrypt(pad(_zip_clair(nom_interne, contenu), AES.block_size))


def _fichier_dechiffre(nom_zip: str, contenu_zip: bytes) -> dict:
    """Forme du dict produit par l'étage decrypt, consommé par l'étage unzip."""
    return {"file_name": nom_zip, "modification_date": None, "decrypted_content": contenu_zip}


def _ecrire(chemin: Path, data: bytes, jour: int) -> None:
    chemin.parent.mkdir(parents=True, exist_ok=True)
    chemin.write_bytes(data)
    ts = time.mktime((2026, 6, jour, 12, 0, 0, 0, 0, -1))
    os.utime(chemin, (ts, ts))


@pytest.fixture(autouse=True)
def _trousseau(monkeypatch):
    monkeypatch.setattr(runtime, "FICHIER_ENV", None)
    monkeypatch.setenv("AES__TROUSSEAU__test__KEY", _KEY.hex())
    monkeypatch.setenv("AES__TROUSSEAU__test__IV", _IV.hex())
    runtime.vider_cache()
    yield
    runtime.vider_cache()


@pytest.fixture
def bucket(tmp_path, monkeypatch):
    """Bucket file:// : C15 entièrement indéchiffrable, R64 = 1 bon + 1 corrompu."""
    b = tmp_path / "bucket"
    # C15 : deux zips de pur bruit (multiples de 16) → padding/magic KO → flux aveugle.
    _ecrire(b / "a_C15_1.zip", os.urandom(48), jour=1)
    _ecrire(b / "a_C15_2.zip", os.urandom(48), jour=2)
    # R64 : un zip valide (déchiffre + parse + land) + un corrompu (échec isolé toléré).
    sub = "R63_R64_R65_R66_R67_C68"
    _ecrire(b / sub / "a_R64_ok.zip", _zip_chiffre("mesure.JSON", _R64_JSON), jour=1)
    _ecrire(b / sub / "a_R64_ko.zip", os.urandom(48), jour=2)
    monkeypatch.setenv("SFTP__URL", f"file://{b}/")
    runtime.vider_cache()
    return b


def test_escalade_decrypt_compte_par_flux(bucket, tmp_path):
    """Étage decrypt (ADR-0037) : déchiffrements OK/KO comptés par flux, le bon R64 landé."""
    stats: dict[str, StatsChaine] = {}
    pipeline = dlt.pipeline(
        pipeline_name="flux_brut_escalade",
        destination=dlt.destinations.duckdb(str(tmp_path / "esc.duckdb")),
        dataset_name="flux_raw",
        pipelines_dir=str(tmp_path / "pipelines"),
    )
    pipeline.run(flux_enedis_brut(_CONFIG, stats=stats))

    # Cas « flux entier KO » : C15 ne déchiffre rien (2 fichiers, 2 échecs de déchiffrement).
    assert stats["C15"].fichiers == 2
    assert stats["C15"].dechiffres == 0 and stats["C15"].echecs_dechiffrement == 2
    # Cas « fichier isolé KO » : R64 déchiffre 1 fichier sur 2 (le corrompu compté, pas bloquant).
    assert stats["R64"].fichiers == 2
    assert stats["R64"].dechiffres == 1 and stats["R64"].echecs_dechiffrement == 1

    # Pas de poison-pill : le bon R64 a bien été landé malgré le fichier corrompu.
    import duckdb

    con = duckdb.connect(str(tmp_path / "esc.duckdb"))
    (n_r64,) = con.execute("select count(*) from flux_raw.raw_r64").fetchone()
    con.close()
    assert n_r64 == 1


# =============================================================================
# Étage unzip : extraits comptés, ZIP corrompu compté sans crash (ADR-0037 ext.)
# =============================================================================


def test_unzip_compte_les_fichiers_extraits():
    """Chaque fichier interne yieldé incrémente `extraits` (aucun échec sur un ZIP sain)."""
    from electricore.ingestion.transformers.archive import _unzip_transformer_base

    stats = StatsChaine()
    fichier = _fichier_dechiffre("a.zip", _zip_clair("mesure.xml", b"<data/>"))

    produits = list(_unzip_transformer_base(fichier, ".xml", "*.xml", stats))

    assert len(produits) == 1
    assert stats.extraits == 1 and stats.echecs_extraction == 0


def test_unzip_compte_un_echec_sur_zip_corrompu_sans_crash():
    """Fin du silence d'unzip (ADR-0037 ext.) : un ZIP illisible est compté
    `echecs_extraction`, sans exception propagée (pas de poison-pill)."""
    from electricore.ingestion.transformers.archive import _unzip_transformer_base

    stats = StatsChaine()
    fichier = _fichier_dechiffre("corrompu.zip", b"ceci n'est pas un zip")

    produits = list(_unzip_transformer_base(fichier, ".xml", "*.xml", stats))

    assert produits == []
    assert stats.extraits == 0 and stats.echecs_extraction == 1
