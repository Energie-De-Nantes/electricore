"""Escalade d'échec de **chaîne** per-flux (ADR-0037 étendu, #445).

La discipline « attraper → compter → continuer » de l'étage `decrypt` est généralisée à
toute la chaîne `decrypt | unzip | parse`. Un seul `StatsChaine` par flux compte chaque
étage ; le prédicat unique `flux_aveugle` (0 document produit malgré ≥ 1 échec) décide de
l'escalade, quel que soit l'étage qui a rendu le flux muet.

Tests unitaires (par fonction de base) : chaque étage compte ses succès/échecs sans crash —
notamment le **pin du spike** (parse) : un document malformé ne fait JAMAIS lever de
`PipelineStepFailed` qui aborterait tout l'`extract`. Test bout-en-bout (bucket `file://`,
un run) couvrant les quatre états en parallèle :

- **Sombre** (C15) : tous les fichiers indéchiffrables → 0 document → flux aveugle →
  `flux_aveugles` le remonte → le runner ferait échouer le job → alerte bot.
- **Mixte/isolé** (R64) : un bon document + un ZIP corrompu (echec_extraction) + un
  document malformé (echec_linearisation) → le bon est landé, échecs comptés par étage,
  flux NON aveugle (documents > 0). Le run **ne plante pas** (régression du spike).
- **Vide par nature** (R15) : un ZIP déchiffrable mais sans fichier interne correspondant
  → 0 document, 0 échec → NON aveugle (pas de faux positif).
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
    "R15": {"file_pattern": "**/*_R15_*.zip", "format": "xml", "file_regex": "*.xml"},
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


def _chiffre(clair: bytes) -> bytes:
    """Chiffre des octets arbitraires (déjà ZIP-like) avec la clé du trousseau."""
    return AES.new(_KEY, AES.MODE_CBC, _IV).encrypt(pad(clair, AES.block_size))


# Déchiffrable (commence par le magic ZIP `PK\x03\x04` → passe l'oracle decrypt) mais ZIP
# illisible (pas d'enregistrement de fin de catalogue) → l'étage unzip lève BadZipFile,
# compté echec_extraction. Octets fixes → déterministe.
_ZIP_CORROMPU_CLAIR = b"PK\x03\x04" + b"ceci commence par le magic ZIP mais n'en est pas un" + b"\x00" * 16


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
    """Bucket file:// couvrant les quatre états de la chaîne en un seul run.

    - C15 (sombre) : deux zips de pur bruit → padding/magic KO → 0 document, aveugle.
    - R64 (mixte) : un bon + un ZIP corrompu (echec_extraction) + un document malformé
      (echec_linearisation) → 1 document landé, flux NON aveugle.
    - R15 (vide par nature) : un ZIP déchiffrable contenant un seul fichier `.txt`
      (aucun `.xml` à extraire) → 0 document, 0 échec, NON aveugle.
    """
    b = tmp_path / "bucket"
    # C15 : pur bruit (multiples de 16) → padding/magic KO → echecs_dechiffrement → aveugle.
    _ecrire(b / "a_C15_1.zip", os.urandom(48), jour=1)
    _ecrire(b / "a_C15_2.zip", os.urandom(48), jour=2)
    # R64 : bon doc + ZIP corrompu + document malformé → échecs par étage, NON aveugle.
    sub = "R63_R64_R65_R66_R67_C68"
    _ecrire(b / sub / "a_R64_ok.zip", _zip_chiffre("mesure.JSON", _R64_JSON), jour=1)
    _ecrire(b / sub / "a_R64_zip_ko.zip", _chiffre(_ZIP_CORROMPU_CLAIR), jour=2)
    _ecrire(b / sub / "a_R64_doc_ko.zip", _zip_chiffre("mauvais.JSON", b"{ pas du JSON valide"), jour=3)
    # R15 : zip déchiffrable mais sans fichier `.xml` interne → vide par nature.
    _ecrire(b / "a_R15_vide.zip", _zip_chiffre("readme.txt", b"rien d'utile ici"), jour=1)
    monkeypatch.setenv("SFTP__URL", f"file://{b}/")
    runtime.vider_cache()
    return b


def test_escalade_chaine_bout_en_bout(bucket, tmp_path):
    """Bout-en-bout : chaque étage compté par flux, prédicat unique, run sans crash."""
    from electricore.ingestion.runner import flux_aveugles

    stats: dict[str, StatsChaine] = {}
    pipeline = dlt.pipeline(
        pipeline_name="flux_brut_escalade",
        destination=dlt.destinations.duckdb(str(tmp_path / "esc.duckdb")),
        dataset_name="flux_raw",
        pipelines_dir=str(tmp_path / "pipelines"),
    )
    # Le run NE plante PAS malgré un document R64 malformé : pin du spike (sinon dlt
    # lèverait PipelineStepFailed et aborterait tout l'extract, tous flux confondus).
    pipeline.run(flux_enedis_brut(_CONFIG, stats=stats))

    # Sombre : C15 ne produit aucun document → aveugle (clé AES manquante en prod).
    c15 = stats["C15"]
    assert c15.fichiers == 2 and c15.dechiffres == 0 and c15.documents == 0
    assert c15.echecs_dechiffrement == 2 and c15.flux_aveugle() is True

    # Mixte : R64 compte un échec à chaque étage aval mais produit 1 document → NON aveugle.
    r64 = stats["R64"]
    assert r64.fichiers == 3 and r64.dechiffres == 3
    assert r64.extraits == 2 and r64.documents == 1
    assert r64.echecs_extraction == 1 and r64.echecs_linearisation == 1
    assert r64.echecs_dechiffrement == 0 and r64.flux_aveugle() is False

    # Vide par nature : R15 déchiffre mais n'extrait aucun .xml → 0 document, 0 échec.
    r15 = stats["R15"]
    assert r15.fichiers == 1 and r15.dechiffres == 1
    assert r15.extraits == 0 and r15.documents == 0
    assert r15.echecs() == 0 and r15.flux_aveugle() is False

    # Seul C15 escalade (→ job failed → alerte bot) ; ni le mixte ni le vide-par-nature.
    assert flux_aveugles(stats) == ["C15"]

    # Pas de poison-pill : le bon document R64 est landé malgré ses voisins fautifs.
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


# =============================================================================
# Étage parse : documents comptés, document malformé compté sans PipelineStepFailed
# =============================================================================


def _fichier_extrait(nom: str, contenu: bytes) -> dict:
    """Forme du dict produit par l'étage unzip, consommé par l'étage parse."""
    return {"extracted_file_name": nom, "modification_date": None, "source_zip": "z.zip", "extracted_content": contenu}


def test_parse_compte_un_document_sur_contenu_valide():
    """Un document linéarisable incrémente `documents` et produit la ligne brute."""
    from electricore.ingestion.sources.sftp_enedis_brut import _vers_document_brut_base

    stats = StatsChaine()
    extrait = _fichier_extrait("mesure.JSON", _R64_JSON)

    produits = list(_vers_document_brut_base(extrait, "R64", est_json=True, stats=stats))

    assert len(produits) == 1
    assert stats.documents == 1 and stats.echecs_linearisation == 0


def test_parse_compte_un_echec_sur_document_malforme_sans_raise():
    """Pin du spike (ADR-0037 ext.) : dlt aborte tout l'`extract` sur une exception non
    rattrapée d'un transformer. Un document malformé doit donc être compté
    `echecs_linearisation` et sauté — SANS exception propagée."""
    from electricore.ingestion.sources.sftp_enedis_brut import _vers_document_brut_base

    stats = StatsChaine()
    extrait = _fichier_extrait("mesure.JSON", b"{ ceci n'est pas du JSON valide")

    produits = list(_vers_document_brut_base(extrait, "R64", est_json=True, stats=stats))

    assert produits == []
    assert stats.documents == 0 and stats.echecs_linearisation == 1
