"""Relais de flux Enedis déchiffrés vers SFTP partenaire (#637) — chemin bout-en-bout.

Couvre les critères d'acceptation, dans l'ordre tracer-bullet de l'issue :
  1. chemin end-to-end sur UN fichier (déchiffré, décompressé, poussé, journalisé) ;
  2. idempotence : un second run ne re-pousse aucun zip déjà livré ;
  3. direction d'échec sûre : un push qui échoue ne marque PAS le zip comme livré ;
  4. incremental: false : re-liste l'intégralité de la source à chaque run ;
  5. filtre configurable (flux + fenêtre date), en config ;
  6. vérification de complétude (zips reçus jamais relayés).

Nécessite l'extra [ingestion] (dlt, PyCryptodome) : uv sync --extra ingestion
"""

import io
import os
import time
import zipfile
from pathlib import Path

import dlt
import duckdb
import pytest

pytest.importorskip("Crypto", reason="Nécessite l'extra [ingestion] : uv sync --extra ingestion")
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from electricore.config import runtime
from electricore.ingestion.relais.pipeline import NOM_DATASET, NOM_RESOURCE, executer, zips_non_relayes

AES_KEY = bytes.fromhex("0102030405060708090a0b0c0d0e0f10")
AES_IV = bytes.fromhex("1112131415161718191a1b1c1d1e1f20")


@pytest.fixture(autouse=True)
def _isoler_env(monkeypatch):
    """Isole le domaine runtime : .env du dépôt neutralisé, cache vidé (cf. tests crypto)."""
    monkeypatch.setattr(runtime, "FICHIER_ENV", None)
    runtime.vider_cache()
    yield
    runtime.vider_cache()


def _zip_chiffre(nom_interne: str, contenu: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(nom_interne, contenu)
    cipher = AES.new(AES_KEY, AES.MODE_CBC, AES_IV)
    return cipher.encrypt(pad(buf.getvalue(), AES.block_size))


def _deposer_zip(bucket: Path, nom: str, contenu_interne: bytes, date=(2026, 6, 15, 12, 0, 0)) -> Path:
    bucket.mkdir(parents=True, exist_ok=True)
    chemin = bucket / nom
    chemin.write_bytes(_zip_chiffre(f"{nom.replace('.zip', '')}.xml", contenu_interne))
    ts = time.mktime((*date, 0, 0, -1))
    os.utime(chemin, (ts, ts))
    return chemin


def _configurer_env(monkeypatch, source: Path, cible: Path, db: Path, *, flux: str = "", depuis: str = "2026-06-01"):
    monkeypatch.setenv("RELAIS__SOURCE_URL", f"file://{source}/")
    monkeypatch.setenv("RELAIS__PARTNER_URL", f"file://{cible}/")
    monkeypatch.setenv("RELAIS__DESTINATION_DB", str(db))
    monkeypatch.setenv("RELAIS__FLUX", flux)
    monkeypatch.setenv("RELAIS__DEPUIS", depuis)
    monkeypatch.setenv("AES__TROUSSEAU__test__KEY", AES_KEY.hex())
    monkeypatch.setenv("AES__TROUSSEAU__test__IV", AES_IV.hex())
    runtime.vider_cache()


def _pipeline(tmp_path: Path, db: Path, nom: str = "relais_test") -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=nom,
        destination=dlt.destinations.duckdb(str(db)),
        dataset_name=NOM_DATASET,
        pipelines_dir=str(tmp_path / "pipelines"),
    )


def _zips_journalises(db: Path) -> list[str]:
    """Zips journalisés — table absente (aucun push réussi pour l'instant) → liste vide."""
    con = duckdb.connect(str(db), read_only=True)
    try:
        return [row[0] for row in con.execute(f'select "zip" from "{NOM_DATASET}"."{NOM_RESOURCE}"').fetchall()]
    except duckdb.CatalogException:
        return []
    finally:
        con.close()


@pytest.mark.integration
def test_bout_en_bout_un_zip_dechiffre_decompresse_pousse(tmp_path, monkeypatch):
    """Critère 1 : un zip chiffré local → déchiffré → décompressé → XML atterrit sur la
    cible file:// de test, record de livraison dans la DuckDB de destination."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db)

    executer(_pipeline(tmp_path, db))

    assert (cible / "ENEDIS_C15_20260615_001.xml").read_bytes() == b"<data>c15</data>"
    assert _zips_journalises(db) == ["ENEDIS_C15_20260615_001.zip"]


@pytest.mark.integration
def test_idempotence_second_run_ne_repousse_pas(tmp_path, monkeypatch):
    """Critère 2 : membership resource_state — un second run ne re-pousse aucun zip déjà livré."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db)

    executer(_pipeline(tmp_path, db))
    (cible / "ENEDIS_C15_20260615_001.xml").unlink()  # preuve qu'un 2e run ne le re-dépose pas

    executer(_pipeline(tmp_path, db))

    assert not (cible / "ENEDIS_C15_20260615_001.xml").exists()
    assert _zips_journalises(db) == ["ENEDIS_C15_20260615_001.zip"]  # une seule ligne, pas deux


@pytest.mark.integration
def test_echec_push_ne_marque_pas_livre_et_retente_au_run_suivant(tmp_path, monkeypatch):
    """Critère 3 (direction d'échec sûre) : cible injoignable au run 1 → zip NON enregistré,
    retenté et livré avec succès au run 2 (cible redevenue joignable)."""
    source = tmp_path / "source"
    cible_valide = tmp_path / "cible"
    cible_injoignable = Path("/n_existe_pas") / "sous_repertoire_impossible"
    db = tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")

    _configurer_env(monkeypatch, source, cible_injoignable, db)
    executer(_pipeline(tmp_path, db))  # le push échoue (permission denied à la racine) : catch, pas de crash
    assert _zips_journalises(db) == []  # PAS enregistré comme livré

    _configurer_env(monkeypatch, source, cible_valide, db)
    executer(_pipeline(tmp_path, db))  # retente : cible désormais valide
    assert (cible_valide / "ENEDIS_C15_20260615_001.xml").read_bytes() == b"<data>c15</data>"
    assert _zips_journalises(db) == ["ENEDIS_C15_20260615_001.zip"]


@pytest.mark.integration
def test_incremental_false_reliste_toute_la_source_a_chaque_run(tmp_path, monkeypatch):
    """Critère 4 : un zip déposé APRÈS le premier run est bien vu au second (pas de curseur
    qui aurait avancé au listing du premier — re-listing intégral, `incremental=False`)."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>un</data>")
    _configurer_env(monkeypatch, source, cible, db)

    executer(_pipeline(tmp_path, db))
    _deposer_zip(source, "ENEDIS_C15_20260616_002.zip", b"<data>deux</data>", date=(2026, 6, 16, 12, 0, 0))
    executer(_pipeline(tmp_path, db))

    assert (cible / "ENEDIS_C15_20260615_001.xml").exists()
    assert (cible / "ENEDIS_C15_20260616_002.xml").exists()
    assert set(_zips_journalises(db)) == {"ENEDIS_C15_20260615_001.zip", "ENEDIS_C15_20260616_002.zip"}


@pytest.mark.integration
def test_filtre_flux_configure_exclut_les_flux_non_retenus(tmp_path, monkeypatch):
    """Critère 5 (filtre) : RELAIS__FLUX=C15 → un zip R151 n'est ni poussé ni journalisé."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _deposer_zip(source, "ENEDIS_R151_20260615_002.zip", b"<data>r151</data>")
    _configurer_env(monkeypatch, source, cible, db, flux="C15")

    executer(_pipeline(tmp_path, db))

    assert (cible / "ENEDIS_C15_20260615_001.xml").exists()
    assert not (cible / "ENEDIS_R151_20260615_002.xml").exists()
    assert _zips_journalises(db) == ["ENEDIS_C15_20260615_001.zip"]


@pytest.mark.integration
def test_filtre_date_exclut_les_zips_anterieurs_a_depuis(tmp_path, monkeypatch):
    """Critère 5 (filtre) : RELAIS__DEPUIS exclut un zip antérieur à la fenêtre configurée."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260101_001.zip", b"<data>vieux</data>", date=(2026, 1, 1, 12, 0, 0))
    _deposer_zip(source, "ENEDIS_C15_20260615_002.zip", b"<data>neuf</data>")
    _configurer_env(monkeypatch, source, cible, db, depuis="2026-06-01")

    executer(_pipeline(tmp_path, db))

    assert not (cible / "ENEDIS_C15_20260101_001.xml").exists()
    assert (cible / "ENEDIS_C15_20260615_002.xml").exists()
    assert _zips_journalises(db) == ["ENEDIS_C15_20260615_002.zip"]


@pytest.mark.integration
def test_completude_liste_les_zips_source_jamais_relayes(tmp_path, monkeypatch):
    """Critère 6 : requête de complétude — zips source absents du journal de destination."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db, flux="R151")  # exclut le C15 déposé → jamais relayé

    executer(_pipeline(tmp_path, db))

    manquants = zips_non_relayes(f"file://{source}/", db)
    assert manquants == ["ENEDIS_C15_20260615_001.zip"]
