"""Relais de flux Enedis déchiffrés vers SFTP partenaire (#637) — chemin bout-en-bout.

Couvre les critères d'acceptation, dans l'ordre tracer-bullet de l'issue :
  1. chemin end-to-end sur UN fichier (déchiffré, décompressé, poussé, journalisé) ;
  2. idempotence : un second run ne re-pousse aucun zip déjà livré ;
  3. direction d'échec sûre : un push qui échoue ne marque PAS le zip comme livré ;
  4. incremental: false : re-liste l'intégralité de la source à chaque run ;
  5. filtre configurable (flux), en config ;
  6. vérification de complétude (zips reçus jamais relayés).

Complété (#643, revue de la PR #638) :
  7. le push réutilise `etape_chaine` (StatsRelais) — compte succès/échecs, journalise
     `statut='pousse'` ; `relais_aveugle()` = 0 push réussi et ≥1 échec, un échec isolé
     noyé dans des succès ne l'est pas ;
  8. amorçage explicite (`seed_avant`) : marque les zips antérieurs comme livrés sans les
     pousser, refuse si le journal est déjà peuplé (`force` outrepasse), journalise
     `statut='amorce'`.

Nécessite l'extra [ingestion] (dlt, PyCryptodome) : uv sync --extra ingestion
"""

import io
import os
import sys
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
from electricore.ingestion.relais.pipeline import (
    NOM_DATASET,
    NOM_RESOURCE,
    executer,
    seed_avant,
    zips_non_relayes,
)

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


def _configurer_env(monkeypatch, source: Path, cible: Path, db: Path, *, flux: str = ""):
    monkeypatch.setenv("RELAIS__SOURCE_URL", f"file://{source}/")
    monkeypatch.setenv("RELAIS__PARTNER_URL", f"file://{cible}/")
    monkeypatch.setenv("RELAIS__DESTINATION_DB", str(db))
    monkeypatch.setenv("RELAIS__FLUX", flux)
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
    """Zips effectivement LIVRÉS (`statut` 'pousse'/'amorce') — exclut 'vu'/'echec' (journal
    enrichi, #646) : table absente (aucun push réussi pour l'instant) → liste vide."""
    con = duckdb.connect(str(db), read_only=True)
    try:
        lignes = con.execute(
            f'select "zip" from "{NOM_DATASET}"."{NOM_RESOURCE}" where "statut" in (\'pousse\', \'amorce\')'
        ).fetchall()
        return [row[0] for row in lignes]
    except duckdb.CatalogException:
        return []
    finally:
        con.close()


def _statuts_journalises(db: Path) -> dict[str, str]:
    """`{zip: statut}` du journal — `'pousse'` (push réussi) ou `'amorce'` (seed, #643).

    N'utiliser que sur un journal où chaque zip n'a qu'UNE ligne (`dict()` collapse sinon
    silencieusement sur la dernière) — `_toutes_lignes_journal` pour les scénarios à
    plusieurs lignes par zip (retry après 'echec', #646)."""
    con = duckdb.connect(str(db), read_only=True)
    try:
        rows = con.execute(f'select "zip", "statut" from "{NOM_DATASET}"."{NOM_RESOURCE}"').fetchall()
        return dict(rows)
    except duckdb.CatalogException:
        return {}
    finally:
        con.close()


def _toutes_lignes_journal(db: Path) -> list[tuple[str, str]]:
    """`[(zip, statut), …]` — TOUTES les lignes, y compris les zips à plusieurs lignes
    (retry après 'echec') — journal enrichi (#646) : 'vu' / 'pousse' / 'amorce' / 'echec'."""
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(f'select "zip", "statut" from "{NOM_DATASET}"."{NOM_RESOURCE}"').fetchall()
    except duckdb.CatalogException:
        return []
    finally:
        con.close()


def _deposer_octets_chiffres_non_zip(bucket: Path, nom: str, date=(2026, 6, 15, 12, 0, 0)) -> Path:
    """Dépose un fichier déchiffrable (bonne clé AES) mais dont le contenu clair N'EST PAS
    un ZIP valide — decrypt réussit, `extract_files_from_zip` lève `BadZipFile` : isole
    l'échec à l'étage push, sans dépendre d'une cible injoignable (#643).

    Le contenu clair commence par le magic bytes ZIP (`PK\\x03\\x04`, oracle de l'étage
    decrypt, cf. `tests/ingestion/test_escalade_chaine.py`) mais n'a pas d'enregistrement
    de fin de catalogue → passe decrypt, échoue à l'extraction."""
    bucket.mkdir(parents=True, exist_ok=True)
    chemin = bucket / nom
    clair = b"PK\x03\x04" + b"ceci commence par le magic ZIP mais n'en est pas un" + b"\x00" * 16
    cipher = AES.new(AES_KEY, AES.MODE_CBC, AES_IV)
    chemin.write_bytes(cipher.encrypt(pad(clair, AES.block_size)))
    ts = time.mktime((*date, 0, 0, -1))
    os.utime(chemin, (ts, ts))
    return chemin


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
def test_completude_liste_les_zips_source_jamais_relayes(tmp_path, monkeypatch):
    """Critère 6 : requête de complétude — zips source absents du journal de destination."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db, flux="R151")  # exclut le C15 déposé → jamais relayé

    executer(_pipeline(tmp_path, db))

    manquants = zips_non_relayes(f"file://{source}/", db)
    assert manquants == ["ENEDIS_C15_20260615_001.zip"]


@pytest.mark.integration
def test_completude_reste_correcte_avec_un_zip_en_echec(tmp_path, monkeypatch):
    """Journal enrichi (#646) : un zip journalisé `statut='echec'` (push qui a échoué) DOIT
    rester « manquant » pour `zips_non_relayes` — sinon un échec de push disparaîtrait à
    tort de la complétude dès sa première tentative (la sémantique « jamais relayé » ne
    doit filtrer que 'pousse'/'amorce', pas toute présence dans le journal)."""
    source = tmp_path / "source"
    cible_injoignable = Path("/n_existe_pas") / "sous_repertoire_impossible"
    db = tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible_injoignable, db)

    executer(_pipeline(tmp_path, db))  # journalise 'echec' (cible injoignable)

    manquants = zips_non_relayes(f"file://{source}/", db)
    assert manquants == ["ENEDIS_C15_20260615_001.zip"]


# =============================================================================
# Critère 7 (#643) : push via etape_chaine — StatsRelais, statut journalisé, escalade
# =============================================================================


@pytest.mark.integration
def test_push_reussi_compte_stats_et_journalise_statut_pousse(tmp_path, monkeypatch):
    """`_pousser` réutilise `etape_chaine` : un push réussi incrémente `stats.pousses`,
    journalise `statut='pousse'`."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db)

    info, stats = executer(_pipeline(tmp_path, db))

    assert (stats.candidats, stats.pousses, stats.echecs_push) == (1, 1, 0)
    assert stats.relais_aveugle() is False
    assert _statuts_journalises(db) == {"ENEDIS_C15_20260615_001.zip": "pousse"}


@pytest.mark.integration
def test_run_tous_push_echoues_est_aveugle(tmp_path, monkeypatch):
    """Critère escalade : des candidats mais 0 push réussi et ≥1 échec → `relais_aveugle()`
    vrai (un relais qui retenterait pour toujours en silence sinon, le reproche fait à
    inotify dans #637)."""
    source = tmp_path / "source"
    cible_injoignable = Path("/n_existe_pas") / "sous_repertoire_impossible"
    db = tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible_injoignable, db)

    info, stats = executer(_pipeline(tmp_path, db))

    assert (stats.candidats, stats.pousses, stats.echecs_push) == (1, 0, 1)
    assert stats.relais_aveugle() is True


@pytest.mark.integration
def test_echec_isole_parmi_des_succes_n_est_pas_aveugle(tmp_path, monkeypatch):
    """Critère escalade : un échec isolé noyé dans des push réussis ne fait PAS échouer
    le run (`relais_aveugle()` faux) — retenté au run suivant, comme avant #643."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>ok</data>")
    _deposer_octets_chiffres_non_zip(source, "ENEDIS_C15_20260615_002.zip", date=(2026, 6, 15, 13, 0, 0))
    _configurer_env(monkeypatch, source, cible, db)

    info, stats = executer(_pipeline(tmp_path, db))

    assert (stats.candidats, stats.pousses, stats.echecs_push) == (2, 1, 1)
    assert stats.relais_aveugle() is False


# =============================================================================
# Critère 8 (#643) : amorçage explicite (`relais seed --avant`)
# =============================================================================


@pytest.mark.integration
def test_seed_marque_livre_sans_pousser_et_journalise_statut_amorce(tmp_path, monkeypatch):
    """`seed_avant` marque un zip antérieur comme livré SANS le pousser (rien sur la cible),
    journalise `statut='amorce'`, et un run normal qui suit ne le pousse pas non plus
    (même état `zips_livrés` que le push)."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260101_001.zip", b"<data>vieux</data>", date=(2026, 1, 1, 12, 0, 0))
    _configurer_env(monkeypatch, source, cible, db)

    seed_avant("2026-06-01", pipeline=_pipeline(tmp_path, db))

    assert not (cible / "ENEDIS_C15_20260101_001.xml").exists()
    assert _statuts_journalises(db) == {"ENEDIS_C15_20260101_001.zip": "amorce"}

    executer(_pipeline(tmp_path, db))  # run normal : ne repousse pas le zip amorcé

    assert not (cible / "ENEDIS_C15_20260101_001.xml").exists()
    assert _statuts_journalises(db) == {"ENEDIS_C15_20260101_001.zip": "amorce"}


@pytest.mark.integration
def test_seed_n_amorce_pas_les_zips_posterieurs_a_avant(tmp_path, monkeypatch):
    """Seuls les zips strictement antérieurs à `--avant` sont amorcés — les nouveaux zips
    restent candidats à un push normal."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260101_001.zip", b"<data>vieux</data>", date=(2026, 1, 1, 12, 0, 0))
    _deposer_zip(source, "ENEDIS_C15_20260615_002.zip", b"<data>neuf</data>")
    _configurer_env(monkeypatch, source, cible, db)

    seed_avant("2026-06-01", pipeline=_pipeline(tmp_path, db))
    executer(_pipeline(tmp_path, db))

    assert not (cible / "ENEDIS_C15_20260101_001.xml").exists()
    assert (cible / "ENEDIS_C15_20260615_002.xml").exists()
    assert _statuts_journalises(db) == {
        "ENEDIS_C15_20260101_001.zip": "amorce",
        "ENEDIS_C15_20260615_002.zip": "pousse",
    }


@pytest.mark.integration
def test_seed_refuse_si_journal_deja_peuple(tmp_path, monkeypatch):
    """Garde-fou (#643) : le seed refuse si le journal contient déjà des livraisons —
    lancé par erreur après la mise en service, il enterrerait silencieusement tout ce
    qui restait à relayer."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>deja livre</data>")
    _configurer_env(monkeypatch, source, cible, db)
    executer(_pipeline(tmp_path, db))  # peuple le journal (statut='pousse')

    with pytest.raises(RuntimeError, match="Amorçage refusé"):
        seed_avant("2026-06-01", pipeline=_pipeline(tmp_path, db))


@pytest.mark.integration
def test_seed_force_outrepasse_le_refus(tmp_path, monkeypatch):
    """`force=True` outrepasse le refus — l'opérateur qui sait ce qu'il fait."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>deja livre</data>")
    _deposer_zip(source, "ENEDIS_C15_20260101_002.zip", b"<data>vieux</data>", date=(2026, 1, 1, 12, 0, 0))
    _configurer_env(monkeypatch, source, cible, db)
    executer(_pipeline(tmp_path, db))  # peuple le journal

    seed_avant("2026-06-01", force=True, pipeline=_pipeline(tmp_path, db))  # ne lève pas

    assert _statuts_journalises(db)["ENEDIS_C15_20260101_002.zip"] == "amorce"


# =============================================================================
# Journal enrichi (#646) : tout zip VU au balayage est journalisé (pas seulement livré)
# =============================================================================


@pytest.mark.integration
def test_echec_push_journalise_statut_echec(tmp_path, monkeypatch):
    """Journal enrichi : un push qui échoue (cible injoignable) journalise une ligne
    `statut='echec'` — le zip reste visible dans le journal, pas seulement absent."""
    source = tmp_path / "source"
    cible_injoignable = Path("/n_existe_pas") / "sous_repertoire_impossible"
    db = tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible_injoignable, db)

    executer(_pipeline(tmp_path, db))

    assert _toutes_lignes_journal(db) == [("ENEDIS_C15_20260615_001.zip", "echec")]
    assert _zips_journalises(db) == []  # toujours pas considéré livré


@pytest.mark.integration
def test_zip_exclu_par_filtre_flux_est_journalise_vu(tmp_path, monkeypatch):
    """Un zip vu au balayage mais exclu par le filtre flux (jamais candidat au push) est
    tout de même journalisé `statut='vu'` — l'audit de réception ne dépend pas du routage
    configuré côté relais (R151 compris si `RELAIS__FLUX` ne le liste pas)."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _deposer_zip(source, "ENEDIS_R151_20260615_002.zip", b"<data>r151</data>")
    _configurer_env(monkeypatch, source, cible, db, flux="C15")

    executer(_pipeline(tmp_path, db))

    lignes = dict(_toutes_lignes_journal(db))
    assert lignes["ENEDIS_C15_20260615_001.zip"] == "pousse"
    assert lignes["ENEDIS_R151_20260615_002.zip"] == "vu"


@pytest.mark.integration
def test_zip_vu_n_est_pas_rejournalise_au_run_suivant(tmp_path, monkeypatch):
    """Un zip déjà journalisé `statut='vu'` ne l'est pas une seconde fois au run suivant —
    une ligne par zip par issue, pas un doublon à chaque balayage réconciliant."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_R151_20260615_002.zip", b"<data>r151</data>")
    _configurer_env(monkeypatch, source, cible, db, flux="C15")

    executer(_pipeline(tmp_path, db))
    executer(_pipeline(tmp_path, db))

    assert _toutes_lignes_journal(db) == [("ENEDIS_R151_20260615_002.zip", "vu")]


# =============================================================================
# Vérification d'écriture (#646) : taille distante vs locale AVANT de marquer livré
# =============================================================================


@pytest.mark.integration
def test_ecriture_tronquee_ne_marque_pas_livre_et_retente(tmp_path, monkeypatch):
    """Critère : un mismatch taille distante/locale (dépôt tronqué) ne marque PAS le zip
    livré — retenté au passage suivant, poussé avec succès une fois la vérification saine."""
    from electricore.ingestion.relais import pipeline as pipeline_module

    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db)

    verif_originale = pipeline_module._verifier_ecriture
    monkeypatch.setattr(
        pipeline_module,
        "_verifier_ecriture",
        lambda fs, chemin, taille_locale: (_ for _ in ()).throw(OSError("tronqué")),
    )
    info, stats = executer(_pipeline(tmp_path, db))
    assert _zips_journalises(db) == []  # PAS marqué livré
    assert (stats.candidats, stats.pousses, stats.echecs_push) == (1, 0, 1)

    monkeypatch.setattr(pipeline_module, "_verifier_ecriture", verif_originale)  # vérification désormais saine
    executer(_pipeline(tmp_path, db))  # retente
    assert (cible / "ENEDIS_C15_20260615_001.xml").read_bytes() == b"<data>c15</data>"
    assert _zips_journalises(db) == ["ENEDIS_C15_20260615_001.zip"]


# =============================================================================
# CLI (__main__.py, #643) : escalade en sortie de process, sous-commande seed
# =============================================================================


@pytest.mark.integration
def test_cli_run_aveugle_sort_en_erreur(tmp_path, monkeypatch):
    """`main()` sort en non-zéro quand `relais_aveugle()` — l'escalade s'arrête au
    processus (systemd marque l'unité failed), pas de retry silencieux pour toujours."""
    from electricore.ingestion.relais.__main__ import main

    source = tmp_path / "source"
    cible_injoignable = Path("/n_existe_pas") / "sous_repertoire_impossible"
    db = tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible_injoignable, db)
    monkeypatch.setattr(sys, "argv", ["relais"])

    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code != 0


@pytest.mark.integration
def test_cli_run_normal_reussi_ne_sort_pas_en_erreur(tmp_path, monkeypatch):
    """Un run normal réussi ne lève pas — `pipelines_dir` épinglé (`destination_db.parent`,
    #643) isole l'état de test sans pipeline injecté (sinon dlt tomberait sur
    `~/.dlt/pipelines`, partagé entre tests)."""
    from electricore.ingestion.relais.__main__ import main

    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db)
    monkeypatch.setattr(sys, "argv", ["relais"])

    main()  # ne lève pas

    assert (cible / "ENEDIS_C15_20260615_001.xml").exists()


@pytest.mark.integration
def test_cli_seed_marque_livre_et_refuse_sans_force_si_deja_peuple(tmp_path, monkeypatch):
    """`relais seed --avant <date>` marque les zips antérieurs livrés sans les pousser ;
    relancé sans `--force` alors que le journal est déjà peuplé → refuse (sortie non-zéro)."""
    from electricore.ingestion.relais.__main__ import main

    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260101_001.zip", b"<data>vieux</data>", date=(2026, 1, 1, 12, 0, 0))
    _configurer_env(monkeypatch, source, cible, db)
    monkeypatch.setattr(sys, "argv", ["relais", "seed", "--avant", "2026-06-01"])

    main()  # ne lève pas

    assert not (cible / "ENEDIS_C15_20260101_001.xml").exists()
    assert _statuts_journalises(db) == {"ENEDIS_C15_20260101_001.zip": "amorce"}

    with pytest.raises(SystemExit) as exc:
        main()  # relancé sans --force : journal déjà peuplé → refuse
    assert exc.value.code != 0
