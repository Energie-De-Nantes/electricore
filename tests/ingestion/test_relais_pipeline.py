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
from datetime import UTC, datetime
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
    StatsRelais,
    _dechiffrer_et_observer,
    executer,
    seed_avant,
    zips_non_relayes,
)

# Monkeypatcher des fonctions internes du module (`_verifier_ecriture`) se fait via
# `executer.__globals__` (le namespace où `executer` résout ses appels), PAS via un `import
# electricore.ingestion.relais.pipeline as pipeline_module` local à un test : `test_relais_
# independance.py` fait `del sys.modules[...]` puis ré-importe (garde dynamique) — un import
# local exécuté APRÈS ce reload (ordre alphabétique de fichiers) résoudrait un AUTRE objet
# module que celui qu'`executer` référence en interne, et un monkeypatch dessus n'aurait
# alors aucun effet sur le code réellement exécuté (bug constaté, #646).

AES_KEY = bytes.fromhex("0102030405060708090a0b0c0d0e0f10")
AES_IV = bytes.fromhex("1112131415161718191a1b1c1d1e1f20")

# Clé/IV JAMAIS enregistrés dans le trousseau de test (`_configurer_env`) — simule un zip
# chiffré avec une clé Enedis que le trousseau configuré ne connaît pas (rotation, #692).
AES_KEY_INCONNUE = bytes.fromhex("ff" * 16)
AES_IV_INCONNUE = bytes.fromhex("ee" * 16)


@pytest.fixture(autouse=True)
def _isoler_env(monkeypatch):
    """Isole le domaine runtime : .env du dépôt neutralisé, cache vidé (cf. tests crypto)."""
    monkeypatch.setattr(runtime, "FICHIER_ENV", None)
    runtime.vider_cache()
    yield
    runtime.vider_cache()


def _zip_chiffre_multi(fichiers: list[tuple[str, bytes]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for nom_interne, contenu in fichiers:
            zf.writestr(nom_interne, contenu)
    cipher = AES.new(AES_KEY, AES.MODE_CBC, AES_IV)
    return cipher.encrypt(pad(buf.getvalue(), AES.block_size))


def _zip_chiffre(nom_interne: str, contenu: bytes) -> bytes:
    return _zip_chiffre_multi([(nom_interne, contenu)])


def _deposer_zip_multi(bucket: Path, nom: str, fichiers: list[tuple[str, bytes]], date=(2026, 6, 15, 12, 0, 0)) -> Path:
    """Dépose un zip chiffré à PLUSIEURS fichiers internes — intra-zip (#646)."""
    bucket.mkdir(parents=True, exist_ok=True)
    chemin = bucket / nom
    chemin.write_bytes(_zip_chiffre_multi(fichiers))
    ts = time.mktime((*date, 0, 0, -1))
    os.utime(chemin, (ts, ts))
    return chemin


def _deposer_zip(bucket: Path, nom: str, contenu_interne: bytes, date=(2026, 6, 15, 12, 0, 0)) -> Path:
    return _deposer_zip_multi(bucket, nom, [(f"{nom.replace('.zip', '')}.xml", contenu_interne)], date=date)


def _deposer_zip_cle_inconnue(bucket: Path, nom: str, contenu_interne: bytes, date=(2026, 6, 15, 12, 0, 0)) -> Path:
    """Dépose un zip chiffré avec `AES_KEY_INCONNUE` — jamais dans le trousseau configuré par
    `_configurer_env` (`AES_KEY`) : simule une rotation de clé Enedis (#692), decrypt échoue
    (`ValueError`, oracle padding/magic bytes), zip noté dans `zips_indechiffrables` (source
    du prédicat et du journal, #695), jamais poussé, jamais journalisé `'vu'`."""
    bucket.mkdir(parents=True, exist_ok=True)
    chemin = bucket / nom
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{nom.replace('.zip', '')}.xml", contenu_interne)
    cipher = AES.new(AES_KEY_INCONNUE, AES.MODE_CBC, AES_IV_INCONNUE)
    chemin.write_bytes(cipher.encrypt(pad(buf.getvalue(), AES.block_size)))
    ts = time.mktime((*date, 0, 0, -1))
    os.utime(chemin, (ts, ts))
    return chemin


def _configurer_env(
    monkeypatch, source: Path, cible: Path, db: Path, *, flux: str = "", cle: bytes = AES_KEY, iv: bytes = AES_IV
):
    """`cle`/`iv` (#695) : défauts = trousseau correct (`AES_KEY`/`AES_IV`) ; passer
    `cle=AES_KEY_INCONNUE, iv=AES_IV_INCONNUE` simule un trousseau entièrement faux (rotation
    de clé Enedis, #692) — remplace l'ancienne fixture dédiée, dupliquée à 7/8 lignes près."""
    monkeypatch.setenv("RELAIS__SOURCE_URL", f"file://{source}/")
    monkeypatch.setenv("RELAIS__PARTNER_URL", f"file://{cible}/")
    monkeypatch.setenv("RELAIS__DESTINATION_DB", str(db))
    monkeypatch.setenv("RELAIS__FLUX", flux)
    monkeypatch.setenv("AES__TROUSSEAU__test__KEY", cle.hex())
    monkeypatch.setenv("AES__TROUSSEAU__test__IV", iv.hex())
    runtime.vider_cache()


def _pipeline(tmp_path: Path, db: Path, nom: str = "relais_test") -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=nom,
        destination=dlt.destinations.duckdb(str(db)),
        dataset_name=NOM_DATASET,
        pipelines_dir=str(tmp_path / "pipelines"),
    )


# NB : ces trois helpers ouvrent une connexion LECTURE-ÉCRITURE (pas `read_only=True`) —
# `executer()` enchaîne un `dbt build` en fin de passe (#646) ; une connexion `read_only`
# juste après aurait une config DuckDB incompatible avec la connexion dbt encore en vie
# dans ce même process (même piège documenté dans `runner.py::bilan`).


def _zips_journalises(db: Path) -> list[str]:
    """Zips effectivement LIVRÉS (`statut` 'pousse'/'amorce') — exclut 'vu'/'echec' (journal
    enrichi, #646) : table absente (aucun push réussi pour l'instant) → liste vide."""
    con = duckdb.connect(str(db))
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
    con = duckdb.connect(str(db))
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
    con = duckdb.connect(str(db))
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

    assert (cible / "C15" / "ENEDIS_C15_20260615_001.xml").read_bytes() == b"<data>c15</data>"
    assert _zips_journalises(db) == ["ENEDIS_C15_20260615_001.zip"]


@pytest.mark.integration
def test_idempotence_second_run_ne_repousse_pas(tmp_path, monkeypatch):
    """Critère 2 : membership resource_state — un second run ne re-pousse aucun zip déjà livré."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db)

    executer(_pipeline(tmp_path, db))
    (cible / "C15" / "ENEDIS_C15_20260615_001.xml").unlink()  # preuve qu'un 2e run ne le re-dépose pas

    executer(_pipeline(tmp_path, db))

    assert not (cible / "C15" / "ENEDIS_C15_20260615_001.xml").exists()
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
    assert (cible_valide / "C15" / "ENEDIS_C15_20260615_001.xml").read_bytes() == b"<data>c15</data>"
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

    assert (cible / "C15" / "ENEDIS_C15_20260615_001.xml").exists()
    assert (cible / "C15" / "ENEDIS_C15_20260616_002.xml").exists()
    assert set(_zips_journalises(db)) == {"ENEDIS_C15_20260615_001.zip", "ENEDIS_C15_20260616_002.zip"}


@pytest.mark.integration
def test_filtre_flux_configure_exclut_les_flux_non_retenus(tmp_path, monkeypatch):
    """Critère 5 (filtre) : RELAIS__FLUX=C15 → un zip R151 n'est ni poussé ni journalisé."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _deposer_zip(source, "ENEDIS_R151_20260615_002.zip", b"<data>r151</data>")
    _configurer_env(monkeypatch, source, cible, db, flux="C15")

    executer(_pipeline(tmp_path, db))

    assert (cible / "C15" / "ENEDIS_C15_20260615_001.xml").exists()
    assert not (cible / "R151" / "ENEDIS_R151_20260615_002.xml").exists()
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
# Dépôt par flux chez le partenaire (#686) : <partner_url>/<CODE_FLUX>/<fichier>
# =============================================================================


@pytest.mark.integration
@pytest.mark.parametrize("zip_name", ["sansflux.zip", "ENEDIS__20260615_001.zip"])
def test_zip_sans_code_flux_echoue_et_ne_depose_rien(tmp_path, monkeypatch, zip_name):
    """Zip dont le nom ne porte pas de code flux — moins de deux segments `_`-délimités, ou
    2e segment VIDE (`ENEDIS__…`, qui concaténé donnerait un chemin racine). Jamais rencontré
    sur un vrai zip Enedis, inatteignable quand `RELAIS__FLUX` est renseigné (ici filtre
    désactivé pour atteindre le push) : le push lève AVANT tout dépôt — rien à la racine ni
    dans un sous-dossier, échec compté, ligne journal `echec`."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, zip_name, b"<data>orpheline</data>")
    _configurer_env(monkeypatch, source, cible, db)  # flux="" : filtre désactivé

    info, stats = executer(_pipeline(tmp_path, db))

    assert not cible.exists() or list(cible.iterdir()) == []  # rien déposé, nulle part
    assert (stats.candidats, stats.pousses, stats.echecs_push) == (1, 0, 1)
    assert dict(_toutes_lignes_journal(db))[zip_name] == "echec"


# =============================================================================
# #683 : zips_non_relayes — db_path str, bruit des flux hors liste
# =============================================================================


@pytest.mark.integration
def test_completude_accepte_un_db_path_str(tmp_path, monkeypatch):
    """`zips_non_relayes` reste appelable ad hoc (notebook, `python -c`) : la commande
    naturelle passe une string, pas un `Path` — coercion en tête de fonction (#683)."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db, flux="R151")  # exclut le C15 déposé → jamais relayé

    executer(_pipeline(tmp_path, db))

    manquants = zips_non_relayes(f"file://{source}/", str(db))  # str, pas Path
    assert manquants == ["ENEDIS_C15_20260615_001.zip"]


@pytest.mark.integration
def test_completude_flux_filtres_exclut_les_zips_hors_liste(tmp_path, monkeypatch):
    """Avec `flux_filtres`, les zips hors liste (jamais dus au partenaire, ex. X13/LTE01)
    n'apparaissent plus dans l'écart — même sémantique que `_match_flux` (#683) : le
    R151 filtré côté relais (`RELAIS__FLUX=C15`) reste « vu » mais jamais relayé, or ce
    n'est pas ce bruit que l'opérateur cherche quand il demande « qu'est-ce qui manque à
    Haulogy ? »."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _deposer_zip(source, "ENEDIS_R151_20260615_002.zip", b"<data>r151</data>")
    _configurer_env(monkeypatch, source, cible, db, flux="C15")  # seul C15 est relayé

    executer(_pipeline(tmp_path, db))  # C15 poussé ; R151 journalisé 'vu', jamais relayé

    assert zips_non_relayes(f"file://{source}/", db) == ["ENEDIS_R151_20260615_002.zip"]
    assert zips_non_relayes(f"file://{source}/", db, flux_filtres={"C15"}) == []


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

    assert not (cible / "C15" / "ENEDIS_C15_20260101_001.xml").exists()
    assert (cible / "C15" / "ENEDIS_C15_20260615_002.xml").exists()
    assert _statuts_journalises(db) == {
        "ENEDIS_C15_20260101_001.zip": "amorce",
        "ENEDIS_C15_20260615_002.zip": "pousse",
    }


@pytest.mark.integration
def test_seed_avant_retourne_le_compte_de_zips_amorces(tmp_path, monkeypatch):
    """#684 : `seed_avant` retourne `(info, n_amorces)` — le chiffre qui compte (combien de
    zips ont été marqués livrés), pas seulement l'`info` dlt verbeuse. Un zip postérieur à
    `--avant` n'est pas amorcé, ne compte donc pas."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260101_001.zip", b"<data>vieux</data>", date=(2026, 1, 1, 12, 0, 0))
    _deposer_zip(source, "ENEDIS_C15_20260102_002.zip", b"<data>vieux aussi</data>", date=(2026, 1, 2, 12, 0, 0))
    _deposer_zip(source, "ENEDIS_C15_20260615_003.zip", b"<data>neuf</data>")
    _configurer_env(monkeypatch, source, cible, db)

    info, n_amorces = seed_avant("2026-06-01", pipeline=_pipeline(tmp_path, db))

    assert n_amorces == 2


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
# Vérité unique des zips indéchiffrables (#695) : `relais_aveugle()` dérive des NOMS
# collectés (`zips_indechiffrables`), pas du compteur interne de la brique decrypt
# (`chaine.echecs_dechiffrement`) — test pur, sans dlt, sur `StatsRelais` seul.
# =============================================================================


def test_relais_aveugle_lit_zips_indechiffrables_pas_le_compteur_chaine():
    """Preuve de l'unification (#695) : `chaine.echecs_dechiffrement` reste à zéro (la brique
    n'a pas tourné) mais `zips_indechiffrables` porte un nom — `relais_aveugle()` doit être
    vrai. Avant #695 (prédicat lisant `chaine.echecs_dechiffrement`), ce test échoue : c'est
    exactement le scénario grave que #692 combat (journal plein d'`echec` mais compteur à
    zéro à cause d'une brique désynchronisée)."""
    stats = StatsRelais(pousses=0, zips_indechiffrables=["ENEDIS_C15_20260615_001.zip"])

    assert stats.chaine.echecs_dechiffrement == 0  # compteur interne resté à zéro
    assert stats.relais_aveugle() is True  # les noms suffisent à faire escalader


class _FichierChiffreEnMemoire(dict):
    """Sosie minimal de `FileItemDict` (dict + `.open()`) — exerce le wrapper observateur via
    le seam de crypto.py (`_decrypt_aes_transformer_base`), sans dlt ni système de fichiers."""

    def __init__(self, nom: str, contenu: bytes):
        super().__init__(file_name=nom, modification_date=datetime(2026, 6, 15, tzinfo=UTC))
        self._contenu = contenu

    def open(self):
        return io.BytesIO(self._contenu)


def _zip_chiffre(contenu_interne: bytes, cle: bytes, iv: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("doc.xml", contenu_interne)
    return AES.new(cle, AES.MODE_CBC, iv).encrypt(pad(buf.getvalue(), AES.block_size))


def test_wrapper_observateur_zip_indechiffrable_note_le_nom_sans_yield():
    """Invariant du wrapper (#695) : la brique ne produit AUCUN document (toutes clés KO,
    `ValueError` avalée par la discipline `etape_chaine`) ⟺ le nom est noté dans
    `zips_indechiffrables`. Le compteur interne de la brique reste au diapason — c'est la
    garde anti-dérive brique↔noms au grain unitaire."""
    stats = StatsRelais()
    fichier = _FichierChiffreEnMemoire(
        "ENEDIS_C15_20260615_001.zip", _zip_chiffre(b"<data>ko</data>", AES_KEY_INCONNUE, AES_IV_INCONNUE)
    )

    docs = list(_dechiffrer_et_observer(fichier, [("test", AES_KEY, AES_IV)], stats))

    assert docs == []
    assert stats.zips_indechiffrables == ["ENEDIS_C15_20260615_001.zip"]
    assert stats.chaine.echecs_dechiffrement == 1  # compteur et noms au diapason (anti-dérive)


def test_wrapper_observateur_zip_dechiffrable_yield_le_doc_sans_noter():
    """Réciproque de l'invariant (#695) : la brique yield exactement un document (clé du
    trousseau correcte) ⟺ rien n'est noté dans `zips_indechiffrables`."""
    stats = StatsRelais()
    fichier = _FichierChiffreEnMemoire("ENEDIS_C15_20260615_001.zip", _zip_chiffre(b"<data>ok</data>", AES_KEY, AES_IV))

    docs = list(_dechiffrer_et_observer(fichier, [("test", AES_KEY, AES_IV)], stats))

    assert [doc["file_name"] for doc in docs] == ["ENEDIS_C15_20260615_001.zip"]
    assert stats.zips_indechiffrables == []
    assert (stats.chaine.fichiers, stats.chaine.dechiffres) == (1, 1)


@pytest.mark.integration
def test_trousseau_entierement_faux_run_aveugle_et_indechiffrables_journalises(tmp_path, monkeypatch):
    """Critère d'acceptation #692 : trousseau entièrement faux → aucun push n'est même
    tenté (rien ne franchit decrypt), `relais_aveugle()` doit malgré tout être vrai (prédicat
    étendu au déchiffrement, lu depuis `zips_indechiffrables` — #695) et chaque zip
    indéchiffrable journalisé `'echec'`."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db, cle=AES_KEY_INCONNUE, iv=AES_IV_INCONNUE)

    info, stats = executer(_pipeline(tmp_path, db))

    assert (stats.candidats, stats.pousses, stats.echecs_push) == (0, 0, 0)  # rien n'atteint le push
    assert stats.chaine.echecs_dechiffrement == 1  # compteur interne de la brique (#695) : toujours tenu
    assert len(stats.zips_indechiffrables) == 1  # source unique du prédicat (#695)
    assert stats.relais_aveugle() is True
    assert dict(_toutes_lignes_journal(db))["ENEDIS_C15_20260615_001.zip"] == "echec"
    assert not (cible / "C15").exists()


@pytest.mark.integration
def test_trousseau_mixte_echecs_dechiffrement_comptes_journalises_et_retentes(tmp_path, monkeypatch):
    """Critère d'acceptation #692 : trousseau mixte (une partie des zips déchiffre) → exit 0
    (un push a réussi, tolérant), l'échec de déchiffrement isolé est compté et journalisé —
    et retenté (pas de 'vu', pas de dédup) au run suivant tant que la clé manque."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>ok</data>")
    _deposer_zip_cle_inconnue(source, "ENEDIS_C15_20260615_002.zip", b"<data>ko</data>", date=(2026, 6, 15, 13, 0, 0))
    _configurer_env(monkeypatch, source, cible, db)  # trousseau correct pour 001 seulement

    info, stats = executer(_pipeline(tmp_path, db))

    assert stats.chaine.echecs_dechiffrement == 1  # compteur interne de la brique (#695) : toujours tenu
    assert len(stats.zips_indechiffrables) == 1  # source unique du prédicat (#695)
    assert stats.pousses == 1
    assert stats.relais_aveugle() is False  # pousses > 0 ⇒ jamais aveugle (tolérant)
    lignes = dict(_toutes_lignes_journal(db))
    assert lignes["ENEDIS_C15_20260615_001.zip"] == "pousse"
    assert lignes["ENEDIS_C15_20260615_002.zip"] == "echec"

    executer(_pipeline(tmp_path, db))  # retente : clé toujours absente, la rotation dure

    statuts_002 = [
        statut for zip_name, statut in _toutes_lignes_journal(db) if zip_name == "ENEDIS_C15_20260615_002.zip"
    ]
    assert statuts_002 == ["echec", "echec"]  # une ligne par run, pas de dédup, jamais 'vu'


@pytest.mark.integration
def test_zip_indechiffrable_reste_dans_l_ecart_de_completude(tmp_path, monkeypatch):
    """Critère d'acceptation #692 : un zip indéchiffrable n'est jamais journalisé 'vu' — il
    reste donc dans l'écart de `zips_non_relayes` (sous-jacent à la sous-commande
    `completude`), comme n'importe quel autre échec."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip_cle_inconnue(source, "ENEDIS_C15_20260615_001.zip", b"<data>ko</data>")
    _configurer_env(monkeypatch, source, cible, db)

    executer(_pipeline(tmp_path, db))

    assert zips_non_relayes(f"file://{source}/", db) == ["ENEDIS_C15_20260615_001.zip"]


@pytest.mark.integration
def test_cli_run_trousseau_faux_sort_en_erreur_avec_resume_dechiffrement(tmp_path, monkeypatch, capsys):
    """Critère d'acceptation #692 : le résumé CLI enrichi affiche les zips entrés au
    déchiffrement et les indéchiffrables — sinon `❌ Relais aveugle : 0 candidat(s), 0
    poussé(s), 0 échec(s)` est illisible quand le déchiffrement, pas le push, est en cause."""
    from electricore.ingestion.relais.__main__ import main

    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db, cle=AES_KEY_INCONNUE, iv=AES_IV_INCONNUE)
    monkeypatch.setattr(sys, "argv", ["relais"])

    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code != 0

    sortie = capsys.readouterr().out
    assert "❌ Relais aveugle" in sortie
    assert "1 entré(s) au déchiffrement" in sortie
    assert "1 indéchiffrable(s)" in sortie


# =============================================================================
# Vérification d'écriture (#646) : taille distante vs locale AVANT de marquer livré
# =============================================================================


@pytest.mark.integration
def test_ecriture_tronquee_ne_marque_pas_livre_et_retente(tmp_path, monkeypatch):
    """Critère : un mismatch taille distante/locale (dépôt tronqué) ne marque PAS le zip
    livré — retenté au passage suivant, poussé avec succès une fois la vérification saine."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db)

    verif_originale = executer.__globals__["_verifier_ecriture"]
    monkeypatch.setitem(
        executer.__globals__,
        "_verifier_ecriture",
        lambda fs, chemin, taille_locale: (_ for _ in ()).throw(OSError("tronqué")),
    )
    info, stats = executer(_pipeline(tmp_path, db))
    assert _zips_journalises(db) == []  # PAS marqué livré
    assert (stats.candidats, stats.pousses, stats.echecs_push) == (1, 0, 1)

    monkeypatch.setitem(executer.__globals__, "_verifier_ecriture", verif_originale)  # vérification désormais saine
    executer(_pipeline(tmp_path, db))  # retente
    assert (cible / "C15" / "ENEDIS_C15_20260615_001.xml").read_bytes() == b"<data>c15</data>"
    assert _zips_journalises(db) == ["ENEDIS_C15_20260615_001.zip"]


# =============================================================================
# Contrôle intra-zip au dézippage (#646) : compteur X/Y, exception R151, F15
# =============================================================================


@pytest.mark.integration
def test_intra_zip_incomplet_bloque_le_push(tmp_path, monkeypatch):
    """Un zip C15 annonçant 3 fichiers (`_XXXXX_00003`) mais n'en contenant que 2 → rien
    n'est poussé, le zip reste non-livré, échec compté et alerté."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    zip_name = "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_00001_20260615120000.zip"
    _deposer_zip_multi(
        source,
        zip_name,
        [
            ("17X100A100A0001A_C15_17X000001117366M_GRD-F139_00017_00001_00003.xml", b"un"),
            ("17X100A100A0001A_C15_17X000001117366M_GRD-F139_00017_00002_00003.xml", b"deux"),
            # rang 00003 manque
        ],
    )
    _configurer_env(monkeypatch, source, cible, db)

    info, stats = executer(_pipeline(tmp_path, db))

    assert not cible.exists() or list(cible.iterdir()) == []  # rien poussé pour ce zip
    assert (stats.candidats, stats.pousses, stats.echecs_push) == (1, 0, 1)
    assert dict(_toutes_lignes_journal(db))[zip_name] == "echec"


@pytest.mark.integration
def test_intra_zip_complet_pousse_normalement(tmp_path, monkeypatch):
    """Les 3 rangs annoncés sont tous présents → push normal, aucune anomalie."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    zip_name = "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_00001_20260615120000.zip"
    _deposer_zip_multi(
        source,
        zip_name,
        [
            ("17X100A100A0001A_C15_17X000001117366M_GRD-F139_00017_00001_00003.xml", b"un"),
            ("17X100A100A0001A_C15_17X000001117366M_GRD-F139_00017_00002_00003.xml", b"deux"),
            ("17X100A100A0001A_C15_17X000001117366M_GRD-F139_00017_00003_00003.xml", b"trois"),
        ],
    )
    _configurer_env(monkeypatch, source, cible, db)

    info, stats = executer(_pipeline(tmp_path, db))

    assert (stats.candidats, stats.pousses, stats.echecs_push) == (1, 1, 0)
    assert _zips_journalises(db) == [zip_name]


@pytest.mark.integration
def test_intra_zip_totaux_incoherents_bloque(tmp_path, monkeypatch):
    """Des totaux Y distincts entre fichiers d'une même archive (`_00001_00002` +
    `_00002_00003`) = archive malformée (le guide garantit un Y unique) → échec + alerte,
    rien n'est poussé — plutôt que de faire silencieusement confiance au premier Y vu."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    zip_name = "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_00001_20260615120000.zip"
    _deposer_zip_multi(
        source,
        zip_name,
        [
            ("17X100A100A0001A_C15_17X000001117366M_GRD-F139_00017_00001_00002.xml", b"un"),
            ("17X100A100A0001A_C15_17X000001117366M_GRD-F139_00017_00002_00003.xml", b"deux"),
        ],
    )
    _configurer_env(monkeypatch, source, cible, db)

    info, stats = executer(_pipeline(tmp_path, db))

    assert (stats.candidats, stats.pousses, stats.echecs_push) == (1, 0, 1)
    assert dict(_toutes_lignes_journal(db))[zip_name] == "echec"


@pytest.mark.integration
def test_r151_echappe_au_controle_intra_zip(tmp_path, monkeypatch):
    """R151 : le compteur est INTER-zips (CONTEXT.md) — un contenu interne « incomplet »
    au sens du compteur X/Y n'est PAS bloqué, contrairement aux autres flux."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    zip_name = "ERDF_R151_17X000001117366M_GRD-F139_108529521_00794_Q_00001_00002_20260615120000.zip"
    _deposer_zip_multi(
        source,
        zip_name,
        [("r151_interne_00001_00002.xml", b"releve")],  # 1/2 au sens du compteur — ignoré pour R151
    )
    _configurer_env(monkeypatch, source, cible, db)

    info, stats = executer(_pipeline(tmp_path, db))

    assert (stats.candidats, stats.pousses, stats.echecs_push) == (1, 1, 0)
    assert _zips_journalises(db) == [zip_name]


@pytest.mark.integration
def test_f15_sans_fichier_donnees_generales_bloque(tmp_path, monkeypatch):
    """F15 : aucun fichier au suffixe `_FA` (données générales, guide SGE 0298) → échec +
    alerte, rien n'est poussé."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    zip_name = "17X100A100A0001A_F15_17X000001117366M_GRD-F139_0321_C_M_1_P_00001_20260615120000.zip"
    _deposer_zip_multi(
        source,
        zip_name,
        [("f15_detail_00001_00001.xml", b"detail")],
    )
    _configurer_env(monkeypatch, source, cible, db)

    info, stats = executer(_pipeline(tmp_path, db))

    assert (stats.candidats, stats.pousses, stats.echecs_push) == (1, 0, 1)
    assert dict(_toutes_lignes_journal(db))[zip_name] == "echec"


@pytest.mark.integration
def test_f15_avec_fichier_donnees_generales_pousse_normalement(tmp_path, monkeypatch):
    """F15 : le fichier de données générales `_FA` est présent en plus des fichiers de
    détail numérotés (forme réelle du corpus EDN : `…_<seq>_FA.xml` + `…_FL_XXXXX_YYYYY.xml`)
    → push normal."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    zip_name = "17X100A100A0001A_F15_17X000001117366M_GRD-F139_0321_C_M_1_P_00001_20260615120000.zip"
    _deposer_zip_multi(
        source,
        zip_name,
        [
            ("17X100A100A0001A_F15_17X000001117366M_GRD-F139_0321_C_M_1_P_00001_FL_00001_00001.xml", b"detail"),
            ("17X100A100A0001A_F15_17X000001117366M_GRD-F139_0321_C_M_1_P_00001_FA.xml", b"generalites"),
        ],
    )
    _configurer_env(monkeypatch, source, cible, db)

    info, stats = executer(_pipeline(tmp_path, db))

    assert (stats.candidats, stats.pousses, stats.echecs_push) == (1, 1, 0)
    assert _zips_journalises(db) == [zip_name]


# =============================================================================
# Vue d'audit embarquée (#646) : bout-en-bout file:// → journal → SELECT sur la vue
# =============================================================================


@pytest.mark.integration
def test_bout_en_bout_journal_puis_vue_audit_couvre_troncature_et_intra_zip(tmp_path, monkeypatch):
    """Critère d'acceptation bout-en-bout : source locale (file://) → dépôt local → journal
    → `SELECT` sur `journal.relais_audit_sequences` — `executer()` enchaîne dlt (push +
    journal enrichi) puis le dbt build embarqué dans le MÊME appel, la vue est donc
    directement requêtable en sortie. Couvre les DEUX cas de durcissement dans le même
    passage : un dépôt tronqué (vérification d'écriture) et un zip intra-zip incomplet —
    tous deux journalisés `statut='echec'` et VISIBLES dans l'audit de réception (ils
    prouvent que Enedis a bien émis ces numéros de séquence, cf. `zip_en_echec_compte_dans_l_audit_de_reception`)."""
    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    zip_sain = "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_00001_20260615120000.zip"
    zip_tronque = "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_00002_20260616120000.zip"
    zip_incomplet = "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_00003_20260617120000.zip"
    _deposer_zip(source, zip_sain, b"<data>un</data>", date=(2026, 6, 15, 12, 0, 0))
    _deposer_zip(source, zip_tronque, b"<data>deux</data>", date=(2026, 6, 16, 12, 0, 0))
    _deposer_zip_multi(
        source,
        zip_incomplet,
        [("17X100A100A0001A_C15_17X000001117366M_GRD-F139_00017_00001_00002.xml", b"un_sur_deux")],
        date=(2026, 6, 17, 12, 0, 0),
    )
    _configurer_env(monkeypatch, source, cible, db)

    verif_originale = executer.__globals__["_verifier_ecriture"]

    def _verif_selective(fs, chemin, taille_locale):
        if zip_tronque.replace(".zip", "") in chemin:
            raise OSError("tronqué")
        return verif_originale(fs, chemin, taille_locale)

    monkeypatch.setitem(executer.__globals__, "_verifier_ecriture", _verif_selective)

    info, stats = executer(_pipeline(tmp_path, db))

    assert (stats.candidats, stats.pousses, stats.echecs_push) == (3, 1, 2)
    statuts = dict(_toutes_lignes_journal(db))
    assert statuts[zip_sain] == "pousse"
    assert statuts[zip_tronque] == "echec"
    assert statuts[zip_incomplet] == "echec"

    con = duckdb.connect(str(db))
    try:
        lignes = con.execute(
            "select flux, cle_sequence, type_anomalie, seq_ou_plage from journal.relais_audit_sequences"
        ).fetchall()
    finally:
        con.close()
    # Les 3 zips comptent dans l'audit de réception (même les 2 en échec de push) : la
    # clé C15|GRD-F139|0327 va jusqu'à 00003 sans trou (aucun numéro manquant entre 1 et 3).
    cles_c15 = [ligne for ligne in lignes if ligne[0] == "C15"]
    assert cles_c15, "la vue doit contenir des lignes pour la clé de séquence C15"
    assert not any(ligne[2] == "trou" for ligne in cles_c15)
    queue = [ligne for ligne in cles_c15 if ligne[2] == "queue_inverifiable"]
    assert len(queue) == 1
    assert queue[0][3] == "00003"


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

    assert (cible / "C15" / "ENEDIS_C15_20260615_001.xml").exists()


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


@pytest.mark.integration
def test_cli_seed_imprime_le_compte_de_zips_amorces(tmp_path, monkeypatch, capsys):
    """#684 : la sortie CLI du seed affiche le chiffre qui compte — combien de zips ont été
    marqués livrés — pas seulement le verbiage dlt (load packages, chemins duckdb)."""
    from electricore.ingestion.relais.__main__ import main

    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260101_001.zip", b"<data>un</data>", date=(2026, 1, 1, 12, 0, 0))
    _deposer_zip(source, "ENEDIS_C15_20260102_002.zip", b"<data>deux</data>", date=(2026, 1, 2, 12, 0, 0))
    _configurer_env(monkeypatch, source, cible, db)
    monkeypatch.setattr(sys, "argv", ["relais", "seed", "--avant", "2026-06-01"])

    main()  # ne lève pas

    sortie = capsys.readouterr().out
    assert "2 zip(s) marqués livrés (antérieurs à 2026-06-01)" in sortie


# =============================================================================
# CLI (__main__.py, #690) : sous-commande completude — remplace le python -c du README
# =============================================================================


@pytest.mark.integration
def test_cli_completude_par_defaut_restreint_aux_flux_dus(tmp_path, monkeypatch, capsys):
    """Défaut : l'écart est restreint aux flux dus au partenaire (`RELAIS__FLUX`, arbitrage
    revue #688) — le R151 (jamais dû ici) n'apparaît pas, même s'il n'a jamais été relayé."""
    from electricore.ingestion.relais.__main__ import main

    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _deposer_zip(source, "ENEDIS_R151_20260615_002.zip", b"<data>r151</data>")
    _configurer_env(monkeypatch, source, cible, db, flux="C15")  # seul C15 est dû au partenaire
    monkeypatch.setattr(sys, "argv", ["relais", "completude"])

    main()  # ne lève pas (exit 0)

    sortie = capsys.readouterr().out
    lignes = sortie.splitlines()
    assert "ENEDIS_C15_20260615_001.zip" in lignes
    assert "ENEDIS_R151_20260615_002.zip" not in lignes
    assert "1 zip(s) manquant(s)" in sortie


@pytest.mark.integration
def test_cli_completude_tous_donne_l_ecart_brut(tmp_path, monkeypatch, capsys):
    """`--tous` : l'écart brut, tous flux confondus (comportement historique de
    `zips_non_relayes`) — le R151 hors liste apparaît désormais."""
    from electricore.ingestion.relais.__main__ import main

    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _deposer_zip(source, "ENEDIS_R151_20260615_002.zip", b"<data>r151</data>")
    _configurer_env(monkeypatch, source, cible, db, flux="C15")
    monkeypatch.setattr(sys, "argv", ["relais", "completude", "--tous"])

    main()  # ne lève pas (exit 0)

    sortie = capsys.readouterr().out
    lignes = sortie.splitlines()
    assert "ENEDIS_C15_20260615_001.zip" in lignes
    assert "ENEDIS_R151_20260615_002.zip" in lignes
    assert "2 zip(s) manquant(s)" in sortie


@pytest.mark.integration
def test_cli_completude_exit_0_meme_avec_des_manquants(tmp_path, monkeypatch, capsys):
    """Consultation passive (#690) : sortie 0 même quand il manque des zips — l'escalade du
    relais reste `relais_aveugle()` (le run normal), jamais cette sous-commande."""
    from electricore.ingestion.relais.__main__ import main

    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db)
    monkeypatch.setattr(sys, "argv", ["relais", "completude"])

    main()  # ne lève PAS SystemExit malgré le manquant — exit 0 implicite

    assert "1 zip(s) manquant(s)" in capsys.readouterr().out


@pytest.mark.integration
def test_cli_completude_sans_manquant_liste_vide_et_compte_zero(tmp_path, monkeypatch, capsys):
    """Rien à signaler → aucune ligne de zip, seule la ligne de compte (0)."""
    from electricore.ingestion.relais.__main__ import main

    source, cible, db = tmp_path / "source", tmp_path / "cible", tmp_path / "relais.duckdb"
    _deposer_zip(source, "ENEDIS_C15_20260615_001.zip", b"<data>c15</data>")
    _configurer_env(monkeypatch, source, cible, db)
    executer(_pipeline(tmp_path, db))  # relaie le seul zip présent
    capsys.readouterr()  # vide le verbiage dbt du executer() ci-dessus

    monkeypatch.setattr(sys, "argv", ["relais", "completude"])
    main()

    assert capsys.readouterr().out.strip() == "0 zip(s) manquant(s)"
