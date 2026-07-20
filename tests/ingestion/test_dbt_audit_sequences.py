"""Audit de séquences Enedis (#645, PRD #644) : macro `audit_sequences` (brique commune,
regexp de nomenclature + segments + trous + queue + noms non reconnus) et mart
`audit_sequences` (union des tables brutes + contrôle intra-zip rétrospectif).

Harnais in-process (mécanique, fixtures contrôlées) : lande des documents synthétiques
dans les tables `raw_*`, `_source_zip` porté par des noms d'archive couvrant chaque
classe d'anomalie (glossaire, `electricore/ingestion/CONTEXT.md` « Complétude des flux »),
lance `dbt build --select +audit_sequences`, vérifie les lignes de la vue.

Skip si dbt absent (`uv sync --extra dbt`).
"""

from pathlib import Path

import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

import duckdb  # noqa: E402
from dbt.cli.main import dbtRunner  # noqa: E402

from electricore.ingestion.parsing.xml import xml_vers_dict  # noqa: E402

RACINE = Path(__file__).parents[2]
PROJET_DBT = RACINE / "electricore" / "ingestion" / "dbt"
FIXTURES = RACINE / "tests" / "fixtures" / "flux"


def _invoke(commande: list[str], db_path: Path, tmp_path: Path):
    import os

    ancien = os.environ.get("DBT_DUCKDB_PATH")
    os.environ["DBT_DUCKDB_PATH"] = str(db_path)
    try:
        return dbtRunner().invoke(
            [
                *commande,
                "--project-dir",
                str(PROJET_DBT),
                "--profiles-dir",
                str(PROJET_DBT),
                "--target-path",
                str(tmp_path / "target"),
            ]
        )
    finally:
        if ancien is None:
            os.environ.pop("DBT_DUCKDB_PATH", None)
        else:
            os.environ["DBT_DUCKDB_PATH"] = ancien


# Contenu XML réutilisé pour toutes les lignes saines par défaut : seul `_source_zip`
# (et `file_name`, pour la distinction de clé primaire dlt) varie d'un scénario à l'autre —
# le CONTENU du document n'a aucune incidence sur l'audit de séquences (qui ne lit que le
# nom de zip). Un seul document par flux est pré-parsé, réutilisé partout.
_CONTENUS = {
    "raw_c15": xml_vers_dict((FIXTURES / "c15_avec_releves.xml").read_bytes()),
    "raw_r15": xml_vers_dict((FIXTURES / "r15.xml").read_bytes()),
    "raw_f12": xml_vers_dict((FIXTURES / "f12.xml").read_bytes()),
    "raw_f15": xml_vers_dict((FIXTURES / "f15.xml").read_bytes()),
    "raw_r151": xml_vers_dict((FIXTURES / "r151.xml").read_bytes()),
    "raw_c12": xml_vers_dict((FIXTURES / "c12_xsd.xml").read_bytes()),
    "raw_affaires": xml_vers_dict((FIXTURES / "affaires_X12.xml").read_bytes()),
}

# Une ligne saine par flux requis (audit_sequences union les 7 tables) — sert de socle,
# overridée scénario par scénario par `_construire_audit`.
_ZIPS_SAINS = {
    "raw_c15": "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_00001_20240530050823.zip",
    "raw_r15": "17X100A100A0001A_R15_17X000001117366M_GRD-F139_0321_00001_20140923034411.zip",
    "raw_f12": "ENEDIS_F12_17X000001117366M_GRD-F139_0322_00001_D_30_20240601183412.zip",
    "raw_f15": "17X100A100A0001A_F15_17X000001117366M_GRD-F139_0321_C_M_1_P_00001_20140923034411.zip",
    "raw_r151": "ERDF_R151_17X000001117366M_GRD-F139_108529521_00001_Q_00001_00001_20260608030716.zip",
    "raw_c12": "ENEDIS_C12_17X000001117366M_GRD-F139_00001_20240401091056.zip",
    "raw_affaires": "ENEDIS_X12_17X000001117366M_00001_20240401091056.zip",
}


def _construire_audit(tmp_path, overrides: dict[str, list[dict]]) -> list[dict]:
    """Lande une ligne saine par flux requis (overridée par `overrides`), bâtit
    `+audit_sequences`, retourne les lignes de la vue (liste de dicts)."""
    import dlt

    from electricore.ingestion.raw_landing import lander_documents_bruts

    db_path = tmp_path / "flux.duckdb"
    pipeline = dlt.pipeline(
        pipeline_name="test_audit_sequences",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    for raw, zip_sain in _ZIPS_SAINS.items():
        documents = overrides.get(
            raw,
            [
                {
                    "file_name": f"{raw}_defaut.xml",
                    "modification_date": "2026-01-01T00:00:00",
                    "content": _CONTENUS[raw],
                    "_source_zip": zip_sain,
                }
            ],
        )
        lander_documents_bruts(pipeline, raw, documents)

    res = _invoke(["build", "--select", "+audit_sequences"], db_path, tmp_path)
    assert res.success, f"dbt build +audit_sequences a échoué : {res.exception}"

    con = duckdb.connect(str(db_path))
    cur = con.execute("select * from flux_enedis.audit_sequences")
    cols = [d[0] for d in cur.description]
    lignes = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    con.close()
    return lignes


def _doc(raw: str, file_name: str, zip_name: str) -> dict:
    return {
        "file_name": file_name,
        "modification_date": "2026-01-01T00:00:00",
        "content": _CONTENUS[raw],
        "_source_zip": zip_name,
    }


def test_socle_sans_anomalie(tmp_path):
    """Un landing entièrement sain (une archive par clé, aucun trou) ne produit AUCUNE
    ligne de type 'trou' — seule la queue invérifiable de chaque clé est présente
    (jamais 0 ligne du tout : le dernier numéro observé est toujours signalé)."""
    lignes = _construire_audit(tmp_path, {})
    types = {ligne["type_anomalie"] for ligne in lignes}
    assert "trou" not in types
    assert "nom_non_reconnu" not in types
    assert "intra_zip_incomplet" not in types
    assert "queue_inverifiable" in types  # jamais 0 ligne : la queue est toujours signalée


def test_trou_franc_detecte(tmp_path):
    """Un numéro absent ENTRE deux numéros observés d'une même clé C15 → 'trou'."""
    prefix = "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_{seq}_2024010{j}000000.zip"
    docs = [
        _doc("raw_c15", "c15_1.xml", prefix.format(seq="00001", j=1)),
        _doc("raw_c15", "c15_2.xml", prefix.format(seq="00002", j=2)),
        # 00003 manque
        _doc("raw_c15", "c15_4.xml", prefix.format(seq="00004", j=4)),
    ]
    lignes = _construire_audit(tmp_path, {"raw_c15": docs})
    trous = [ligne for ligne in lignes if ligne["flux"] == "C15" and ligne["type_anomalie"] == "trou"]
    assert len(trous) == 1
    assert trous[0]["seq_ou_plage"] == "00003"
    assert trous[0]["cle_sequence"] == "C15|GRD-F139|0327"


def test_redemarrage_f15_ginko_v2_pas_un_trou(tmp_path):
    """Un redémarrage à 00001 (F15/Ginko v2) — numéro qui repart en arrière
    CHRONOLOGIQUEMENT — n'est PAS un trou."""
    docs = [
        _doc(
            "raw_f15",
            "f15_1.xml",
            "17X100A100A0001A_F15_17X000001117366M_GRD-F139_0321_C_M_1_P_99998_20240101000000.zip",
        ),
        _doc(
            "raw_f15",
            "f15_2.xml",
            "17X100A100A0001A_F15_17X000001117366M_GRD-F139_0321_C_M_1_P_99999_20240102000000.zip",
        ),
        _doc(
            "raw_f15",
            "f15_3.xml",
            "17X100A100A0001A_F15_17X000001117366M_GRD-F139_0321_C_M_1_P_00001_20240201000000.zip",
        ),
    ]
    lignes = _construire_audit(tmp_path, {"raw_f15": docs})
    trous_f15 = [ligne for ligne in lignes if ligne["flux"] == "F15" and ligne["type_anomalie"] == "trou"]
    assert trous_f15 == []
    # La queue invérifiable suit le redémarrage (le numéro le plus RÉCENT, pas le plus grand).
    queue_f15 = [ligne for ligne in lignes if ligne["flux"] == "F15" and ligne["type_anomalie"] == "queue_inverifiable"]
    assert len(queue_f15) == 1
    assert queue_f15[0]["seq_ou_plage"] == "00001"


def test_queue_toujours_signalee_jamais_saine(tmp_path):
    """Le dernier numéro observé d'une clé est TOUJOURS en 'queue_inverifiable' —
    même sur un landing par ailleurs sans aucun trou (jamais déclaré sain)."""
    docs = [
        _doc(
            "raw_c15",
            "c15_1.xml",
            "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_00001_20240101000000.zip",
        ),
        _doc(
            "raw_c15",
            "c15_2.xml",
            "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_00002_20240102000000.zip",
        ),
    ]
    lignes = _construire_audit(tmp_path, {"raw_c15": docs})
    queues = [ligne for ligne in lignes if ligne["flux"] == "C15" and ligne["type_anomalie"] == "queue_inverifiable"]
    assert len(queues) == 1
    assert queues[0]["seq_ou_plage"] == "00002"


def test_nom_de_zip_non_reconnu_jamais_ignore(tmp_path):
    """Un nom de zip hors nomenclature apparaît dans la vue avec le type dédié — jamais
    silencieusement ignoré."""
    docs = [
        _doc("raw_c15", "c15_ok.xml", _ZIPS_SAINS["raw_c15"]),
        _doc("raw_c15", "c15_bizarre.xml", "un_nom_de_zip_totalement_hors_nomenclature.zip"),
    ]
    lignes = _construire_audit(tmp_path, {"raw_c15": docs})
    non_reconnus = [ligne for ligne in lignes if ligne["type_anomalie"] == "nom_non_reconnu"]
    assert len(non_reconnus) == 1
    assert non_reconnus[0]["seq_ou_plage"] == "un_nom_de_zip_totalement_hors_nomenclature.zip"
    assert non_reconnus[0]["cle_sequence"] is None


def test_intra_zip_rang_manquant_detecte(tmp_path):
    """Un zip C15 dont le compteur intra-zip `_XXXXX_YYYYY` annonce 3 fichiers mais dont
    seuls 2 ont atterri en base → 'intra_zip_incomplet' (échec de linéarisation isolé
    que l'escalade d'échec de chaîne tolère)."""
    zip_name = "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_00001_20240101000000.zip"
    docs = [
        _doc("raw_c15", "17X100A100A0001A_C15_17X000001117366M_GRD-F139_00017_00001_00003.xml", zip_name),
        _doc("raw_c15", "17X100A100A0001A_C15_17X000001117366M_GRD-F139_00017_00002_00003.xml", zip_name),
        # rang 00003 manque (annoncé par YYYYY=00003)
    ]
    lignes = _construire_audit(tmp_path, {"raw_c15": docs})
    incomplets = [
        ligne for ligne in lignes if ligne["flux"] == "C15" and ligne["type_anomalie"] == "intra_zip_incomplet"
    ]
    assert len(incomplets) == 1
    assert zip_name in incomplets[0]["seq_ou_plage"]
    assert "2/3" in incomplets[0]["seq_ou_plage"]


def test_intra_zip_complet_pas_d_anomalie(tmp_path):
    """Les 3 rangs 1..3 annoncés sont tous landés → pas d'anomalie intra-zip."""
    zip_name = "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_00001_20240101000000.zip"
    docs = [
        _doc("raw_c15", "17X100A100A0001A_C15_17X000001117366M_GRD-F139_00017_00001_00003.xml", zip_name),
        _doc("raw_c15", "17X100A100A0001A_C15_17X000001117366M_GRD-F139_00017_00002_00003.xml", zip_name),
        _doc("raw_c15", "17X100A100A0001A_C15_17X000001117366M_GRD-F139_00017_00003_00003.xml", zip_name),
    ]
    lignes = _construire_audit(tmp_path, {"raw_c15": docs})
    incomplets = [ligne for ligne in lignes if ligne["type_anomalie"] == "intra_zip_incomplet"]
    assert incomplets == []


def test_r151_inter_zips_rang_manquant_detecte(tmp_path):
    """R151 : le compteur est INTER-zips (un XML par zip). 2 zips annoncés (YYYYY=00002)
    pour la même séquence, un seul atterri → 'intra_zip_incomplet'."""
    docs = [
        _doc(
            "raw_r151",
            "r151_1.xml",
            "ERDF_R151_17X000001117366M_GRD-F139_108529521_00794_Q_00001_00002_20260608030716.zip",
        ),
        # zip XXXXX=00002/00002 jamais atterri
    ]
    lignes = _construire_audit(tmp_path, {"raw_r151": docs})
    incomplets = [
        ligne for ligne in lignes if ligne["flux"] == "R151" and ligne["type_anomalie"] == "intra_zip_incomplet"
    ]
    assert len(incomplets) == 1
    assert "1/2" in incomplets[0]["seq_ou_plage"]


def test_r151_inter_zips_complet_pas_d_anomalie(tmp_path):
    docs = [
        _doc(
            "raw_r151",
            "r151_1.xml",
            "ERDF_R151_17X000001117366M_GRD-F139_108529521_00794_Q_00001_00002_20260608030716.zip",
        ),
        _doc(
            "raw_r151",
            "r151_2.xml",
            "ERDF_R151_17X000001117366M_GRD-F139_108529521_00794_Q_00002_00002_20260608030823.zip",
        ),
    ]
    lignes = _construire_audit(tmp_path, {"raw_r151": docs})
    incomplets = [
        ligne for ligne in lignes if ligne["flux"] == "R151" and ligne["type_anomalie"] == "intra_zip_incomplet"
    ]
    assert incomplets == []


def test_x13_derive_du_nom_de_zip(tmp_path):
    """`raw_affaires` sert X12 ET X13, distingués par le nom de zip (même convention que
    `stg_affaires` sur file_name) : une clé de séquence X13 apparaît sous flux='X13'."""
    docs = [
        _doc("raw_affaires", "x13.xml", "ENEDIS_X13_17X000001117366M_00001_20240401091056.zip"),
    ]
    lignes = _construire_audit(tmp_path, {"raw_affaires": docs})
    cles_x13 = {ligne["cle_sequence"] for ligne in lignes if ligne["flux"] == "X13"}
    assert any(cle is not None and cle.startswith("X13|") for cle in cles_x13)


def test_data_test_severity_warn_zero_anomalie_sur_landing_partiel(tmp_path):
    """Le data test `aucune_anomalie` tourne en `warn` : sur un landing avec un trou franc,
    `dbt build` reste GLOBALEMENT réussi (warning, pas d'échec) — c'est l'invariant du
    critère d'acceptation (le job ne casse jamais dessus)."""
    prefix = "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_{seq}_2024010{j}000000.zip"
    docs = [
        _doc("raw_c15", "c15_1.xml", prefix.format(seq="00001", j=1)),
        _doc("raw_c15", "c15_3.xml", prefix.format(seq="00003", j=3)),
    ]
    # `_construire_audit` asserte déjà `res.success` — un data test `warn` qui ferait
    # échouer le build romprait ce test avant même d'atteindre les assertions ci-dessous.
    lignes = _construire_audit(tmp_path, {"raw_c15": docs})
    assert any(ligne["type_anomalie"] == "trou" for ligne in lignes)
