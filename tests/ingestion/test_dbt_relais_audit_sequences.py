"""Étage Transform du relais (#646, PRD #644) : mini-projet dbt embarqué qui matérialise
`journal.relais_audit_sequences` sur le journal du relais, en réutilisant SANS AUCUNE règle
dupliquée la macro `audit_sequences` du projet dbt principal (#645) — importée en PACKAGE
LOCAL (`electricore/ingestion/relais/dbt/packages.yml` -> `local: ../../dbt`).

Harnais in-process (mécanique, fixtures contrôlées) : peuple le journal via un run dlt
minimal (même forme que `pipeline.py`), lance `dbt deps` puis `dbt build`, vérifie les
lignes de la vue. Miroir de `tests/ingestion/test_dbt_audit_sequences.py` (harnais côté
ingestion) adapté au journal du relais (une seule table, tous flux mélangés, flux dérivé
du nom de zip).

Skip si dbt absent (`uv sync --extra dbt`).
"""

from pathlib import Path

import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

import dlt  # noqa: E402
import duckdb  # noqa: E402
from dbt.cli.main import dbtRunner  # noqa: E402

RACINE = Path(__file__).parents[2]
PROJET_DBT = RACINE / "electricore" / "ingestion" / "relais" / "dbt"


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
                # --target-path : pas une option de `dbt deps`, seulement build/run.
                *(["--target-path", str(tmp_path / "target")] if commande[0] != "deps" else []),
            ]
        )
    finally:
        if ancien is None:
            os.environ.pop("DBT_DUCKDB_PATH", None)
        else:
            os.environ["DBT_DUCKDB_PATH"] = ancien


def _peupler_journal(db_path: Path, lignes: list[dict]) -> None:
    """Écrit dans `journal.relais_livraisons` via dlt — même table/forme que `pipeline.py`."""
    pipeline = dlt.pipeline(
        pipeline_name="test_dbt_relais_audit_sequences",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="journal",
    )
    pipeline.run(lignes, table_name="relais_livraisons", write_disposition="append")


def _construire_audit(tmp_path, lignes: list[dict]) -> list[dict]:
    db_path = tmp_path / "relais.duckdb"
    _peupler_journal(db_path, lignes)

    deps = _invoke(["deps"], db_path, tmp_path)
    assert deps.success, f"dbt deps a échoué : {deps.exception}"

    # --select restreint au package RACINE (electricore_relais) : `dbt build` sans sélecteur
    # construirait AUSSI les modèles du package local importé (electricore_flux, dont son
    # PROPRE mart `audit_sequences` du projet principal) — collision de nom + tests/data
    # tests qui ne concernent pas le relais (vue passive, #646).
    build = _invoke(["build", "--select", "package:electricore_relais"], db_path, tmp_path)
    assert build.success, f"dbt build a échoué : {build.exception}"

    con = duckdb.connect(str(db_path))
    cur = con.execute("select * from journal.relais_audit_sequences")
    cols = [d[0] for d in cur.description]
    resultat = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    con.close()
    return resultat


def _ligne(zip_name: str, statut: str = "pousse") -> dict:
    return {"zip": zip_name, "fichiers": [], "statut": statut, "at": "2026-06-01T00:00:00Z"}


def test_socle_sans_anomalie(tmp_path):
    """Un journal sain (une archive par clé, aucun trou) ne produit aucune ligne 'trou' —
    seule la queue invérifiable est présente."""
    lignes = _construire_audit(
        tmp_path,
        [_ligne("17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_00001_20260615120000.zip")],
    )
    types = {ligne["type_anomalie"] for ligne in lignes}
    assert "trou" not in types
    assert "queue_inverifiable" in types


def test_trou_franc_detecte(tmp_path):
    """Un numéro absent entre deux numéros observés d'une même clé C15 → 'trou' — la macro
    du projet principal fait tout le travail, aucune règle recopiée ici."""
    prefix = "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_{seq}_2026010{j}000000.zip"
    lignes = _construire_audit(
        tmp_path,
        [
            _ligne(prefix.format(seq="00001", j=1)),
            _ligne(prefix.format(seq="00002", j=2)),
            # 00003 manque
            _ligne(prefix.format(seq="00004", j=4)),
        ],
    )
    trous = [ligne for ligne in lignes if ligne["flux"] == "C15" and ligne["type_anomalie"] == "trou"]
    assert len(trous) == 1
    assert trous[0]["seq_ou_plage"] == "00003"


def test_zip_en_echec_compte_dans_l_audit_de_reception(tmp_path):
    """Un zip journalisé `statut='echec'` (push raté) compte quand même dans l'audit de
    réception — sa présence prouve qu'Enedis a bien émis ce numéro, l'exclure créerait un
    faux trou (#646 : distinct de `zips_non_relayes`, qui lui filtre sur pousse/amorce)."""
    prefix = "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_{seq}_2026010{j}000000.zip"
    lignes = _construire_audit(
        tmp_path,
        [
            _ligne(prefix.format(seq="00001", j=1), statut="pousse"),
            _ligne(prefix.format(seq="00002", j=2), statut="echec"),  # push raté, mais REÇU
        ],
    )
    trous = [ligne for ligne in lignes if ligne["flux"] == "C15" and ligne["type_anomalie"] == "trou"]
    assert trous == []  # pas de trou : le 00002 a bien été vu, même en échec de push


def test_flux_derive_du_nom_de_zip(tmp_path):
    """Le journal n'a pas de colonne `flux` — dérivé du nom de zip (2e segment `_`) : un
    zip R151 apparaît sous flux='R151' dans la vue."""
    lignes = _construire_audit(
        tmp_path,
        [_ligne("ERDF_R151_17X000001117366M_GRD-F139_108529521_00794_Q_00001_00002_20260615120000.zip")],
    )
    assert any(ligne["flux"] == "R151" for ligne in lignes)


def test_vue_passive_aucun_data_test_aucune_anomalie(tmp_path):
    """Vue passive (#646) : `dbt build` reste vert même sur un journal avec un trou franc —
    aucun data test `aucune_anomalie` n'est câblé côté relais (contrairement à l'ingestion),
    la supervision reste `StatsRelais`/exit code."""
    prefix = "17X100A100A0001A_C15_17X000001117366M_GRD-F139_0327_{seq}_2026010{j}000000.zip"
    lignes = _construire_audit(
        tmp_path,
        [
            _ligne(prefix.format(seq="00001", j=1)),
            _ligne(prefix.format(seq="00003", j=3)),
        ],
    )  # `_construire_audit` asserte déjà `build.success` — un test bloquant romprait ici
    assert any(ligne["type_anomalie"] == "trou" for ligne in lignes)
