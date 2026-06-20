"""Mart *Chronologie des relevés* (ADR-0041, #376) — projection énergie de la spine.

Assemblée ENTIÈREMENT en dbt : relevés C15 aux événements impactant l'énergie + bornes
FACTURATION appariées aux relevés périodiques (R151/R64) au **grain JOUR** (equi-join,
qui REMPLACE l'asof « nearest 4h » de l'ex-assemblage cœur), dédoublonnées par priorité
de source (C15 > R64 > R151) via QUALIFY.

Test de **structure (CI)** : bâtie depuis les fixtures, grain `(rsc, date_releve,
ordre_index)` unique, contrat `ChronologieReleves` validé par le loader `chronologie()`.
La parité behaviour-diff vs le cœur était le garde-fou HITL de #376 (validée à la revue
sur base réelle) ; #377 retire l'assemblage cœur, il n'y a plus de baseline à comparer.

Skip si dbt absent (`uv sync --extra dbt`).
"""

import json
import shutil
from pathlib import Path

import duckdb
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

from dbt.cli.main import dbtRunner  # noqa: E402

from electricore.core.loaders import chronologie  # noqa: E402
from electricore.ingestion.parsing.xml import xml_vers_dict  # noqa: E402

RACINE = Path(__file__).parents[2]
PROJET_DBT = RACINE / "electricore" / "ingestion" / "dbt"
FIXTURES = RACINE / "tests" / "fixtures" / "flux"
BORNE = "2026-07-01"


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
                "--vars",
                f"{{borne_facturation_genereuse: '{BORNE}'}}",
            ]
        )
    finally:
        if ancien is None:
            os.environ.pop("DBT_DUCKDB_PATH", None)
        else:
            os.environ["DBT_DUCKDB_PATH"] = ancien


@pytest.fixture(scope="module")
def base_fixtures(tmp_path_factory):
    """Lande C15 + R151 + R64, bâtit `+chronologie_releves`, retourne une copie lisible."""
    import dlt

    from electricore.ingestion.raw_landing import lander_documents_bruts

    tmp = tmp_path_factory.mktemp("chrono")
    db_path = tmp / "flux.duckdb"
    pipe = dlt.pipeline(
        pipeline_name="test_chronologie",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(
        pipe,
        "raw_c15",
        [
            {
                "file_name": "c15_avec_releves.xml",
                "modification_date": "2026-01-01T00:00:00",
                "content": xml_vers_dict((FIXTURES / "c15_avec_releves.xml").read_bytes()),
            }
        ],
    )
    lander_documents_bruts(
        pipe,
        "raw_r151",
        [
            {
                "file_name": "r151.xml",
                "modification_date": "2026-01-01T00:00:00",
                "content": xml_vers_dict((FIXTURES / "r151.xml").read_bytes()),
            }
        ],
    )
    lander_documents_bruts(
        pipe,
        "raw_r64",
        [
            {
                "file_name": "r64.json",
                "modification_date": "2026-01-01T00:00:00",
                "content": json.loads((FIXTURES / "r64.json").read_text()),
            }
        ],
    )
    res = _invoke(["build", "--select", "+chronologie_releves"], db_path, tmp)
    assert res.success, f"dbt build +chronologie_releves a échoué : {res.exception}"
    con = duckdb.connect(str(db_path))
    con.execute("checkpoint")
    con.close()
    copie = tmp / "flux_lecture.duckdb"
    shutil.copy(db_path, copie)
    return copie


def test_chronologie_grain_et_contrat(base_fixtures):
    """Le loader valide `ChronologieReleves` ; grain unique ; sources dans l'énum."""
    df = chronologie(base_fixtures).collect()  # lève si le contrat Pandera échoue
    assert df.height > 0
    cle = ["ref_situation_contractuelle", "date_releve", "ordre_index"]
    assert df.select(cle).n_unique() == df.height  # grain 1 ligne / (rsc, date, ordre)
    assert df["ref_situation_contractuelle"].null_count() == 0
    sources = set(df["source"].drop_nulls().unique().to_list())
    assert sources <= {"flux_C15", "flux_R64", "flux_R151"}


# La parité behaviour-diff vs `_assembler_chronologie` (cœur) était le garde-fou HITL
# de #376 (validée à la revue, base réelle). #377 retire `_assembler_chronologie` du cœur :
# il n'existe plus de baseline à comparer — le mart EST désormais la Chronologie des relevés.
