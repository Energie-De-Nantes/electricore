"""Modèle de relevés canonique `releves` (ADR-0029, #241).

Lande R151 (XML) + R64 (JSON) dans une même DuckDB, lance `dbt build --select
+releves` (modèles + data_tests, dont `unique releve_id` = invariant de grain et de
dedup même-source), puis vérifie l'union, l'harmonisation des dates et la présence
des sources.

Skip si dbt absent (`uv sync --extra dbt`).
"""

import json
from pathlib import Path

import duckdb
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

from dbt.cli.main import dbtRunner  # noqa: E402

from electricore.ingestion.parsing.xml import xml_vers_dict  # noqa: E402

RACINE = Path(__file__).parents[2]
PROJET_DBT = RACINE / "electricore" / "ingestion" / "dbt"
FIXTURES = RACINE / "tests" / "fixtures" / "flux"


@pytest.fixture
def base_periodiques(tmp_path, monkeypatch):
    """Lande R151 (XML) + R64 (JSON) en colonnes brutes dans une DuckDB temporaire."""
    import dlt

    from electricore.ingestion.raw_landing import lander_documents_bruts

    db_path = tmp_path / "flux.duckdb"
    pipeline = dlt.pipeline(
        pipeline_name="test_releves",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(
        pipeline,
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
        pipeline,
        "raw_r64",
        [
            {
                "file_name": "r64.json",
                "modification_date": "2026-01-01T00:00:00",
                "content": json.loads((FIXTURES / "r64.json").read_text()),
            }
        ],
    )
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(db_path))
    return db_path


def _build_releves(target_parent):
    return dbtRunner().invoke(
        [
            "build",
            "--select",
            "+releves",
            "--project-dir",
            str(PROJET_DBT),
            "--profiles-dir",
            str(PROJET_DBT),
            "--target-path",
            str(target_parent / "target"),
        ]
    )


def test_releves_union_grain_et_harmonisation(base_periodiques):
    # `dbt build` matérialise releves ET exécute `unique releve_id` : un build vert
    # prouve l'invariant de grain / dedup même-source.
    resultat = _build_releves(base_periodiques.parent)
    assert resultat.success, f"dbt build releves a échoué : {resultat.exception}"

    con = duckdb.connect(str(base_periodiques))

    # Grain : 1 ligne par releve_id (dedup même-source).
    n, n_uniq = con.execute("select count(*), count(distinct releve_id) from flux_enedis.releves").fetchone()
    assert n == n_uniq > 0, "grain : releve_id doit être unique dans releves"

    # Les deux sources périodiques sont présentes et étiquetées.
    sources = {r[0] for r in con.execute("select distinct source from flux_enedis.releves").fetchall()}
    assert sources == {"flux_R151", "flux_R64"}

    # Harmonisation R151 : date_releve = date brute (flux_r151) + 1 jour (ADR-0003).
    bad = con.execute(
        """
        select count(*)
        from flux_enedis.releves r
        join flux_enedis.flux_r151 f on r.releve_id = f.releve_id
        where date_trunc('day', r.date_releve at time zone 'Europe/Paris')
              <> f.date_releve + interval '1 day'
        """
    ).fetchone()[0]
    assert bad == 0, "R151 : date_releve doit être harmonisée J → J+1"

    con.close()
