"""Parité golden du modèle dbt flux_r64 (ADR-0020, issue #123).

Harnais réutilisable : on landé la fixture R64 anonymisée en colonne JSON
(comme dlt en production), on lance dbt, et on compare la table matérialisée
`flux_r64` aux records golden — modulo traçabilité (`_*`, `modification_date`),
ajoutée hors linéarisation.

Skip si dbt n'est pas installé (`uv sync --extra dbt`).
"""

import json
from pathlib import Path

import duckdb
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

from dbt.cli.main import dbtRunner  # noqa: E402

RACINE = Path(__file__).parents[2]
PROJET_DBT = RACINE / "electricore" / "etl" / "dbt"
FIXTURE = RACINE / "tests" / "fixtures" / "flux" / "r64.json"
GOLDEN = RACINE / "tests" / "fixtures" / "flux" / "golden" / "flux_r64.json"

# Colonnes de traçabilité : ajoutées hors linéarisation, hors périmètre parité.
TRACABILITE = {"_source_zip", "_flux_type", "_json_name", "modification_date"}


def _sans_tracabilite(record: dict) -> dict:
    return {k: v for k, v in record.items() if k not in TRACABILITE}


@pytest.fixture
def base_avec_brut(tmp_path, monkeypatch):
    """Landé la fixture R64 en colonne JSON dans une DuckDB temporaire (fork α)."""
    import dlt

    from electricore.etl.raw_landing import lander_documents_bruts

    db_path = tmp_path / "flux.duckdb"
    document = json.loads(FIXTURE.read_text())
    pipeline = dlt.pipeline(
        pipeline_name="test_raw_r64",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(
        pipeline,
        "raw_r64",
        [{"file_name": "r64.json", "modification_date": "2026-01-01T00:00:00", "content": document}],
    )
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(db_path))
    return db_path


def test_flux_r64_reproduit_le_golden(base_avec_brut):
    # `dbt build` matérialise les modèles ET exécute les data_tests (not_null) :
    # un build vert prouve à la fois la parité golden et les contrats dbt.
    resultat = dbtRunner().invoke(
        [
            "build",
            "--select",
            "+flux_r64",
            "--project-dir",
            str(PROJET_DBT),
            "--profiles-dir",
            str(PROJET_DBT),
            "--target-path",
            str(base_avec_brut.parent / "target"),
        ]
    )
    # `success` n'est vrai que si tout passe : 2 modèles (stg_r64, flux_r64)
    # + 2 data_tests not_null.
    assert resultat.success, f"dbt build a échoué : {resultat.exception}"
    assert len(resultat.result) == 4

    # Connexion en écriture (même config que dbt dans le process) pour éviter
    # le conflit read_only ; dbt matérialise dans le schéma `flux_enedis`.
    con = duckdb.connect(str(base_avec_brut))
    cur = con.execute("select * from flux_enedis.flux_r64")
    cols = [d[0] for d in cur.description]
    obtenu = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    con.close()

    golden = json.loads(GOLDEN.read_text())
    assert len(obtenu) == len(golden)

    par_pdl = {r["pdl"]: r for r in obtenu}
    for attendu in golden:
        ligne = par_pdl[attendu["pdl"]]
        ref = _sans_tracabilite(attendu)
        for k, v in ref.items():
            obt = ligne[k]
            # date_releve : golden en str, dbt en datetime → comparaison sur la valeur.
            assert str(obt) == str(v), f"pdl {attendu['pdl']} colonne {k}: dbt={obt!r} golden={v!r}"
