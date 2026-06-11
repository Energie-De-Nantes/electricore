"""Parité golden du modèle dbt flux_c15 (ADR-0020, issue #124).

C15 est le flux le plus dur : `nested_fields` conditionnels (avant/après via
Code_Qualification) + pivot des cadrans. On convertit le XML en dict générique
(`xml_vers_dict`), on le landé en colonne JSON (comme dlt en production), on lance
dbt, et on compare la table `flux_c15` au golden — modulo traçabilité **et fuseau**.

Le golden a été produit par l'ancien parseur (tout en `str`, offsets `+02:00`
préservés). Le modèle dbt type les horodatages en instant-correct (TIMESTAMPTZ) :
la parité compare donc l'**instant**, pas la représentation string (ADR-0020,
politique timezone tranchée en #124).

Skip si dbt n'est pas installé (`uv sync --extra dbt`).
"""

import json
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

from dbt.cli.main import dbtRunner  # noqa: E402

from electricore.etl.parsing.xml import xml_vers_dict  # noqa: E402

RACINE = Path(__file__).parents[2]
PROJET_DBT = RACINE / "electricore" / "etl" / "dbt"
FIXTURE = RACINE / "tests" / "fixtures" / "flux" / "c15_avec_releves.xml"
GOLDEN = RACINE / "tests" / "fixtures" / "flux" / "golden" / "flux_c15.json"

# Colonnes de traçabilité : ajoutées hors linéarisation, hors périmètre parité.
TRACABILITE = {"_source_zip", "_flux_type", "_xml_name", "modification_date"}

# Horodatages typés en instant par dbt : comparés sur l'instant, pas la string.
COLONNES_INSTANT = {"date_evenement", "avant_date_releve", "apres_date_releve"}


@pytest.fixture
def base_avec_brut(tmp_path, monkeypatch):
    """Convertit + landé la fixture C15 en colonne JSON dans une DuckDB temporaire."""
    import dlt

    from electricore.etl.raw_landing import lander_documents_bruts

    db_path = tmp_path / "flux.duckdb"
    document = xml_vers_dict(FIXTURE.read_bytes())
    pipeline = dlt.pipeline(
        pipeline_name="test_raw_c15",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(
        pipeline,
        "raw_c15",
        [
            {
                "file_name": "c15_avec_releves.xml",
                "modification_date": "2026-01-01T00:00:00",
                "content": document,
            }
        ],
    )
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(db_path))
    return db_path


def test_flux_c15_reproduit_le_golden(base_avec_brut):
    resultat = dbtRunner().invoke(
        [
            "build",
            "--select",
            "+flux_c15",
            "--project-dir",
            str(PROJET_DBT),
            "--profiles-dir",
            str(PROJET_DBT),
            "--target-path",
            str(base_avec_brut.parent / "target"),
        ]
    )
    # `build` vert = 2 modèles (stg_c15, flux_c15) + 2 data_tests (not_null) passent.
    assert resultat.success, f"dbt build a échoué : {resultat.exception}"
    assert len(resultat.result) == 4

    con = duckdb.connect(str(base_avec_brut))
    cur = con.execute("select * from main.flux_c15")
    cols = [d[0] for d in cur.description]
    obtenu = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    con.close()

    golden = json.loads(GOLDEN.read_text())
    assert len(obtenu) == len(golden)

    par_pdl = {r["pdl"]: r for r in obtenu}
    for attendu in golden:
        ligne = par_pdl[attendu["pdl"]]
        for k, v in attendu.items():
            if k in TRACABILITE:
                continue
            obt = ligne[k]
            if k in COLONNES_INSTANT:
                # Instant-correct : parse les deux côtés en datetime aware et compare.
                assert _instant(obt) == _instant(v), f"pdl {attendu['pdl']} {k}: {obt!r} != {v!r}"
            else:
                # str() absorbe les types (bigint/double/date) : "6.0" == 6.0, "2531" == 2531.
                assert str(obt) == str(v), f"pdl {attendu['pdl']} colonne {k}: dbt={obt!r} golden={v!r}"


def _instant(valeur) -> datetime:
    """Normalise un horodatage (datetime aware ou string ISO) en instant comparable."""
    if isinstance(valeur, datetime):
        return valeur
    return datetime.fromisoformat(str(valeur))
