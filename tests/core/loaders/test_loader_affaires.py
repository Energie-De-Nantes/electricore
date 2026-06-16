"""Loader affaires() sur une base au format de production dbt (#276).

Bout-en-bout du suivi opérationnel : fixture X12 landée → dbt build flux_affaires →
loader affaires() → rollup affaires_ouvertes. Vérifie que l'extraction d'attributs XML
(id, codes) traverse jusqu'au DataFrame, que jalon_date_heure reste un instant tz-aware
(pas réinterprété), et que la vue read-time roule correctement sur des types réels.

Skip si dbt n'est pas installé (`uv sync --extra dbt`).
"""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

RACINE = Path(__file__).parents[3]
PROJET_DBT = RACINE / "electricore" / "ingestion" / "dbt"
FIXTURES = RACINE / "tests" / "fixtures" / "flux"
PARIS = ZoneInfo("Europe/Paris")


@pytest.fixture(scope="module")
def base_affaires(tmp_path_factory):
    """Base DuckDB de prod : fixture X12 landée dans raw_affaires + dbt build."""
    import dlt

    from electricore.ingestion.parsing.xml import xml_vers_dict
    from electricore.ingestion.raw_landing import lander_documents_bruts

    dossier = tmp_path_factory.mktemp("base_affaires")
    db_path = dossier / "flux.duckdb"
    document = xml_vers_dict((FIXTURES / "affaires_X12.xml").read_bytes())
    pipeline = dlt.pipeline(
        pipeline_name="test_loader_affaires",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(
        pipeline,
        "raw_affaires",
        [{"file_name": "affaires_X12.xml", "modification_date": "2026-01-01T00:00:00", "content": document}],
    )
    env = {**os.environ, "DBT_DUCKDB_PATH": str(db_path)}
    resultat = subprocess.run(
        [
            str(Path(sys.executable).parent / "dbt"),
            "build",
            "--select",
            "+flux_affaires",
            "--project-dir",
            str(PROJET_DBT),
            "--profiles-dir",
            str(PROJET_DBT),
            "--target-path",
            str(dossier / "target"),
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert resultat.returncode == 0, f"dbt build a échoué :\n{resultat.stdout}\n{resultat.stderr}"
    return db_path


def test_affaires_charge_attributs_et_instant_tzaware(base_affaires):
    """Les codes portés en attributs XML traversent jusqu'au DataFrame, et
    jalon_date_heure reste un instant tz-aware (pas d'heure-mur réinterprétée)."""
    from electricore.core.loaders import affaires

    df = affaires(database_path=base_affaires).collect()

    assert df.schema["jalon_date_heure"] == pl.Datetime("us", "Europe/Paris")
    assert df.height == 8  # 4 + 1 + 2 + 1 jalons des 4 affaires de la fixture
    assert df["origine"].unique().to_list() == ["initiee"]
    # L'état CPNR (porté par l'attribut affaireEtat/@code) est bien remonté.
    assert "CPNR" in df["affaire_etat"].to_list()
    # statut COURS porté par un attribut SEUL (<statut code="COURS"/>) survit jusqu'au DataFrame.
    assert "COURS" in df["statut"].to_list()


def test_affaires_ouvertes_bout_en_bout(base_affaires):
    """DB → loader → rollup : l'affaire CFN en cours (statut COURS porté par attribut
    seul) ressort — AME écartée par défaut — avec son dernier état."""
    from electricore.core.loaders import affaires
    from electricore.core.pipelines.affaires import affaires_ouvertes

    df = affaires(database_path=base_affaires).collect()
    vue = affaires_ouvertes(df, maintenant=datetime(2024, 11, 25, 9, tzinfo=PARIS))

    # AFF0000004 = CFN COURS (statut attribut-seul) ; AFF0000002 = AME COURS, exclue par défaut.
    assert vue["affaire_id"].to_list() == ["AFF0000004"]
    assert vue["dernier_etat"].to_list() == ["DMTR"]
    assert vue["prestation"].to_list() == ["CFN"]
