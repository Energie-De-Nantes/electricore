"""Les loaders core sur une base au format de production dbt (#136).

Les tables `flux_enedis.flux_*` sont désormais typées à la source (TIMESTAMPTZ,
BIGINT, DOUBLE — modèles dbt, types XSD). Les loaders ne doivent plus « rattraper »
les types : en particulier, écraser un TIMESTAMPTZ en TIMESTAMP naïf faisait
interpréter l'heure-mur Paris comme de l'UTC par l'aval (`convert_time_zone`),
décalant les instants d'1-2 h. Ce test charge une base construite par le vrai
chemin de production (landing JSON → dbt build) et vérifie le contrat aval :
dtype Pandera ET instant exact.

Skip si dbt n'est pas installé (`uv sync --extra dbt`).
"""

import json
import os
import subprocess
import sys
from datetime import date, datetime
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
def base_prod_dbt(tmp_path_factory):
    """Base DuckDB au format de production : fixtures landées + dbt build."""
    import dlt

    from electricore.ingestion.parsing.xml import xml_vers_dict
    from electricore.ingestion.raw_landing import lander_documents_bruts

    dossier = tmp_path_factory.mktemp("base_dbt")
    db_path = dossier / "flux.duckdb"
    cas = [
        ("c15_avec_releves.xml", "raw_c15"),
        ("r151.xml", "raw_r151"),
        ("r15.xml", "raw_r15"),
    ]
    for fixture, source in cas:
        contenu = (FIXTURES / fixture).read_bytes()
        document = json.loads(contenu) if fixture.endswith(".json") else xml_vers_dict(contenu)
        pipeline = dlt.pipeline(
            pipeline_name=f"test_loaders_{source}",
            destination=dlt.destinations.duckdb(str(db_path)),
            dataset_name="flux_raw",
        )
        lander_documents_bruts(
            pipeline,
            source,
            [{"file_name": fixture, "modification_date": "2026-01-01T00:00:00", "content": document}],
        )
    # dbt en sous-processus : ses connexions DuckDB ne survivent pas, les loaders
    # peuvent ensuite ouvrir la base en read_only (configs incompatibles sinon).
    env = {**os.environ, "DBT_DUCKDB_PATH": str(db_path)}
    resultat = subprocess.run(
        [
            str(Path(sys.executable).parent / "dbt"),
            "build",
            "--select",
            "+flux_c15",
            "+flux_r151",
            "+flux_r15",
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


def test_c15_charge_des_instants_corrects(base_prod_dbt):
    """L'horodatage C15 reste l'instant vrai de la source (00:01 heure de Paris),
    pas l'heure-mur réinterprétée comme UTC (qui donnait 02:01)."""
    from electricore.core.loaders import c15

    df = c15(database_path=base_prod_dbt).collect()
    assert df.schema["date_evenement"] == pl.Datetime("us", "Europe/Paris")
    (date_evenement,) = df["date_evenement"].to_list()
    # Fixture : <Date_Evenement>2024-10-04T00:01:00+02:00</Date_Evenement>
    assert date_evenement == datetime(2024, 10, 4, 0, 1, tzinfo=PARIS)


def test_c15_charge_le_niveau_ouverture_services(base_prod_dbt):
    """Statut de communication (épique #313) : le loader c15() expose le niveau
    d'ouverture aux services (Utf8, xsd:string {0,1,2}) et sa date de bascule (Date),
    de bout en bout (flux_c15 → loader)."""
    from electricore.core.loaders import c15

    df = c15(database_path=base_prod_dbt).collect()
    assert df.schema["niveau_ouverture_services"] == pl.Utf8
    assert df.schema["date_changement_niveau_ouverture_services"] == pl.Date
    # Fixture c15_avec_releves.xml : <Niveau_Ouverture_Services>2</…>, bascule 2018-04-13.
    (niveau,) = df["niveau_ouverture_services"].to_list()
    assert niveau == "2"
    (date_changement,) = df["date_changement_niveau_ouverture_services"].to_list()
    assert date_changement == date(2018, 4, 13)


def test_r15_charge_des_instants_corrects(base_prod_dbt):
    """Même contrat que C15 : l'horodatage R15 est l'instant vrai de la source."""
    from electricore.core.loaders import r15

    df = r15(database_path=base_prod_dbt).collect()
    assert df.schema["date_releve"] == pl.Datetime("us", "Europe/Paris")
    (date_releve,) = df["date_releve"].to_list()
    # Fixture : <Date_Releve>2024-07-30T00:01:00+02:00</Date_Releve>
    assert date_releve == datetime(2024, 7, 30, 0, 1, tzinfo=PARIS)


def test_r151_sert_la_date_brute_et_les_index_entiers(base_prod_dbt):
    """ADR-0003 amendé (#294) : /flux/r151 sert la date BRUTE (fidèle source, convention
    fin de journée), SANS le +1 jour — l'harmonisation J → J+1 ne vit plus que dans le mart
    `releves`. Index en Int64 (kWh entiers, ADR-0034)."""
    from electricore.core.loaders import r151

    df = r151(database_path=base_prod_dbt).collect()
    (date_releve,) = df["date_releve"].to_list()
    # Fixture : Date_Releve 2024-04-04 (date nue, fin de journée), ancrée minuit Paris — sans +1j.
    assert date_releve == datetime(2024, 4, 4, 0, 0, tzinfo=PARIS)
    assert df.schema["index_hph_kwh"] == pl.Int64
