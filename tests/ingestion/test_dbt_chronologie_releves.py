"""Mart *Chronologie des relevés* (ADR-0041, #376) — projection énergie de la spine.

Assemblée ENTIÈREMENT en dbt : relevés C15 aux événements impactant l'énergie + bornes
FACTURATION appariées aux relevés périodiques (R151/R64) au **grain JOUR** (equi-join,
qui REMPLACE l'asof « nearest 4h » du cœur `_assembler_chronologie`), dédoublonnées par
priorité de source (C15 > R64 > R151) via QUALIFY.

Deux niveaux :
- **structure (CI)** : bâtie depuis les fixtures, grain `(rsc, date_releve, ordre_index)`
  unique, contrat `ChronologieReleves` validé par le loader `chronologie()` ;
- **parité (local, base dev)** : behaviour-diff vs `_assembler_chronologie`. ⚠️ HITL —
  le SEUL changement de comportement de la refonte est l'asof→equi-join. Le test encode
  le diff caractérisé : rien perdu, partie C15 identique, et les écarts périodiques sont
  des swaps R151→R64 (priorité ADR-0028, index identique → neutre en énergie) + les bornes
  FACTURATION héritées du fix month_start de la spine (#380).

Skip si dbt absent (`uv sync --extra dbt`).
"""

import json
import shutil
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import polars as pl
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

from dbt.cli.main import dbtRunner  # noqa: E402

from electricore.core.loaders import c15, chronologie, releves  # noqa: E402
from electricore.core.pipelines.energie import _assembler_chronologie  # noqa: E402
from electricore.core.pipelines.historique import pipeline_historique  # noqa: E402
from electricore.ingestion.parsing.xml import xml_vers_dict  # noqa: E402

RACINE = Path(__file__).parents[2]
PROJET_DBT = RACINE / "electricore" / "ingestion" / "dbt"
FIXTURES = RACINE / "tests" / "fixtures" / "flux"
DEV_DB = RACINE / "electricore" / "ingestion" / "flux_enedis_pipeline.duckdb"
PARIS = ZoneInfo("Europe/Paris")
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


# --- Parité behaviour-diff vs _assembler_chronologie (HITL, ADR-0041 §7) ------------------

_INDEX_COLS = [f"index_{c}_kwh" for c in ("base", "hp", "hc", "hph", "hch", "hpb", "hcb")]


def _cle_proj(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        [
            "ref_situation_contractuelle",
            pl.col("date_releve").dt.convert_time_zone("UTC").alias("dr"),
            "ordre_index",
            "source",
            *_INDEX_COLS,
        ]
    )


@pytest.mark.skipif(not DEV_DB.exists(), reason="base dev absente (behaviour-diff local sur données réelles)")
def test_parite_sur_donnees_reelles(tmp_path):
    """Behaviour-diff réel : rien perdu, C15 identique, écarts = swaps R151→R64 (index
    identique, neutre en énergie) + bornes FACTURATION héritées du fix month_start (#380)."""
    db_path = tmp_path / "flux.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("create schema if not exists flux_enedis")
    con.execute(f"attach '{DEV_DB}' as dev (read_only)")
    for t in ("flux_c15", "flux_r64", "flux_r151"):
        con.execute(f"create table flux_enedis.{t} as select * from dev.flux_enedis.{t}")
    con.execute("detach dev")
    con.execute("checkpoint")
    con.close()
    res = _invoke(
        ["run", "--select", "int_releves__c15", "releves", "spine_contrat", "chronologie_releves"],
        db_path,
        tmp_path,
    )
    assert res.success, f"dbt run a échoué : {res.exception}"
    con = duckdb.connect(str(db_path))
    con.execute("checkpoint")
    con.close()
    clean = tmp_path / "flux_lecture.duckdb"
    shutil.copy(db_path, clean)

    horizon = datetime(2026, 6, 1, tzinfo=PARIS)
    mart = chronologie(clean).collect().filter(pl.col("date_releve") <= horizon)
    hist = pipeline_historique(c15(clean).lazy(), horizon=horizon)
    core = _assembler_chronologie(hist.filter(pl.col("impacte_energie")), releves(clean).lazy()).collect()

    key = ["ref_situation_contractuelle", "dr", "ordre_index"]
    # Drapeaux de présence : distinguer « clé absente » de « clé présente mais source/index
    # null » (borne FACTURATION sans relevé — releve_manquant — qui existe des deux côtés).
    m = _cle_proj(mart).rename({c: f"{c}_m" for c in ["source", *_INDEX_COLS]}).with_columns(present_m=pl.lit(True))
    c = _cle_proj(core).rename({c: f"{c}_c" for c in ["source", *_INDEX_COLS]}).with_columns(present_c=pl.lit(True))
    j = m.join(c, on=key, how="full", coalesce=True)

    only_core = j.filter(pl.col("present_m").is_null())
    only_mart = j.filter(pl.col("present_c").is_null())
    shared = j.filter(pl.col("present_m") & pl.col("present_c"))

    # (1) Rien perdu : aucune borne du cœur absente de la spine.
    assert only_core.height == 0, f"{only_core.height} bornes du cœur perdues par le mart"

    # (2) Swaps de source : tous R151(cœur)→R64(mart) (priorité ADR-0028), JAMAIS l'inverse.
    swaps = shared.filter(pl.col("source_m") != pl.col("source_c"))
    directions = set(swaps.select("source_c", "source_m").unique().rows())
    assert directions <= {("flux_R151", "flux_R64")}, f"swaps inattendus : {directions}"

    # (3) Les swaps sont neutres en énergie : index identique R64 vs R151.
    index_differe = pl.any_horizontal(
        (pl.col(f"{x}_m") != pl.col(f"{x}_c")) & pl.col(f"{x}_m").is_not_null() & pl.col(f"{x}_c").is_not_null()
        for x in _INDEX_COLS
    )
    assert swaps.filter(index_differe).height == 0, "un swap R151→R64 change l'index (impact énergie)"

    # (4) Les extras du mart sont des bornes FACTURATION (ordre_index=false) — héritées du
    #     fix month_start de la spine (#380), pas des relevés C15 perdus.
    assert (~only_mart["ordre_index"]).all(), "extra mart hors borne FACTURATION (ordre_index)"
