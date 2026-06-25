"""*Chronologie des relevés* (ADR-0041, #376 ; normalisée ADR-0045, #431).

Assemblée ENTIÈREMENT en dbt : relevés C15 aux événements impactant l'énergie + bornes
FACTURATION appariées aux relevés périodiques (R151/R64) au **grain JOUR** (equi-join,
qui REMPLACE l'asof « nearest 4h » de l'ex-assemblage cœur), dédoublonnées par priorité
de source (C15 > R64 > R151) via QUALIFY.

Depuis ADR-0045 (#431), le modèle est **normalisé** : le mart MINCE
`chronologie_releves_situation` (matérialisé) porte l'identité + la situation + `releve_id`
(référence) + les attributs de slot ; la **vue** `chronologie_releves` ré-attache le payload
d'index par **star-join** sur `releves` (source de vérité unique, ADR-0029/0038). Le loader
`chronologie()` lit la vue ; le contrat `ChronologieReleves` est inchangé.

Test de **structure (CI)** : bâtie depuis les fixtures, grain `(rsc, date_releve,
ordre_index)` unique, contrat `ChronologieReleves` validé par le loader `chronologie()`,
+ mart mince (sans payload), vue (star-join), parité du payload vs `releves[releve_id]`.

Skip si dbt absent (`uv sync --extra dbt`).
"""

import json
import shutil
from pathlib import Path

import duckdb
import polars as pl
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

from dbt.cli.main import dbtRunner  # noqa: E402

from electricore.core.loaders import chronologie, releves  # noqa: E402
from electricore.core.models.cadrans import CADRANS, col_index  # noqa: E402
from electricore.ingestion.parsing.xml import xml_vers_dict  # noqa: E402

# Payload d'index pur (fonction 1:1 de `releve_id`, ADR-0045) : retiré du mart mince
# `chronologie_releves_situation`, ré-attaché par la vue `chronologie_releves` depuis
# `releves` (source de vérité unique, ADR-0029/0038).
PAYLOAD_INDEX = ("nature_index", "id_calendrier_distributeur", *(col_index(c) for c in CADRANS))

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
    # Copie lisible (évite le lock writer) — en PRÉSERVANT le nom de fichier : depuis ADR-0045
    # `chronologie_releves` est une VUE dont le corps SQL référence le catalogue de build
    # (= le stem du fichier, ici `flux`, comme toute vue dbt-duckdb). Renommer la copie
    # (`flux_lecture.duckdb` → catalogue `flux_lecture`) casserait la résolution de la vue. En
    # prod, le loader rouvre le MÊME fichier que dbt a bâti → catalogue identique, pas de souci.
    copie_dir = tmp / "lecture"
    copie_dir.mkdir()
    copie = copie_dir / db_path.name
    shutil.copy(db_path, copie)
    return copie


def _colonnes(db_path: Path, relation: str) -> set[str]:
    """Noms de colonnes d'une relation `flux_enedis.<relation>` (sans la matérialiser)."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        desc = con.execute(f"select * from flux_enedis.{relation} limit 0").description
        return {d[0] for d in desc}
    finally:
        con.close()


def test_chronologie_grain_et_contrat(base_fixtures):
    """Le loader valide `ChronologieReleves` ; grain unique ; sources dans l'énum."""
    df = chronologie(base_fixtures).collect()  # lève si le contrat Pandera échoue
    assert df.height > 0
    cle = ["ref_situation_contractuelle", "date_releve", "ordre_index"]
    assert df.select(cle).n_unique() == df.height  # grain 1 ligne / (rsc, date, ordre)
    assert df["ref_situation_contractuelle"].null_count() == 0
    sources = set(df["source"].drop_nulls().unique().to_list())
    assert sources <= {"flux_C15", "flux_R64", "flux_R151"}


def test_mart_situation_est_mince(base_fixtures):
    """Le mart `chronologie_releves_situation` ne recopie PLUS le payload d'index (ADR-0045) :
    il porte l'identité + la situation + `releve_id` (référence) + les attributs de slot."""
    cols = _colonnes(base_fixtures, "chronologie_releves_situation")
    encore_la = set(PAYLOAD_INDEX) & cols
    assert not encore_la, f"payload d'index encore inliné dans le mart mince : {encore_la}"
    attendu = {
        "pdl",
        "ref_situation_contractuelle",
        "formule_tarifaire_acheminement",
        "niveau_ouverture_services",
        "date_releve",
        "ordre_index",
        "source",
        "releve_id",
        "releve_manquant",
    }
    assert attendu <= cols, f"colonnes attendues manquantes : {attendu - cols}"


def test_chronologie_releves_est_une_vue(base_fixtures):
    """`chronologie_releves` est désormais une VUE (star-join), plus un mart matérialisé."""
    con = duckdb.connect(str(base_fixtures), read_only=True)
    try:
        row = con.execute(
            "select table_type from information_schema.tables "
            "where table_schema = 'flux_enedis' and table_name = 'chronologie_releves'"
        ).fetchone()
    finally:
        con.close()
    assert row is not None, "la relation `chronologie_releves` est absente"
    assert row[0] == "VIEW", f"`chronologie_releves` devrait être une vue, pas {row[0]}"


def test_index_proviennent_de_releves(base_fixtures):
    """`releves` est la source de vérité unique du payload d'index (ADR-0029/0038/0045).

    La vue ré-attache `index_*` / `nature_index` / `id_calendrier_distributeur` par star-join
    sur `releve_id` : pour toute ligne référençant un relevé, les valeurs sont CELLES de
    `releves[releve_id]` ; pour une borne sans relevé apparié (`releve_manquant`), elles sont
    nulles (left join). C'est la parité qui garantit que la sortie énergie est inchangée.
    """
    chrono = chronologie(base_fixtures).collect()
    payload = [c for c in PAYLOAD_INDEX]

    # La vue porte bien le payload ré-attaché (contrat ChronologieReleves inchangé).
    assert set(payload) <= set(chrono.columns)

    # Parité : payload de la vue == payload de `releves` pour le même `releve_id`.
    source = releves(base_fixtures).collect().select(["releve_id", *payload])
    apparie = (
        chrono.filter(pl.col("releve_id").is_not_null())
        .select(["releve_id", *payload])
        .join(source, on="releve_id", how="left", suffix="_src")
    )
    assert apparie.height > 0, "aucune ligne appariée — fixture inattendue"
    for c in payload:
        identiques = apparie.select(pl.col(c).eq_missing(pl.col(f"{c}_src")).all()).item()
        assert identiques, f"payload `{c}` diverge de releves[releve_id]"

    # Les bornes sans relevé apparié héritent d'un payload NUL (comme aujourd'hui).
    manquants = chrono.filter(pl.col("releve_manquant"))
    for c in payload:
        assert manquants[c].null_count() == manquants.height, f"`{c}` non-nul sur releve_manquant"
