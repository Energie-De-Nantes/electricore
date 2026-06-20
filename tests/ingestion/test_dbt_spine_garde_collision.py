"""Garde-fou ADR-0041 §7 (#374) : test dbt « 0 collision même-instant ».

Sur la *Chronologie du contrat*, le forward-fill de situation
(`last_value(<attr> IGNORE NULLS) OVER (PARTITION BY ref_situation_contractuelle
ORDER BY date_evenement)`) n'est déterministe que si le timestamp `date_evenement`
ordonne les faits d'une même RSC **sans ambiguïté**. Le test dbt singulier
`assert_c15_situation_sans_collision_meme_instant` asserte, pour chaque attribut de
situation, qu'aucun couple d'événements de la même RSC ne porte deux valeurs
non-nulles **différentes** au **même** `date_evenement`.

Ce harnais prouve le garde-fou en deux temps :
- **vert sur données réelles** : `flux_c15` bâti depuis la fixture C15 → 0 collision,
  et le test tourne bien dans `dbt build` (le chemin de prod, cf. `construire_dbt`) ;
- **rouge sur collision** : on injecte une paire colisionnante (même RSC + même
  `date_evenement`, une valeur d'attribut divergente) dans une copie de la base, et le
  garde-fou doit échouer — un attribut à la fois.

Skip si dbt absent (`uv sync --extra dbt`).
"""

import shutil
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

GARDE = "assert_c15_situation_sans_collision_meme_instant"


def _invoke(commande: list[str], db_path: Path, tmp_path: Path):
    """Lance dbt sur la base `db_path` (via DBT_DUCKDB_PATH), target isolé dans `tmp_path`."""
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


def _statut_test(resultat, nom: str) -> str | None:
    """Statut dbt ('pass'/'fail'/…) du nœud `nom` dans un résultat de run, ou None."""
    if resultat.result is None:
        return None
    for r in resultat.result.results:
        if r.node.name == nom:
            return str(r.status)
    return None


@pytest.fixture(scope="module")
def base_c15_propre(tmp_path_factory):
    """`flux_c15` bâti depuis la fixture C15 (données réelles, sans collision)."""
    import dlt

    from electricore.ingestion.raw_landing import lander_documents_bruts

    tmp = tmp_path_factory.mktemp("spine_garde")
    db_path = tmp / "flux.duckdb"
    pipeline = dlt.pipeline(
        pipeline_name="test_spine_garde",
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
                "content": xml_vers_dict((FIXTURES / "c15_avec_releves.xml").read_bytes()),
            }
        ],
    )
    resultat = _invoke(["build", "--select", "+flux_c15"], db_path, tmp)
    assert resultat.success, f"dbt build +flux_c15 a échoué : {resultat.exception}"
    # Fusionner le WAL dans le fichier principal : sans CHECKPOINT, les tables vivent
    # dans `flux.duckdb.wal` et un `shutil.copy` du seul `.duckdb` perd le schéma.
    con = duckdb.connect(str(db_path))
    con.execute("checkpoint")
    con.close()
    return db_path, resultat


def test_garde_tourne_dans_build_et_verte(base_c15_propre):
    """Le garde-fou est sélectionné par `+flux_c15` (forme de la sélection prod,
    cf. `construire_dbt`) et passe sur les données réelles de la fixture (0 collision)."""
    _, build = base_c15_propre
    assert _statut_test(build, GARDE) == "pass", (
        f"le garde-fou n'a pas tourné/passé dans `dbt build --select +flux_c15` (statut={_statut_test(build, GARDE)})"
    )


# Un attribut de situation, la colonne flux_c15 qui le porte, et une expression SQL
# produisant une valeur DIFFÉRENTE de l'existant (collision garantie au même instant).
ATTRIBUTS = [
    ("formule_tarifaire_acheminement", "'COLLISION_' || formule_tarifaire_acheminement"),
    ("puissance_souscrite_kva", "puissance_souscrite_kva + 999"),
    ("niveau_ouverture_services", "'9'"),
    ("segment_clientele", "'COLLISION_' || segment_clientele"),
]


@pytest.mark.parametrize("colonne,valeur_divergente", ATTRIBUTS, ids=[a[0] for a in ATTRIBUTS])
def test_garde_detecte_collision(base_c15_propre, colonne, valeur_divergente, tmp_path):
    """Le garde-fou échoue si un attribut de situation collisionne au même instant."""
    base_propre, _ = base_c15_propre
    db_path = tmp_path / "flux.duckdb"
    shutil.copy(base_propre, db_path)

    # Injecter une ligne jumelle (même RSC + même date_evenement) avec UNE valeur
    # d'attribut divergente → collision sur cet attribut, à cet instant.
    con = duckdb.connect(str(db_path))
    con.execute(f"""
        insert into flux_enedis.flux_c15 by name
        select * replace ({valeur_divergente} as {colonne})
        from flux_enedis.flux_c15
        where {colonne} is not null
          and ref_situation_contractuelle is not null
          and date_evenement is not null
        limit 1
    """)
    con.close()

    resultat = _invoke(["test", "--select", GARDE], db_path, tmp_path)
    assert _statut_test(resultat, GARDE) == "fail", (
        f"le garde-fou n'a pas détecté la collision sur {colonne} "
        f"(statut={_statut_test(resultat, GARDE)}, success={resultat.success})"
    )
