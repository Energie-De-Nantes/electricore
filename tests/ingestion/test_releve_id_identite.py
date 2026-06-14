"""Identité de relevé : la clé métier `releve_id` est stable sur re-livraison (#232, ADR-0028).

`releve_id` est une **clé métier déterministe** dérivée de `(pdl, date_releve, source,
discriminant)`, mintée au seam dbt pour r151/r15/r64. Elle ne dépend ni de l'id
d'occurrence fichier (`fichier#position`), ni de `modification_date` : ré-ingérer le
même document, ou recevoir le même relevé logique R64 dans une fenêtre chevauchante
plus récente, **ne change pas** le `releve_id`.

Ce test landé le même corpus deux fois (livraisons distinctes, dates de modification
distinctes), reconstruit via le chemin dbt de production, et compare les `releve_id`.

Skip si dbt n'est pas installé (`uv sync --extra dbt`).
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


def _construire_base(tmp_path, livraisons: list[tuple[str, str, str, str]]):
    """Landé une suite de livraisons (fixture, source, file_name, modification_date) puis dbt build.

    Renvoie le chemin DuckDB. Plusieurs livraisons de la même fixture simulent une
    re-ingestion / une fenêtre R64 chevauchante (la plus récente gagne le dédoublonnage).
    """
    import dlt

    from electricore.ingestion.raw_landing import lander_documents_bruts

    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "flux.duckdb"
    modeles: set[str] = set()
    for fixture, source, file_name, modif in livraisons:
        contenu = (FIXTURES / fixture).read_bytes()
        document = json.loads(contenu) if fixture.endswith(".json") else xml_vers_dict(contenu)
        pipeline = dlt.pipeline(
            pipeline_name=f"test_id_{source}",
            destination=dlt.destinations.duckdb(str(db_path)),
            dataset_name="flux_raw",
        )
        lander_documents_bruts(
            pipeline,
            source,
            [{"file_name": file_name, "modification_date": modif, "content": document}],
        )
        modeles.add("flux_" + source.removeprefix("raw_"))

    select = []
    for m in sorted(modeles):
        select += ["--select", f"+{m}"]
    import os

    env_path = str(db_path)
    os.environ["DBT_DUCKDB_PATH"] = env_path
    resultat = dbtRunner().invoke(
        [
            "build",
            *select,
            "--project-dir",
            str(PROJET_DBT),
            "--profiles-dir",
            str(PROJET_DBT),
            "--target-path",
            str(tmp_path / "target"),
        ]
    )
    assert resultat.success, f"dbt build a échoué : {resultat.exception}"
    return db_path


def _releve_ids(db_path, model: str) -> list[str]:
    con = duckdb.connect(str(db_path))
    rows = con.execute(f"select releve_id from flux_enedis.{model} order by releve_id").fetchall()
    con.close()
    return [r[0] for r in rows]


def test_releve_id_r64_stable_sur_fenetre_chevauchante(tmp_path):
    """Même relevé R64 livré deux fois (modif date différente) → même `releve_id`.

    Les fenêtres R64 se chevauchent : la linéarisation garde la livraison la plus
    récente (qualify row_number sur modification_date). L'id d'occurrence fichier
    gagnant bouge, mais le `releve_id` métier (pdl, date, source, type_releve) non.
    """
    base = _construire_base(
        tmp_path,
        [
            ("r64.json", "raw_r64", "r64_livraison1.json", "2026-01-01T00:00:00"),
            ("r64.json", "raw_r64", "r64_livraison2.json", "2026-02-01T00:00:00"),
        ],
    )
    ids = _releve_ids(base, "flux_r64")
    # Dédoublonnage : 1 relevé logique par (pdl, type, date) malgré 2 livraisons.
    assert len(ids) == len(set(ids)), "releve_id doit être unique par relevé logique"
    # Stabilité : reconstruit avec une seule livraison, les mêmes ids ressortent.
    base_solo = _construire_base(
        tmp_path / "solo",
        [("r64.json", "raw_r64", "r64_solo.json", "2025-12-01T00:00:00")],
    )
    assert _releve_ids(base_solo, "flux_r64") == ids


def test_releve_id_r151_deterministe(tmp_path):
    """R151 (sans id natif) : `releve_id` minté, déterministe et unique par relevé logique."""
    base = _construire_base(
        tmp_path,
        [("r151.xml", "raw_r151", "r151.xml", "2026-01-01T00:00:00")],
    )
    ids = _releve_ids(base, "flux_r151")
    assert all(isinstance(i, str) and i for i in ids)
    assert len(ids) == len(set(ids))


def _colonne(db_path, model: str, colonne: str) -> list:
    con = duckdb.connect(str(db_path))
    rows = con.execute(f"select {colonne} from flux_enedis.{model}").fetchall()
    con.close()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Nature canonique par source (ADR-0028) : réel / estimé / corrigé
# ---------------------------------------------------------------------------


def test_nature_canonique_r151_reel(tmp_path):
    """R151 (télérelevé périodique, pas de Nature_Index) → nature_index 'réel'."""
    base = _construire_base(tmp_path, [("r151.xml", "raw_r151", "r151.xml", "2026-01-01T00:00:00")])
    assert set(_colonne(base, "flux_r151", "nature_index")) == {"réel"}


def test_nature_canonique_r15_mappe_nature_index(tmp_path):
    """R15 : REEL → réel (fixture réelle) et id_releve = Id_Releve natif (provenance)."""
    base = _construire_base(tmp_path, [("r15.xml", "raw_r15", "r15.xml", "2026-01-01T00:00:00")])
    assert set(_colonne(base, "flux_r15", "nature_index")) == {"réel"}
    # Id_Releve natif présent (provenance), distinct du releve_id métier.
    assert all(v is not None for v in _colonne(base, "flux_r15", "id_releve"))


def test_nature_canonique_r15_estime_xsd(tmp_path):
    """Fixture XSD R15 (Nature_Index cyclée REEL/ESTIME) : ESTIME → estimé exposé."""
    base = _construire_base(tmp_path, [("r15_xsd.xml", "raw_r15", "r15_xsd.xml", "2026-01-01T00:00:00")])
    natures = set(_colonne(base, "flux_r15", "nature_index"))
    assert natures <= {"réel", "estimé"}
    assert "estimé" in natures


def test_nature_canonique_r64_etape_metier(tmp_path):
    """R64 : etapeMetier BRUT → réel (la fixture réelle est BRUT)."""
    base = _construire_base(tmp_path, [("r64.json", "raw_r64", "r64.json", "2026-01-01T00:00:00")])
    assert set(_colonne(base, "flux_r64", "nature_index")) == {"réel"}


def test_nature_canonique_c15_mappe_avant_apres(tmp_path):
    """C15 : avant/apres_nature_index projetés en nature canonique (REEL → réel)."""
    base = _construire_base(tmp_path, [("c15_avec_releves.xml", "raw_c15", "c15.xml", "2026-01-01T00:00:00")])
    con = duckdb.connect(str(base))
    rows = con.execute("select avant_nature_index, apres_nature_index from flux_enedis.flux_c15").fetchall()
    con.close()
    valeurs = {v for ligne in rows for v in ligne if v is not None}
    assert valeurs <= {"réel", "estimé", "corrigé"}
