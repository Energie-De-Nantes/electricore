"""Dédup cross-livraisons de flux_affaires (#275).

Les flux X12/X13 quotidiens sont des snapshots cumulatifs : une affaire qui avance
réapparaît chaque jour avec sa liste de jalons enrichie. flux_affaires déduplique sur
la clé logique (affaire_id, jalon_num) en gardant la livraison la plus récente — calque
de l'identité de relevé (ADR-0028). Ce test lande deux livraisons de la *même* affaire
(noms de fichiers distincts, modification_date croissante) et vérifie qu'un jalon
ré-émis n'apparaît qu'une fois, dans sa version la plus récente.
"""

from pathlib import Path

import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

import duckdb  # noqa: E402
from dbt.cli.main import dbtRunner  # noqa: E402

from electricore.core.models.affaire_jalon import AffaireJalon  # noqa: E402
from electricore.core.models.parite_typage import ecarts_de_typage  # noqa: E402
from electricore.ingestion.parsing.xml import xml_vers_dict  # noqa: E402

RACINE = Path(__file__).parents[2]
PROJET_DBT = RACINE / "electricore" / "ingestion" / "dbt"


def _affaire(statut: str | None, jalons: list[tuple[int, str]]) -> bytes:
    """Document <affaires> minimal d'une affaire AFF1, statut donné, jalons (num, état).

    `statut=None` émet une balise `<statut/>` vide — la forme prod d'une affaire
    fraîchement initiée qu'Enedis n'a pas encore rangée (statut absent → null)."""
    lignes = "".join(
        f"<jalon><num>{n}</num><dateHeure>2024-11-2{n}T09:00:00+01:00</dateHeure>"
        f'<affaireEtat code="{e}"><libelle>{e}</libelle></affaireEtat></jalon>'
        for n, e in jalons
    )
    bloc_statut = "<statut/>" if statut is None else f'<statut code="{statut}"><libelle>{statut}</libelle></statut>'
    return (
        '<affaires><affaire id="AFF1"><donneesGenerales>'
        f"{bloc_statut}"
        f"<jalons>{lignes}</jalons>"
        "<donneesPoint><id>99000000000017</id></donneesPoint><segment>C5</segment>"
        "</donneesGenerales></affaire></affaires>"
    ).encode()


def test_statut_vide_donne_null_sans_casser_le_build(tmp_path, monkeypatch):
    """Une affaire fraîchement initiée, qu'Enedis n'a pas encore rangée, arrive avec un
    `<statut>` vide (→ null). Le build ne doit PAS la rejeter (statut nullable, #296) et
    `statut` sort null. Garde-fou contre un retour du `not_null` sur statut."""
    import dlt

    from electricore.ingestion.raw_landing import lander_documents_bruts

    db_path = tmp_path / "flux.duckdb"
    pipeline = dlt.pipeline(
        pipeline_name="test_affaires_statut_vide",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(
        pipeline,
        "raw_affaires",
        [
            {
                "file_name": "ENEDIS_X12_pending.xml",
                "modification_date": "2024-11-22T09:30:00",
                "content": xml_vers_dict(_affaire(None, [(0, "DMTR")])),
            },
        ],
    )
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(db_path))
    resultat = dbtRunner().invoke(
        [
            "build",
            "--select",
            "+flux_affaires",
            "--project-dir",
            str(PROJET_DBT),
            "--profiles-dir",
            str(PROJET_DBT),
            "--target-path",
            str(tmp_path / "target"),
        ]
    )
    # not_null retiré de statut (#296) : le build passe malgré statut null.
    assert resultat.success, f"dbt build a échoué : {resultat.exception}"

    con = duckdb.connect(str(db_path))
    rows = con.execute("select jalon_num, statut from flux_enedis.flux_affaires").fetchall()
    con.close()
    # L'affaire en attente Enedis est ingérée fidèlement : statut null, jalon 0 conservé.
    assert rows == [(0, None)]


def test_jalon_reemis_dedupe_derniere_livraison_gagne(tmp_path, monkeypatch):
    import dlt

    from electricore.ingestion.raw_landing import lander_documents_bruts

    db_path = tmp_path / "flux.duckdb"
    pipeline = dlt.pipeline(
        pipeline_name="test_affaires_dedup",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    # Jour 1 : affaire en cours, un seul jalon. Jour 2 : même affaire, terminée, jalon 1
    # ré-émis + jalon 2. Fichiers distincts (sinon dédup au landing par file_name).
    lander_documents_bruts(
        pipeline,
        "raw_affaires",
        [
            {
                "file_name": "ENEDIS_X12_jour1.xml",
                "modification_date": "2024-11-22T09:30:00",
                "content": xml_vers_dict(_affaire("COURS", [(1, "DMTR")])),
            },
            {
                "file_name": "ENEDIS_X12_jour2.xml",
                "modification_date": "2024-11-23T09:30:00",
                "content": xml_vers_dict(_affaire("TERMN", [(1, "DMTR"), (2, "CPRE")])),
            },
        ],
    )
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(db_path))
    resultat = dbtRunner().invoke(
        [
            "build",
            "--select",
            "+flux_affaires",
            "--project-dir",
            str(PROJET_DBT),
            "--profiles-dir",
            str(PROJET_DBT),
            "--target-path",
            str(tmp_path / "target"),
        ]
    )
    assert resultat.success, f"dbt build a échoué : {resultat.exception}"

    con = duckdb.connect(str(db_path))
    rows = con.execute("select jalon_num, statut from flux_enedis.flux_affaires order by jalon_num").fetchall()
    con.close()

    # Un jalon ré-émis ne crée pas de doublon : 2 lignes (jalon 1, jalon 2), pas 3.
    assert [r[0] for r in rows] == [1, 2]
    # Le jalon 1 ré-émis porte le statut de la livraison la plus récente (TERMN), pas COURS.
    assert all(r[1] == "TERMN" for r in rows)


def test_dbt_affaires_respecte_le_contrat_pandera(tmp_path, monkeypatch):
    """Parité de typage dbt↔cœur au seam `affaires_ouvertes` ← `flux_affaires` (#295,
    ADR-0035). Le schéma réellement émis par `flux_affaires` doit être type-compatible
    avec le contrat Pandera `AffaireJalon`, via la table de correspondance SQL↔Polars. On
    lit le type **réellement émis par dbt** (`DESCRIBE`), pas la sortie post-cast du loader :
    sinon un re-cast côté loader blanchirait une dérive du modèle dbt (la pièce à conviction
    d'ADR-0034). Nullabilité hors périmètre (axe par couche). Calque de
    `test_releves_dbt_respecte_le_contrat_pandera` — c'est le garde-fou de frontière qui
    manquait sur ce seam (loader `affaires()` portait `validator=None`)."""
    import dlt

    from electricore.ingestion.raw_landing import lander_documents_bruts

    db_path = tmp_path / "flux.duckdb"
    pipeline = dlt.pipeline(
        pipeline_name="test_affaires_parite",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(
        pipeline,
        "raw_affaires",
        [
            {
                "file_name": "ENEDIS_X12_parite.xml",
                "modification_date": "2024-11-22T09:30:00",
                "content": xml_vers_dict(_affaire("COURS", [(0, "DMTR"), (1, "INPL")])),
            },
        ],
    )
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(db_path))
    resultat = dbtRunner().invoke(
        [
            "build",
            "--select",
            "+flux_affaires",
            "--project-dir",
            str(PROJET_DBT),
            "--profiles-dir",
            str(PROJET_DBT),
            "--target-path",
            str(tmp_path / "target"),
        ]
    )
    assert resultat.success, f"dbt build flux_affaires a échoué : {resultat.exception}"

    con = duckdb.connect(str(db_path))
    schema_sql = {nom: type_sql for nom, type_sql, *_ in con.execute("describe flux_enedis.flux_affaires").fetchall()}
    con.close()

    ecarts = ecarts_de_typage(schema_sql, AffaireJalon)
    assert not ecarts, f"divergences de typage dbt↔AffaireJalon (dbt, pandera) : {ecarts}"
