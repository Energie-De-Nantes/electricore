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

import duckdb
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
        ("r64.json", "raw_r64"),
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
            "+flux_r64",
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


def test_c15_porte_unite_et_precision_kwh(base_prod_dbt):
    """Les métadonnées unite/precision="kWh" du C15 (jadis ajoutées en Polars par
    `transform_add_defaults`, désormais littéraux SQL du descripteur, #390) restent
    présentes en sortie du loader — contrat aval inchangé."""
    from electricore.core.loaders import c15

    df = c15(database_path=base_prod_dbt).collect()
    assert df.schema["unite"] == pl.Utf8
    assert df.schema["precision"] == pl.Utf8
    assert df["unite"].unique().to_list() == ["kWh"]
    assert df["precision"].unique().to_list() == ["kWh"]


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


def test_r64_charge_le_flux_brut(base_prod_dbt):
    """Régression #333 : le chemin loader R64 BRUT (`/flux/r64`) se chargeait en Binder
    Error parce que `SCHEMA_R64` déclarait `modification_date` (+ `_source_zip`/`_flux_type`/
    `_json_name`), colonnes que le mart `flux_r64` ne projette pas depuis la refonte
    `releves` (#241/#304/#285). Le mart `releves` n'était pas affecté — d'où une régression
    invisible jusqu'en prod. Ce test exerce le loader brut, pas seulement le mart."""
    from electricore.core.loaders import r64

    df = r64(database_path=base_prod_dbt).collect()
    assert df.height > 0
    assert "modification_date" not in df.columns
    assert {"pdl", "date_releve", "index_base_kwh", "source"} <= set(df.columns)


def _lignes_sous_session_tz(query, db_path, tz):
    """Exécute le SQL bâti par le loader sous un fuseau de SESSION DuckDB donné.

    Mime la différence prod (VPS en UTC) vs dev (poste en Europe/Paris) : le `WHERE`
    s'exécute sur l'instant brut, le littéral est interprété dans le fuseau de session.
    """
    sql, params = query._build_final_query()
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.execute(f"SET TimeZone='{tz}'")
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def test_filtre_offset_est_deterministe_quel_que_soit_le_fuseau_de_session(base_prod_dbt):
    """#391 : un filtre de date sur une colonne à offset (TIMESTAMPTZ) doit renvoyer les
    MÊMES lignes sous `TZ=UTC` (VPS) et `TZ=Europe/Paris` (dev). Le `WHERE` tournant sur
    l'instant brut AVANT le cast, le littéral doit être interprété en heure légale française
    (mirror du cast), pas dans le fuseau de session."""
    from electricore.core.loaders import c15

    # Fixture : un événement à 2024-10-04T00:01:00+02:00 (= 2024-10-03 22:01 UTC), juste
    # APRÈS minuit Paris du 4 mais AVANT minuit UTC : c'est la ligne-bord qui distingue
    # les deux fuseaux pour la borne `>= '2024-10-04'`.
    q = c15(database_path=base_prod_dbt).filter({"date_evenement": ">= '2024-10-04'"})
    assert _lignes_sous_session_tz(q, base_prod_dbt, "UTC") == _lignes_sous_session_tz(q, base_prod_dbt, "Europe/Paris")


def test_filtre_offset_respecte_la_semantique_europe_paris(base_prod_dbt):
    """#391 (correct-by-construction) : la borne respecte la sémantique Europe/Paris.
    L'événement à 00:01 Paris le 2024-10-04 est INCLUS par sa propre borne de minuit Paris
    (`>= '2024-10-04'`) et EXCLU dès le lendemain (`>= '2024-10-05'`)."""
    from electricore.core.loaders import c15

    inclus = c15(database_path=base_prod_dbt).filter({"date_evenement": ">= '2024-10-04'"}).collect()
    assert inclus.height == 1
    exclus = c15(database_path=base_prod_dbt).filter({"date_evenement": ">= '2024-10-05'"}).collect()
    assert exclus.height == 0


def test_filtre_colonne_naive_ou_date_inchange_et_deterministe(base_prod_dbt):
    """#391 : les colonnes naïves (R64) et DATE (R151) sont déjà déterministes (naïf↔naïf,
    date↔date) — leur `WHERE` n'enveloppe PAS le littéral (placeholder nu) et reste
    identique quel que soit le fuseau de session ; aucun `SET TimeZone` global n'est émis."""
    from electricore.core.loaders import r64, r151

    for query in (
        r64(database_path=base_prod_dbt).filter({"date_releve": ">= '2022-01-01'"}),
        r151(database_path=base_prod_dbt).filter({"date_releve": ">= '2024-01-01'"}),
    ):
        sql, _ = query._build_final_query()
        clause_where = sql.split("WHERE")[-1]
        assert "timezone(" not in clause_where  # le WHERE n'ancre pas les colonnes non-offset
        assert "set timezone" not in sql.lower()  # pas de réglage global du fuseau
        assert _lignes_sous_session_tz(query, base_prod_dbt, "UTC") == _lignes_sous_session_tz(
            query, base_prod_dbt, "Europe/Paris"
        )


def _colonnes_tz_aware_projetees(desc, db_path) -> dict:
    """{nom: forme} des colonnes tz-aware (TIMESTAMPTZ) de la table que le descripteur projette."""
    schema_name, table_name = desc.table.split(".")
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        tz_aware = {
            nom
            for (nom,) in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ? AND data_type LIKE '%WITH TIME ZONE%'",
                [schema_name, table_name],
            ).fetchall()
        }
    finally:
        con.close()
    projetees = {c.name: c.forme for c in desc.columns}
    return {nom: projetees[nom] for nom in tz_aware & projetees.keys()}


@pytest.mark.parametrize("flux", ["c15", "r151", "r15", "r64"])
def test_garde_inventaire_colonnes_tz_aware_sont_offset(base_prod_dbt, flux):
    """#391 (garde d'inventaire) : toute colonne tz-aware (TIMESTAMPTZ) qu'un descripteur
    legacy projette doit être déclarée OFFSET — sinon son filtre dériverait selon le fuseau
    de session (cf. test de déterminisme) et son cast de lecture serait incohérent. Une
    future colonne tz-aware ajoutée à la projection sans forme OFFSET échoue donc ICI
    (vacuité assumée pour les flux sans colonne tz-aware ; cf. la garde-de-la-garde)."""
    from electricore.core.loaders.duckdb.registry import FLUX_DESCRIPTORS
    from electricore.core.loaders.duckdb.sql import FormeTemporelle

    for nom, forme in _colonnes_tz_aware_projetees(FLUX_DESCRIPTORS[flux], base_prod_dbt).items():
        assert forme is FormeTemporelle.OFFSET, (
            f"{flux}.{nom} est tz-aware (TIMESTAMPTZ) mais n'est pas déclarée OFFSET : "
            f"son filtre dériverait selon le fuseau de session (#391)."
        )


def test_la_garde_inventaire_n_est_pas_vacante(base_prod_dbt):
    """Garde-de-la-garde : C15 expose bel et bien des colonnes tz-aware (date_evenement,
    avant/apres_date_releve) — la garde d'inventaire mord donc sur du réel, elle n'est pas
    vacuement verte."""
    from electricore.core.loaders.duckdb.registry import FLUX_DESCRIPTORS

    assert _colonnes_tz_aware_projetees(FLUX_DESCRIPTORS["c15"], base_prod_dbt)


@pytest.mark.parametrize("flux", ["c15", "r151", "r15", "r64"])
def test_aucune_derive_loader_mart(base_prod_dbt, flux):
    """Garde anti-dérive loader↔mart (#333) : chaque colonne déclarée par un `FluxDescriptor`
    doit exister dans le mart correspondant. On exécute le SELECT du loader avec `LIMIT 0` —
    DuckDB lie alors toutes les colonnes référencées sans matérialiser de ligne ; une colonne
    déclarée mais absente du mart lève un Binder Error. Une future dérive (comme #333) échoue
    donc ICI, en test, plutôt qu'en prod sur l'endpoint."""
    from electricore.core.loaders.duckdb.registry import FLUX_DESCRIPTORS
    from electricore.core.loaders.duckdb.sql import build_base_query

    sql = build_base_query(FLUX_DESCRIPTORS[flux])
    con = duckdb.connect(str(base_prod_dbt), read_only=True)
    try:
        con.execute(f"SELECT * FROM ({sql}) LIMIT 0")  # Binder Error si dérive
    except duckdb.BinderException as exc:  # pragma: no cover - chemin d'échec explicite
        pytest.fail(f"Dérive loader↔mart sur {flux} : {str(exc).splitlines()[0]}")
    finally:
        con.close()
