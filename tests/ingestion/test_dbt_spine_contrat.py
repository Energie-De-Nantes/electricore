"""Mart *spine* de la Chronologie du contrat (ADR-0041, #375).

La spine assemble en dbt — Class-Table Inheritance — l'épine commune
`(pdl, ref_situation_contractuelle, date_evenement, source, type_fait)` des faits d'une
RSC : événements C15 ∪ grille FACTURATION (1ᵉʳ de chaque mois), avec les attributs de
**situation** forward-fillés **en SQL** sur la timeline d'événements complète
(`last_value(<attr> IGNORE NULLS) OVER (PARTITION BY rsc ORDER BY date_evenement)`).

Ce harnais (mécanique, sur fixtures contrôlées) vérifie :
- la projection des événements C15 sur l'épine (source/type_fait) ;
- la génération de la grille FACTURATION (1ᵉʳ du mois, entrée+1mois → résiliation) ;
- l'héritage de situation par forward-fill (FACTURATION ← dernier événement) ;
- la correction du bug `month_start` de bord de mois (ADR-0041) ;
- le contrat Pandera de la spine via le loader `spine()`.

La parité vs l'ancien `pipeline_historique` (qui ré-assemblait depuis C15) a été prouvée à
l'introduction de la spine (#375) ; depuis #378 `pipeline_historique` **consomme** la spine
(la comparaison serait circulaire), donc le harnais valide la spine sur ses propres goldens.
Skip si dbt absent (`uv sync --extra dbt`).
"""

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import polars as pl
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

from dbt.cli.main import dbtRunner  # noqa: E402

from electricore.core.loaders import spine  # noqa: E402
from electricore.core.loaders.duckdb.registry import DESCRIPTOR_C15  # noqa: E402
from electricore.ingestion.parsing.xml import xml_vers_dict  # noqa: E402

RACINE = Path(__file__).parents[2]
PROJET_DBT = RACINE / "electricore" / "ingestion" / "dbt"
FIXTURES = RACINE / "tests" / "fixtures" / "flux"
PARIS = ZoneInfo("Europe/Paris")

# Borne généreuse fixe pour des tests déterministes (≥ tout horizon des fixtures 2024).
BORNE = "2026-06-01"


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
                "--vars",
                f"{{borne_facturation_genereuse: '{BORNE}'}}",
            ]
        )
    finally:
        if ancien is None:
            os.environ.pop("DBT_DUCKDB_PATH", None)
        else:
            os.environ["DBT_DUCKDB_PATH"] = ancien


def _construire_spine(tmp, documents_c15: list[dict]) -> Path:
    """Lande des documents C15 (parsés), bâtit `+spine_contrat`, retourne une **copie**.

    dbt (adapter in-process) garde une connexion read-write ouverte sur la base bâtie ;
    le loader `spine()` ouvre en read-only → conflit de configuration DuckDB. On rend donc
    une copie sans connexion ouverte (checkpoint d'abord pour fusionner le WAL).
    """
    import shutil

    import dlt

    from electricore.ingestion.raw_landing import lander_documents_bruts

    db_path = tmp / "flux.duckdb"
    pipeline = dlt.pipeline(
        pipeline_name="test_spine_contrat",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(pipeline, "raw_c15", documents_c15)
    res = _invoke(["build", "--select", "+spine_contrat"], db_path, tmp)
    assert res.success, f"dbt build +spine_contrat a échoué : {res.exception}"
    con = duckdb.connect(str(db_path))
    con.execute("checkpoint")
    con.close()
    copie = tmp / "flux_lecture.duckdb"
    shutil.copy(db_path, copie)
    return copie


# Colonnes RÉELLES de flux_c15 (sql_expr == name ⟹ vraie colonne table, pas un littéral
# comme `'flux_C15' as source`). Dérivées de DESCRIPTOR_C15 (source unique) pour bâtir un
# flux_c15 synthétique COMPLET : le mart spine ET le loader c15()/pipeline_historique le
# lisent (parité). Type heuristique aligné sur le contrat XSD/dbt.
_COLS_C15_REELLES = tuple(c.name for c in DESCRIPTOR_C15.columns if c.sql_expr == c.name)


def _type_c15(nom: str) -> str:
    if nom in ("date_evenement", "avant_date_releve", "apres_date_releve"):
        return "timestamptz"
    if nom.endswith("_kwh"):
        return "bigint"
    if nom == "puissance_souscrite_kva":
        return "double"
    if nom == "date_changement_niveau_ouverture_services":
        return "date"
    return "varchar"


def _spine_depuis_evenements(tmp, evenements: list[dict]) -> Path:
    """Bâtit un `flux_c15` synthétique COMPLET depuis des dicts d'événements, puis `spine_contrat`.

    Contourne le XML : crée la table `flux_c15` avec TOUTES ses colonnes (SCHEMA_C15), insère
    les événements (seuls épine+situation renseignés, le reste null ; date_evenement en
    wall-clock Paris, déterministe quel que soit le fuseau de session), puis
    `dbt run --select spine_contrat` (une seule invocation dbt → pas de conflit de connexion).
    Retourne une copie sans connexion ouverte (le mart ET c15() s'y lisent en read-only).
    """
    import shutil

    db_path = tmp / "flux.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("create schema if not exists flux_enedis")
    cols_ddl = ", ".join(f"{n} {_type_c15(n)}" for n in _COLS_C15_REELLES)
    con.execute(f"create table flux_enedis.flux_c15 ({cols_ddl})")
    for e in evenements:
        noms = list(e.keys())
        valeurs = []
        for n in noms:
            v = e[n]
            if n == "date_evenement":
                valeurs.append(f"(timestamp '{v}' at time zone 'Europe/Paris')")
            elif v is None:
                valeurs.append("null")
            elif _type_c15(n) == "double":
                valeurs.append(str(v))
            else:
                valeurs.append(f"'{v}'")
        con.execute(f"insert into flux_enedis.flux_c15 ({', '.join(noms)}) values ({', '.join(valeurs)})")
    con.execute("checkpoint")
    con.close()

    res = _invoke(["run", "--select", "spine_contrat"], db_path, tmp)
    assert res.success, f"dbt run spine_contrat a échoué : {res.exception}"
    con = duckdb.connect(str(db_path))
    con.execute("checkpoint")
    con.close()
    copie = tmp / "flux_lecture.duckdb"
    shutil.copy(db_path, copie)
    return copie


def _evt(date: str, evt: str, rsc: str = "R1", **kw) -> dict:
    """Un événement C15 synthétique minimal (situation par défaut surchargée par kw)."""
    base = {
        "date_evenement": date,
        "pdl": "PDL000000000001",
        "ref_situation_contractuelle": rsc,
        "evenement_declencheur": evt,
        "type_evenement": "contractuel",
        "segment_clientele": "C5",
        "etat_contractuel": "EN SERVICE",
        "puissance_souscrite_kva": 6.0,
        "formule_tarifaire_acheminement": "BTINFCU4",
        "type_compteur": "CCB",
        "num_compteur": "C1",
        "niveau_ouverture_services": "2",
    }
    base.update(kw)
    return base


@pytest.fixture(scope="module")
def base_un_evenement(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("spine_un_evt")
    return _construire_spine(
        tmp,
        [
            {
                "file_name": "c15_avec_releves.xml",
                "modification_date": "2026-01-01T00:00:00",
                "content": xml_vers_dict((FIXTURES / "c15_avec_releves.xml").read_bytes()),
            }
        ],
    )


def test_spine_projette_evenement_c15_sur_epine(base_un_evenement):
    """Un événement C15 devient une ligne de spine, épine renseignée (source/type_fait)."""
    df = spine(base_un_evenement).collect()
    evt = df.filter(pl.col("type_fait") == "evenement")
    assert evt.height == 1
    ligne = evt.row(0, named=True)
    assert ligne["pdl"] == "99660599682036"
    assert ligne["ref_situation_contractuelle"] == "248912973"
    assert ligne["source"] == "flux_C15"
    assert ligne["date_evenement"] == datetime(2024, 10, 4, 0, 1, tzinfo=PARIS)


def test_grille_facturation_premier_de_chaque_mois(tmp_path):
    """FACTURATION au 1ᵉʳ de chaque mois, de l'entrée+1mois à la résiliation (incluse)."""
    db = _spine_depuis_evenements(
        tmp_path,
        [_evt("2024-01-15 00:01:00", "MES"), _evt("2024-04-20 00:01:00", "RES")],
    )
    df = spine(db).collect()
    fact = df.filter((pl.col("ref_situation_contractuelle") == "R1") & (pl.col("type_fait") == "facturation"))
    dates = sorted(fact["date_evenement"].to_list())
    assert dates == [
        datetime(2024, 2, 1, tzinfo=PARIS),
        datetime(2024, 3, 1, tzinfo=PARIS),
        datetime(2024, 4, 1, tzinfo=PARIS),
    ]
    assert (fact["source"] == "synthese_mensuelle").all()
    assert (fact["evenement_declencheur"] == "FACTURATION").all()


def test_situation_forward_fillee_sur_facturation(tmp_path):
    """Une FACTURATION hérite la situation du dernier événement qui la PRÉCÈDE (timestamp)."""
    db = _spine_depuis_evenements(
        tmp_path,
        [
            _evt("2024-01-15 00:01:00", "MES", puissance_souscrite_kva=6.0),
            _evt("2024-03-10 00:01:00", "MCT", puissance_souscrite_kva=9.0),
            _evt("2024-04-20 00:01:00", "RES", puissance_souscrite_kva=9.0),
        ],
    )
    df = spine(db).collect()
    fact = df.filter(pl.col("type_fait") == "facturation").sort("date_evenement")
    puiss = dict(zip(fact["date_evenement"].to_list(), fact["puissance_souscrite_kva"].to_list(), strict=True))
    # 02-01 et 03-01 héritent du MES (6) ; le MCT (10/03) ne s'applique qu'à partir du 04-01.
    assert puiss[datetime(2024, 2, 1, tzinfo=PARIS)] == 6.0
    assert puiss[datetime(2024, 3, 1, tzinfo=PARIS)] == 6.0
    assert puiss[datetime(2024, 4, 1, tzinfo=PARIS)] == 9.0
    # La situation est aussi propagée (FTA portée par le forward-fill).
    assert (fact["formule_tarifaire_acheminement"] == "BTINFCU4").all()


def test_niveau_forward_fille_sur_mdprm(tmp_path):
    """ADR-0039 : le niveau d'ouverture se propage depuis un MDPRM (événement SANS index).

    Le bug corrigé : le niveau était forward-fillé depuis les seuls relevés C15 indexés →
    un MDPRM qui change l'ouverture restait invisible. La spine forward-fille sur la
    timeline d'événements COMPLÈTE → le MDPRM compte.
    """
    db = _spine_depuis_evenements(
        tmp_path,
        [
            _evt("2024-01-15 00:01:00", "MES", niveau_ouverture_services="0"),
            _evt("2024-02-20 00:01:00", "MDPRM", niveau_ouverture_services="2"),
            _evt("2024-04-20 00:01:00", "RES", niveau_ouverture_services="2"),
        ],
    )
    df = spine(db).collect()
    fact = df.filter(pl.col("type_fait") == "facturation").sort("date_evenement")
    niv = dict(zip(fact["date_evenement"].to_list(), fact["niveau_ouverture_services"].to_list(), strict=True))
    assert niv[datetime(2024, 2, 1, tzinfo=PARIS)] == "0"  # avant le MDPRM
    assert niv[datetime(2024, 3, 1, tzinfo=PARIS)] == "2"  # après le MDPRM
    assert niv[datetime(2024, 4, 1, tzinfo=PARIS)] == "2"


# --- Correction du bug month_start de bord de mois (ADR-0041) ----------------------------
#
# La parité vs l'ANCIEN `pipeline_historique` (assemblage depuis C15) a été prouvée à
# l'introduction de la spine (#375). Depuis #378, `pipeline_historique` consomme la spine :
# la comparer à elle-même serait circulaire. La parité de bout en bout (abonnements +
# facturation mensuelle ≡ avant) est portée par les snapshots de `tests/integration/
# test_pipelines_snapshot.py` (sortie d'abonnement inchangée) + validation HITL prod.


def test_spine_corrige_la_facturation_de_bord_de_mois(tmp_path):
    """ADR-0041 corrige un bug latent de l'ancien `pipeline_historique` : `dt.month_start()`
    y PRÉSERVAIT l'heure, donc pour une entrée le mois précédant l'horizon (à 00:01), la borne
    FACTURATION unique (premier_mois 00:01 > dernier_mois 00:00) était silencieusement omise.

    La spine génère la grille en calendrier (heure remise à zéro) → la borne est présente.
    C'est un fix : la période devient facturable. Depuis #378 `pipeline_historique` hérite
    naturellement de cette grille correcte (il consomme la spine)."""
    db = _spine_depuis_evenements(tmp_path, [_evt("2024-08-01 00:01:00", "MES", rsc="Z")])
    sp = spine(db).collect().filter(pl.col("date_evenement") <= datetime(2024, 9, 1, tzinfo=PARIS))

    sp_fact = sp.filter(pl.col("type_fait") == "facturation")["date_evenement"].to_list()
    assert sp_fact == [datetime(2024, 9, 1, tzinfo=PARIS)]  # borne présente (grille calendaire)
