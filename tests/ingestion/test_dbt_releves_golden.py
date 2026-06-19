"""Modèle de relevés canonique `releves` (ADR-0029, #241).

Lande R151 (XML) + R64 (JSON) dans une même DuckDB, lance `dbt build --select
+releves` (modèles + data_tests, dont `unique releve_id` = invariant de grain et de
dedup même-source), puis vérifie l'union, l'harmonisation des dates et la présence
des sources.

Skip si dbt absent (`uv sync --extra dbt`).
"""

import json
import re
from pathlib import Path

import duckdb
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

from dbt.cli.main import dbtRunner  # noqa: E402

from electricore.core.models.cadrans import CADRANS, col_index  # noqa: E402
from electricore.core.models.parite_typage import ecarts_de_typage  # noqa: E402
from electricore.core.models.releve_index import RelevéIndex  # noqa: E402
from electricore.ingestion.parsing.xml import xml_vers_dict  # noqa: E402

RACINE = Path(__file__).parents[2]
PROJET_DBT = RACINE / "electricore" / "ingestion" / "dbt"
FIXTURES = RACINE / "tests" / "fixtures" / "flux"


@pytest.fixture
def base_periodiques(tmp_path, monkeypatch):
    """Lande R151 (XML) + R64 (JSON) en colonnes brutes dans une DuckDB temporaire."""
    import dlt

    from electricore.ingestion.raw_landing import lander_documents_bruts

    db_path = tmp_path / "flux.duckdb"
    pipeline = dlt.pipeline(
        pipeline_name="test_releves",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(
        pipeline,
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
        pipeline,
        "raw_r64",
        [
            {
                "file_name": "r64.json",
                "modification_date": "2026-01-01T00:00:00",
                "content": json.loads((FIXTURES / "r64.json").read_text()),
            }
        ],
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
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(db_path))
    return db_path


def _build_releves(target_parent):
    return dbtRunner().invoke(
        [
            "build",
            "--select",
            "+releves",
            "--project-dir",
            str(PROJET_DBT),
            "--profiles-dir",
            str(PROJET_DBT),
            "--target-path",
            str(target_parent / "target"),
        ]
    )


def test_runner_construit_releves_via_sa_propre_selection(base_periodiques):
    """Régression rc11 : le runner de prod (`construire_dbt`) sélectionne par `+flux_*`
    (ancêtres des raw landés) + `releves`. `int_releves__c15` est un ancêtre de `releves`
    qui n'est PAS un `flux_*` → la sélection du runner doit l'inclure (sinon
    `Catalog Error: int_releves__c15 does not exist`). Le golden ci-dessous utilise
    `+releves` (qui tire les ancêtres) et ne couvrait donc PAS le chemin de prod ;
    ce test exerce la VRAIE sélection du runner."""
    from electricore.ingestion.runner import construire_dbt

    assert construire_dbt(base_periodiques), (
        "le runner doit construire releves ET tous ses ancêtres (dont int_releves__c15)"
    )
    con = duckdb.connect(str(base_periodiques))
    n = con.execute("select count(*) from flux_enedis.releves").fetchone()[0]
    con.close()
    assert n > 0, "releves doit être matérialisé et non vide"


def test_releves_dbt_respecte_le_contrat_pandera(base_periodiques):
    """Parité de typage dbt↔cœur (ADR-0035, #291). Le schéma réellement émis par le
    mart `releves` doit être type-compatible avec le contrat Pandera `RelevéIndex`, via
    la table de correspondance SQL↔Polars. C'est le garde-fou de frontière qui manquait
    quand le bug ADR-0034 (index ~1000× trop grands) a glissé silencieusement. On lit le
    type **réellement émis par dbt** (`DESCRIBE`), pas la sortie post-cast du loader :
    sinon un re-typage côté loader blanchirait une dérive du modèle dbt. Nullabilité hors
    périmètre (axe par couche)."""
    resultat = _build_releves(base_periodiques.parent)
    assert resultat.success, f"dbt build releves a échoué : {resultat.exception}"

    con = duckdb.connect(str(base_periodiques))
    schema_sql = {nom: type_sql for nom, type_sql, *_ in con.execute("describe flux_enedis.releves").fetchall()}
    con.close()

    ecarts = ecarts_de_typage(schema_sql, RelevéIndex)
    assert not ecarts, f"divergences de typage dbt↔RelevéIndex (dbt, pandera) : {ecarts}"


def test_releves_emet_exactement_les_index_de_cadrans_py(base_periodiques):
    """Source unique du fan-out cadran (ADR-0035 §1, #292) : le mart émet EXACTEMENT les
    colonnes d'index dérivées de `cadrans.py` (la macro `pivot_cadrans` est alimentée par
    la var `cadrans_releve` = `CADRANS`). Garde-fou complémentaire à la parité de types
    (#291, qui compare l'intersection et ne verrait pas une colonne d'index manquante)."""
    resultat = _build_releves(base_periodiques.parent)
    assert resultat.success, f"dbt build releves a échoué : {resultat.exception}"

    con = duckdb.connect(str(base_periodiques))
    cols = {nom for nom, *_ in con.execute("describe flux_enedis.releves").fetchall()}
    con.close()

    index_mart = {c for c in cols if c.startswith("index_") and c.endswith("_kwh")}
    assert index_mart == {col_index(c) for c in CADRANS}


def test_releves_union_grain_et_harmonisation(base_periodiques):
    # `dbt build` matérialise releves ET exécute `unique releve_id` : un build vert
    # prouve l'invariant de grain / dedup même-source.
    resultat = _build_releves(base_periodiques.parent)
    assert resultat.success, f"dbt build releves a échoué : {resultat.exception}"

    con = duckdb.connect(str(base_periodiques))

    # Grain : 1 ligne par releve_id (dedup même-source).
    n, n_uniq = con.execute("select count(*), count(distinct releve_id) from flux_enedis.releves").fetchone()
    assert n == n_uniq > 0, "grain : releve_id doit être unique dans releves"

    # Les trois sources sont présentes et étiquetées (R151, R64 périodiques + C15).
    sources = {r[0] for r in con.execute("select distinct source from flux_enedis.releves").fetchall()}
    assert sources == {"flux_R151", "flux_R64", "flux_C15"}

    # Harmonisation R151 : date_releve = date brute (flux_r151) + 1 jour (ADR-0003).
    bad = con.execute(
        """
        select count(*)
        from flux_enedis.releves r
        join flux_enedis.flux_r151 f on r.releve_id = f.releve_id
        where date_trunc('day', r.date_releve at time zone 'Europe/Paris')
              <> f.date_releve + interval '1 day'
        """
    ).fetchone()[0]
    assert bad == 0, "R151 : date_releve doit être harmonisée J → J+1"

    con.close()


def test_releves_inclut_c15_avant_apres(base_periodiques):
    """C15 dépivoté : avant/après deviennent des lignes, RSC portée nativement, nature mappée."""
    resultat = _build_releves(base_periodiques.parent)
    assert resultat.success, f"dbt build releves a échoué : {resultat.exception}"

    con = duckdb.connect(str(base_periodiques))
    c15 = [
        dict(zip([d[0] for d in cur.description], r, strict=True))
        for cur in [con.execute("select * from flux_enedis.releves where source = 'flux_C15'")]
        for r in cur.fetchall()
    ]
    con.close()

    assert c15, "C15 doit produire des relevés dans releves"
    # Avant (False) et après (True) coexistent en lignes distinctes.
    assert {r["ordre_index"] for r in c15} <= {False, True}
    assert any(r["ordre_index"] is True for r in c15), "au moins un relevé C15 'après'"
    # RSC portée nativement par l'événement contractuel (pas de forward-fill ici).
    assert all(r["ref_situation_contractuelle"] is not None for r in c15)
    # releve_id minté en hash court (ADR-0038, #359) + nature canonique mappée.
    assert all(r["releve_id"] and re.fullmatch(r"[0-9a-f]{16}", r["releve_id"]) for r in c15)
    assert all(r["nature_index"] in {"réel", "estimé", "corrigé"} for r in c15)


def test_releve_id_est_un_hash_court_stable(base_periodiques):
    """ADR-0038 (#359) : `releve_id` est reformaté en **hash court déterministe**
    (`substr(md5(<source|pdl|date|discriminant>), 1, 16)`) au seam dbt, AVANT exposition.
    Seul l'encodage change ; les composantes d'identité (ADR-0028) sont inchangées.

    On vérifie le contrat observable du mart `releves` :
    - **format** : 16 caractères hex bas-de-casse, plus aucune concaténation verbeuse
      (`|`, timestamp+tz) — coût de stockage/affichage côté ERP réduit ;
    - **pas de collision** : le grain `(RSC, date_releve, ordre_index)` reste injecté
      dans des `releve_id` distincts (1 hash par relevé logique) ;
    - **discriminant préservé** : l'avant et l'après d'un MÊME événement C15 (même source,
      pdl, date, discriminant différent) gardent des `releve_id` DISTINCTS — preuve que le
      discriminant reste une composante du hash (regroupement intact, `CORR ≡ BRUT`)."""
    resultat = _build_releves(base_periodiques.parent)
    assert resultat.success, f"dbt build releves a échoué : {resultat.exception}"

    con = duckdb.connect(str(base_periodiques))
    lignes = [
        dict(zip([d[0] for d in cur.description], r, strict=True))
        for cur in [con.execute("select releve_id, source, pdl, date_releve, ordre_index from flux_enedis.releves")]
        for r in cur.fetchall()
    ]
    con.close()

    # Format : 16 hex, jamais la concaténation verbeuse.
    assert all(re.fullmatch(r"[0-9a-f]{16}", r["releve_id"]) for r in lignes), (
        f"releve_id doit être un hash court 16-hex, vu : {sorted({r['releve_id'] for r in lignes})[:3]}"
    )

    # Pas de collision : autant de releve_id distincts que de relevés.
    ids = [r["releve_id"] for r in lignes]
    assert len(ids) == len(set(ids)) > 0, "pas de collision : 1 releve_id par relevé logique"

    # Discriminant préservé : avant (False) et après (True) d'un MÊME C15 logique
    # (même pdl + date) ont des releve_id DISTINCTS — le discriminant reste dans le hash.
    c15 = [r for r in lignes if r["source"] == "flux_C15"]
    par_evt: dict[tuple, set] = {}
    for r in c15:
        par_evt.setdefault((r["pdl"], r["date_releve"]), set()).add(r["releve_id"])
    assert any(len(ids_evt) >= 2 for ids_evt in par_evt.values()), (
        "un événement C15 avec avant ET après doit produire deux releve_id distincts"
    )


def test_releves_porte_niveau_ouverture_depuis_c15(base_periodiques):
    """#324 (voie communicante, ADR-0036) : le mart `releves` porte
    `niveau_ouverture_services` — la *jumelle* de `nature_index`. La valeur est portée
    NATIVEMENT par les relevés C15 (niveau PRM lu dans flux_c15), comme RSC/FTA. La
    fixture C15 a `Niveau_Ouverture_Services=2` au niveau PRM → tous ses relevés C15 le
    portent. Le forward-fill par PDL vers les périodiques est prouvé séparément sur une
    entrée synthétique (fixtures à PDL disjoints inter-flux, cf.
    `test_forward_fill_niveau_pilote_par_c15`)."""
    resultat = _build_releves(base_periodiques.parent)
    assert resultat.success, f"dbt build releves a échoué : {resultat.exception}"

    con = duckdb.connect(str(base_periodiques))
    cols = {nom for nom, *_ in con.execute("describe flux_enedis.releves").fetchall()}
    niveaux_c15 = (
        [
            r[0]
            for r in con.execute(
                "select distinct niveau_ouverture_services from flux_enedis.releves where source = 'flux_C15'"
            ).fetchall()
        ]
        if "niveau_ouverture_services" in cols
        else None
    )
    con.close()

    assert "niveau_ouverture_services" in cols, f"colonne niveau_ouverture_services manquante, vu : {sorted(cols)}"
    assert niveaux_c15 == ["2"], f"les relevés C15 doivent porter le niveau PRM, vu : {niveaux_c15}"


def test_releves_sans_colonne_id_releve_natif(base_periodiques):
    """L'id natif Enedis est retiré du contrat canonique (#304) : toujours NULL pour les
    trois sources vivantes (R151/R64 n'en ont pas, C15 le nullait), aucun consommateur ne
    le lit (ni ChronologieReleves ni RelevéIndex). La traçabilité repose sur releve_id
    (clé métier) + occurrence_id (provenance fichier). flux_r15/flux_r15_acc le gardent."""
    resultat = _build_releves(base_periodiques.parent)
    assert resultat.success, f"dbt build releves a échoué : {resultat.exception}"

    con = duckdb.connect(str(base_periodiques))
    cols = [r[0] for r in con.execute("describe flux_enedis.releves").fetchall()]
    con.close()

    assert "id_releve" not in cols, f"id_releve doit être retiré du contrat canonique, vu : {cols}"


def test_r64_porte_son_calendrier_distributeur(base_periodiques):
    """Fidélité R64 (#304) : un relevé R64 porte son calendrier distributeur (DI00000X)
    au lieu d'un NULL codé en dur — `flux_r64` filtre déjà sur ce calendrier puis le
    jette. Le cœur en dérive précision et cadrans (RelevéIndex). Fixture R64 = DI000003."""
    resultat = _build_releves(base_periodiques.parent)
    assert resultat.success, f"dbt build releves a échoué : {resultat.exception}"

    con = duckdb.connect(str(base_periodiques))
    cals = [
        r[0]
        for r in con.execute(
            "select distinct id_calendrier_distributeur from flux_enedis.releves where source = 'flux_R64'"
        ).fetchall()
    ]
    con.close()

    assert cals == ["DI000003"], f"R64 doit porter son calendrier distributeur, vu : {cals}"


def test_forward_fill_rsc_pilote_par_c15():
    """Enrichissement contractuel (#243) : une ligne périodique hérite la RSC/FTA du
    dernier relevé C15 amont (par PDL). Vérifie la fenêtre de forward-fill du modèle
    `releves` sur une entrée synthétique (les fixtures ont des PDL disjoints inter-flux ;
    la parité sur données réelles est la garde de la bascule cœur #244)."""
    con = duckdb.connect()
    con.execute(
        """
        create table synth (
            pdl varchar, date_releve timestamptz, source varchar, ordre_index boolean,
            ref_situation_contractuelle varchar, formule_tarifaire_acheminement varchar
        )
        """
    )
    con.execute(
        """
        insert into synth values
          -- PDL A : entrée C15 (après, RSC1/FTA1) puis 2 télérelevés périodiques (RSC nulle)
          ('A', timestamptz '2026-01-01 00:00:00+01', 'flux_C15',  true,  'RSC1', 'FTA1'),
          ('A', timestamptz '2026-01-05 00:00:00+01', 'flux_R151', false, null,   null),
          ('A', timestamptz '2026-02-01 00:00:00+01', 'flux_R64',  false, null,   null),
          -- PDL B : un télérelevé AVANT tout C15 → reste sans RSC
          ('B', timestamptz '2026-01-03 00:00:00+01', 'flux_R151', false, null,   null)
        """
    )
    rows = con.execute(
        """
        select pdl, strftime(date_releve at time zone 'Europe/Paris', '%Y-%m-%d') as jour,
            coalesce(ref_situation_contractuelle,
                     last_value(ref_situation_contractuelle ignore nulls) over w) as rsc,
            coalesce(formule_tarifaire_acheminement,
                     last_value(formule_tarifaire_acheminement ignore nulls) over w) as fta
        from synth
        window w as (
            partition by pdl
            order by date_releve,
                     case source when 'flux_C15' then 0 when 'flux_R64' then 1 else 2 end,
                     ordre_index
            rows between unbounded preceding and current row
        )
        """
    ).fetchall()
    con.close()

    by = {(r[0], r[1]): (r[2], r[3]) for r in rows}
    # PDL A : les périodiques héritent RSC1/FTA1 du C15 amont.
    assert by[("A", "2026-01-05")] == ("RSC1", "FTA1")
    assert by[("A", "2026-02-01")] == ("RSC1", "FTA1")
    # PDL B : périodique avant tout C15 → pas de RSC propagée.
    assert by[("B", "2026-01-03")] == (None, None)


def test_forward_fill_niveau_pilote_par_c15():
    """#324 (ADR-0036) : `niveau_ouverture_services` est porté par les relevés C15 (niveau
    PRM) et propagé aux périodiques par le MÊME forward-fill par PDL que RSC/FTA — la
    *jumelle* de plomberie de l'axe « voie communicante ». Une ligne périodique hérite le
    niveau du dernier relevé C15 amont (par PDL) ; un périodique avant tout C15 reste null.
    Prouvé sur une entrée synthétique (fixtures à PDL disjoints inter-flux)."""
    con = duckdb.connect()
    con.execute(
        """
        create table synth (
            pdl varchar, date_releve timestamptz, source varchar, ordre_index boolean,
            niveau_ouverture_services varchar
        )
        """
    )
    con.execute(
        """
        insert into synth values
          -- PDL A : événement C15 niveau 2, puis deux télérelevés périodiques (niveau null)
          ('A', timestamptz '2026-01-01 00:00:00+01', 'flux_C15',  true,  '2'),
          ('A', timestamptz '2026-01-05 00:00:00+01', 'flux_R151', false, null),
          ('A', timestamptz '2026-02-01 00:00:00+01', 'flux_R64',  false, null),
          -- PDL B : un télérelevé AVANT tout C15 → reste sans niveau
          ('B', timestamptz '2026-01-03 00:00:00+01', 'flux_R151', false, null)
        """
    )
    rows = con.execute(
        """
        select pdl, strftime(date_releve at time zone 'Europe/Paris', '%Y-%m-%d') as jour,
            coalesce(niveau_ouverture_services,
                     last_value(niveau_ouverture_services ignore nulls) over w) as niveau
        from synth
        window w as (
            partition by pdl
            order by date_releve,
                     case source when 'flux_C15' then 0 when 'flux_R64' then 1 else 2 end,
                     ordre_index
            rows between unbounded preceding and current row
        )
        """
    ).fetchall()
    con.close()

    by = {(r[0], r[1]): r[2] for r in rows}
    # PDL A : les périodiques héritent le niveau 2 du C15 amont.
    assert by[("A", "2026-01-05")] == "2"
    assert by[("A", "2026-02-01")] == "2"
    # PDL B : périodique avant tout C15 → pas de niveau propagé.
    assert by[("B", "2026-01-03")] is None
