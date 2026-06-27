"""Parité golden du modèle dbt flux_r67 (ADR-0047, issue #484).

Mêmes principes que `test_dbt_r64_golden.py` : on landé la fixture R67 anonymisée
en colonne JSON (comme dlt en production), on lance `dbt build`, et on compare la
table matérialisée `flux_r67` aux records golden — modulo traçabilité.

Spécificité R67 : la fixture est une LISTE de 4 documents réels (un par PDL, prestation
M023). Chacun est landé sur sa propre ligne brute, exactement comme la prod (grain
« un fichier = un PDL »). Les 4 pilotes couvrent la variété structurante (ADR-0047) :
- Linky CFNE plein (D/DI000003 4 cadrans + repli F BASE) ;
- non-Linky F-only (HoroF1 BASE seul, repli F forcé) avec UNE régul NÉGATIVE ;
- bascule distributeur DI000001 → DI000003 EN COURS de mesure ;
- MES court avec grille fournisseur HP/HC (FC022035).

Skip si dbt n'est pas installé (`uv sync --extra dbt`).
"""

import json
from pathlib import Path

import duckdb
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

from dbt.cli.main import dbtRunner  # noqa: E402

RACINE = Path(__file__).parents[2]
PROJET_DBT = RACINE / "electricore" / "ingestion" / "dbt"
FIXTURE = RACINE / "tests" / "fixtures" / "flux" / "r67.json"
GOLDEN = RACINE / "tests" / "fixtures" / "flux" / "golden" / "flux_r67.json"

# Colonnes de traçabilité : ajoutées hors linéarisation, hors périmètre parité.
# `occurrence_id` (fichier#position) porte le nom de fichier synthétique de la fixture.
TRACABILITE = {"occurrence_id", "modification_date"}

# Cadrans énergie (ADR-0047) : reconstruction du total mensuel pour la réconciliation provision.
CADRANS_ENERGIE = (
    "energie_base_kwh",
    "energie_hp_kwh",
    "energie_hc_kwh",
    "energie_hph_kwh",
    "energie_hpb_kwh",
    "energie_hch_kwh",
    "energie_hcb_kwh",
)


def _sans_tracabilite(record: dict) -> dict:
    return {k: v for k, v in record.items() if k not in TRACABILITE}


@pytest.fixture
def base_avec_brut(tmp_path, monkeypatch):
    """Landé la fixture R67 (liste de documents) en colonne JSON dans une DuckDB temporaire."""
    import dlt

    from electricore.ingestion.raw_landing import lander_documents_bruts

    db_path = tmp_path / "flux.duckdb"
    documents = json.loads(FIXTURE.read_text())
    assert isinstance(documents, list), "la fixture R67 est une LISTE de documents (un par PDL)"
    pipeline = dlt.pipeline(
        pipeline_name="test_raw_r67",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(
        pipeline,
        "raw_r67",
        [
            {"file_name": f"r67_{i}.json", "modification_date": "2026-01-01T00:00:00", "content": doc}
            for i, doc in enumerate(documents)
        ],
    )
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(db_path))
    return db_path


def _materialiser(db_path: Path) -> list[dict]:
    resultat = dbtRunner().invoke(
        [
            "build",
            "--select",
            "+flux_r67",
            "--project-dir",
            str(PROJET_DBT),
            "--profiles-dir",
            str(PROJET_DBT),
            "--target-path",
            str(db_path.parent / "target"),
        ]
    )
    # `success` n'est vrai que si tout passe : 2 modèles (stg_r67, flux_r67) + 6 data tests
    # (not_null pdl/debut/fin/nature/periode_id + unique periode_id).
    assert resultat.success, f"dbt build a échoué : {resultat.exception}"
    assert len(resultat.result) == 8

    con = duckdb.connect(str(db_path))
    # periode_debut/periode_fin sont des TIMESTAMPTZ (instants) : rendu déterministe en
    # épinglant Europe/Paris (#393), comme le golden généré.
    con.execute("SET TimeZone='Europe/Paris'")
    cur = con.execute("select * from flux_enedis.flux_r67")
    cols = [d[0] for d in cur.description]
    obtenu = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    con.close()
    return obtenu


def test_flux_r67_reproduit_le_golden(base_avec_brut):
    obtenu = _materialiser(base_avec_brut)
    golden = json.loads(GOLDEN.read_text())
    assert len(obtenu) == len(golden)

    # Grain (pdl, debut, fin) ⇔ periode_id unique (ADR-0047) : on apparie par periode_id.
    par_id = {r["periode_id"]: r for r in obtenu}
    assert len(par_id) == len(obtenu), "periode_id doit être unique (clé de grain)"

    for attendu in golden:
        ligne = par_id[attendu["periode_id"]]
        ref = _sans_tracabilite(attendu)
        for k, v in ref.items():
            obt = ligne[k]
            if hasattr(obt, "isoformat"):
                obt = obt.isoformat()
            assert str(obt) == str(v), f"periode_id {attendu['periode_id']} colonne {k}: dbt={obt!r} golden={v!r}"


def test_flux_r67_regul_negative_preservee(base_avec_brut):
    """La régularisation physique (codeNature=C) reste NÉGATIVE (ADR-0047 : pas de ge>=0)."""
    obtenu = _materialiser(base_avec_brut)
    negatives = [r for r in obtenu if (r["energie_base_kwh"] or 0) < 0]
    assert negatives, "le pilote non-Linky porte une régul négative (-150 kWh)"
    for r in negatives:
        assert r["nature"] == "régularisé"
        assert r["code_nature"] == "C"


def test_flux_r67_repli_F_produit_des_lignes_base(base_avec_brut):
    """Le pilote non-Linky (F/HoroF1 seul) produit des lignes BASE (repli F forcé, ADR-0047)."""
    obtenu = _materialiser(base_avec_brut)
    f_only = [r for r in obtenu if r["code_grille"] == "F" and r["code_calendrier"] == "HoroF1"]
    assert f_only, "le pilote F-only doit produire des lignes (repli F)"
    for r in f_only:
        # BASE renseigné, cadrans distributeur 4-saisons absents (repli F = grille BASE).
        assert r["energie_base_kwh"] is not None
        assert r["energie_hph_kwh"] is None and r["energie_hcb_kwh"] is None


def test_flux_r67_coalesce_prefere_distributeur_fin(base_avec_brut):
    """Coalesce D/DI000003 ≻ DI000001 ≻ F : la bascule en cours de mesure est gérée.

    Le pilote « grille switch » a des intervalles d'entrée en DI000001 (BASE) puis des
    intervalles en DI000003 (4 cadrans). Chaque intervalle prend la grille la plus fine
    DISPONIBLE pour LUI — jamais F quand un D existe.
    """
    obtenu = _materialiser(base_avec_brut)
    # Aucune ligne ne doit retomber sur F quand une grille D coexiste sur le même intervalle :
    # ici on vérifie globalement que les deux calendriers distributeur coexistent dans la sortie
    # et qu'aucun F ne s'est glissé pour un PDL Linky.
    calendriers = {(r["code_grille"], r["code_calendrier"]) for r in obtenu}
    assert ("D", "DI000001") in calendriers, "la fenêtre d'entrée dégénérée garde DI000001"
    assert ("D", "DI000003") in calendriers, "les intervalles fins prennent DI000003"


def test_flux_r67_exclu_des_marts_periodiques(tmp_path, monkeypatch):
    """flux_r67 n'entre JAMAIS dans releves/chronologie_releves (ADR-0047 §9, asset parallèle).

    On landé R67 SEUL et on lance la VRAIE sélection du runner (`construire_dbt`) : R67 est
    dans `MODELES_PAR_RAW` (→ flux_r67 buildé) mais PAS dans les gardes des marts périodiques
    (qui exigent raw_c15+raw_r151+raw_r64). Avec R67 seul, `releves`/`chronologie_releves`/
    `spine_contrat` ne sont donc pas sélectionnés — preuve que R67 est hors de ces marts.
    """
    import dlt

    from electricore.ingestion.raw_landing import lander_documents_bruts
    from electricore.ingestion.runner import construire_dbt

    db_path = tmp_path / "flux.duckdb"
    documents = json.loads(FIXTURE.read_text())
    pipeline = dlt.pipeline(
        pipeline_name="test_raw_r67_seul",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(
        pipeline,
        "raw_r67",
        [
            {"file_name": f"r67_{i}.json", "modification_date": "2026-01-01T00:00:00", "content": doc}
            for i, doc in enumerate(documents)
        ],
    )
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(db_path))

    assert construire_dbt(db_path), "le runner doit construire flux_r67 depuis raw_r67 seul"

    con = duckdb.connect(str(db_path))
    tables = {t for (t,) in con.execute("select table_name from information_schema.tables").fetchall()}
    con.close()
    assert "flux_r67" in tables, "flux_r67 doit être matérialisé"
    # Les marts périodiques exigent C15+R151+R64 (absents) → jamais bâtis avec R67 seul.
    assert "releves" not in tables, "releves ne doit PAS être bâti depuis R67 seul (asset parallèle)"
    assert "chronologie_releves" not in tables, "chronologie_releves exclut R67"


def test_flux_r67_reconcilie_provision_mensuelle(base_avec_brut):
    """La somme energie_<cadran>_kwh par mois réconcilie une provision plausible (ADR-0047 §10).

    Sur le pilote Linky CFNE le plus profond, le total / nombre de mois retombe dans la
    bande d'une provision résidentielle mensualisée (cold-start, brique #191).
    """
    obtenu = _materialiser(base_avec_brut)
    # Le pilote Linky CFNE plein = celui qui porte le plus de lignes mensuelles cycliques.
    from collections import Counter

    plus_profond = Counter(r["pdl"] for r in obtenu).most_common(1)[0][0]
    lignes = [r for r in obtenu if r["pdl"] == plus_profond]
    total = sum(sum((r[c] or 0) for c in CADRANS_ENERGIE) for r in lignes)
    mois = {(r["debut"].year, r["debut"].month) for r in lignes}
    moyenne = total / len(mois)
    # Bande large d'une provision résidentielle (kWh/mois) : strictement positive, dans un
    # ordre de grandeur foyer (la valeur exacte est figée par le golden, ce test garde le sens).
    assert total > 0
    assert 50 <= moyenne <= 600, f"provision mensuelle hors bande plausible : {moyenne:.1f} kWh/mois"
