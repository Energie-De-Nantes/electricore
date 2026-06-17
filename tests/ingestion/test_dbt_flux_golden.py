"""Parité golden des modèles dbt de linéarisation des flux XML (ADR-0020).

Harnais unique, paramétré par flux : on convertit le XML en dict générique
(`xml_vers_dict`), on le landé en colonne JSON (comme dlt en production), on lance
`dbt build` (modèles + data_tests), et on compare la table matérialisée au golden.

Deux garanties par cas :
- **Parité de valeurs modulo représentation** : traçabilité exclue ; horodatages typés
  comparés sur l'instant (pas la string) ; décimaux comparés par valeur
  (« 20.000 » == 20.0). Pendant de la politique timezone tranchée en #124.
- **Contrat de types XSD** : chaque colonne typée doit avoir le type DuckDB dicté par
  le XSD Enedis (dateTime → instant, date, integer, decimal). Verrouille les casts là
  où la parité de valeurs est laxiste sur les dtypes.

Couvre #124 (C15) et #125 (R15, R15 ACC, R151, F12, F15) ; R64 a son propre test JSON.
Inclut un edge-case ACC forgé (schéma-valide) qui peuple `flux_r15_acc` (golden réel
vide). Skip si dbt n'est pas installé (`uv sync --extra dbt`).
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
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
GOLDEN = FIXTURES / "golden"

# Colonnes de traçabilité : ajoutées hors linéarisation, hors périmètre parité.
TRACABILITE = {"_source_zip", "_flux_type", "_xml_name", "_json_name", "modification_date"}

# Types DuckDB issus des XSD Enedis (dateTime → instant, date → date nue, integer,
# decimal). Le contrat de types verrouille les casts : un cast retiré (colonne
# retombée en VARCHAR) fait échouer le test, là où la parité de valeurs ne le verrait
# pas. Les colonnes d'énergie (*_kwh, Valeur = xsd:integer) sont BIGINT par règle
# uniforme ; les scalaires typés sont déclarés par flux.
TZ = "TIMESTAMP WITH TIME ZONE"  # xsd:dateTime (offset → instant)
DATE = "DATE"  # xsd:date (date nue)
DOUBLE = "DOUBLE"  # xsd:decimal


@dataclass(frozen=True)
class FluxSpec:
    """Spécifie un cas de flux XML à valider contre son golden.

    `model` est le modèle dbt construit ; `cas` nomme le golden (par défaut = model).
    Plusieurs cas peuvent viser le même modèle (fixture réelle vs edge-case forgé).
    """

    model: str  # nom du modèle dbt = nom de la table
    fixture: str  # fichier XML source
    source: str  # table source brute (raw_*)
    cas: str | None = None  # stem du golden ; None → model
    instant_cols: frozenset[str] = field(default_factory=frozenset)  # comparés sur l'instant
    numeric_cols: frozenset[str] = field(default_factory=frozenset)  # comparés par valeur
    type_contract: dict[str, str] = field(default_factory=dict)  # colonne → type DuckDB attendu (XSD)

    @property
    def golden_stem(self) -> str:
        return self.cas or self.model


SPECS = [
    FluxSpec(
        "flux_c15",
        "c15_avec_releves.xml",
        "raw_c15",
        instant_cols=frozenset({"date_evenement", "avant_date_releve", "apres_date_releve"}),
        type_contract={
            "puissance_souscrite_kva": DOUBLE,
            "date_derniere_modification_fta": DATE,
            "date_changement_niveau_ouverture_services": DATE,
            "date_evenement": TZ,
            "avant_date_releve": TZ,
            "apres_date_releve": TZ,
        },
    ),
    FluxSpec(
        "flux_r15",
        "r15.xml",
        "raw_r15",
        instant_cols=frozenset({"date_releve"}),
        type_contract={"date_releve": TZ},
    ),
    FluxSpec(
        "flux_r15_acc",
        "r15.xml",
        "raw_r15",
        instant_cols=frozenset({"date_releve"}),
        type_contract={"date_releve": TZ},
    ),
    # Edge-case forgé (schéma-valide R15) : ACC peuplé, classes 3-6, autoconsommée 2 cadrans.
    FluxSpec(
        "flux_r15_acc",
        "r15_acc.xml",
        "raw_r15",
        cas="flux_r15_acc_peuple",
        instant_cols=frozenset({"date_releve"}),
        type_contract={"date_releve": TZ},
    ),
    FluxSpec("flux_r151", "r151.xml", "raw_r151", type_contract={"date_releve": DATE}),
    FluxSpec(
        "flux_f12_detail",
        "f12.xml",
        "raw_f12",
        numeric_cols=frozenset({"puissance_ponderee_kva", "quantite", "prix_unitaire", "montant_ht"}),
        type_contract={
            "puissance_ponderee_kva": DOUBLE,
            "quantite": DOUBLE,
            "prix_unitaire": DOUBLE,
            "montant_ht": DOUBLE,
            "date_debut": DATE,
            "date_fin": DATE,
            "date_facture": DATE,
        },
    ),
    FluxSpec(
        "flux_f15_detail",
        "f15.xml",
        "raw_f15",
        numeric_cols=frozenset({"prix_unitaire", "quantite", "montant_ht"}),
        type_contract={
            "prix_unitaire": DOUBLE,
            "quantite": DOUBLE,
            "montant_ht": DOUBLE,
            "date_debut": DATE,
            "date_fin": DATE,
            "date_facture": DATE,
        },
    ),
    # Fixtures générées depuis les XSD Enedis (generer_fixtures_xsd.py) : instances
    # maximales schéma-valides — optionnels présents, enums cyclées. Si un modèle dbt
    # casse sur un champ que les échantillons réels n'exercent pas, ces cas le voient.
    FluxSpec(
        "flux_c15",
        "c15_xsd.xml",
        "raw_c15",
        cas="flux_c15_xsd",
        instant_cols=frozenset({"date_evenement", "avant_date_releve", "apres_date_releve"}),
    ),
    FluxSpec("flux_r15", "r15_xsd.xml", "raw_r15", cas="flux_r15_xsd", instant_cols=frozenset({"date_releve"})),
    FluxSpec(
        "flux_r15_acc",
        "r15_xsd.xml",
        "raw_r15",
        cas="flux_r15_acc_xsd",
        instant_cols=frozenset({"date_releve"}),
    ),
    FluxSpec("flux_r151", "r151_xsd.xml", "raw_r151", cas="flux_r151_xsd"),
    FluxSpec(
        "flux_f12_detail",
        "f12_xsd.xml",
        "raw_f12",
        cas="flux_f12_detail_xsd",
        numeric_cols=frozenset({"puissance_ponderee_kva", "quantite", "prix_unitaire", "montant_ht"}),
    ),
    FluxSpec(
        "flux_f15_detail",
        "f15_xsd.xml",
        "raw_f15",
        cas="flux_f15_detail_xsd",
        numeric_cols=frozenset({"prix_unitaire", "quantite", "montant_ht"}),
    ),
    # X12/X13 (affaires SGE) : id d'affaire + codes statut/objet/état portés en
    # *attributs* XML (premier flux à en exposer) ; origine dérivée du file_name.
    FluxSpec(
        "flux_affaires",
        "affaires_X12.xml",
        "raw_affaires",
        instant_cols=frozenset({"jalon_date_heure"}),
        type_contract={"jalon_date_heure": TZ, "affaire_date_effet": DATE, "jalon_num": "INTEGER"},
    ),
    FluxSpec(
        "flux_affaires",
        "affaires_X13.xml",
        "raw_affaires",
        cas="flux_affaires_x13",
        instant_cols=frozenset({"jalon_date_heure"}),
        type_contract={"jalon_date_heure": TZ, "affaire_date_effet": DATE, "jalon_num": "INTEGER"},
    ),
]


def _instant(valeur) -> datetime:
    return valeur if isinstance(valeur, datetime) else datetime.fromisoformat(str(valeur))


def _correspond(attendu: dict, obtenu: dict, spec: FluxSpec) -> bool:
    """Vrai si tous les champs golden (hors traçabilité) matchent la ligne obtenue."""
    for k, v in attendu.items():
        if k in TRACABILITE:
            continue
        if k not in obtenu:
            return False
        o = obtenu[k]
        if k in spec.instant_cols:
            if _instant(o) != _instant(v):
                return False
        elif k in spec.numeric_cols:
            if o is None or float(o) != float(v):
                return False
        elif str(o) != str(v):
            return False
    return True


@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.golden_stem)
def test_flux_reproduit_le_golden(spec, tmp_path, monkeypatch):
    import dlt

    from electricore.ingestion.raw_landing import lander_documents_bruts

    db_path = tmp_path / "flux.duckdb"
    document = xml_vers_dict((FIXTURES / spec.fixture).read_bytes())
    pipeline = dlt.pipeline(
        pipeline_name=f"test_{spec.source}",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(
        pipeline,
        spec.source,
        [{"file_name": spec.fixture, "modification_date": "2026-01-01T00:00:00", "content": document}],
    )
    monkeypatch.setenv("DBT_DUCKDB_PATH", str(db_path))

    resultat = dbtRunner().invoke(
        [
            "build",
            "--select",
            f"+{spec.model}",
            "--project-dir",
            str(PROJET_DBT),
            "--profiles-dir",
            str(PROJET_DBT),
            "--target-path",
            str(tmp_path / "target"),
        ]
    )
    assert resultat.success, f"dbt build {spec.model} a échoué : {resultat.exception}"

    con = duckdb.connect(str(db_path))
    types = {n: t for n, t, *_ in con.execute(f"describe flux_enedis.{spec.model}").fetchall()}
    cur = con.execute(f"select * from flux_enedis.{spec.model}")
    cols = [d[0] for d in cur.description]
    obtenu = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    con.close()

    # Contrat de types XSD : scalaires typés déclarés + énergie (*_kwh) toujours BIGINT.
    for col, attendu_type in spec.type_contract.items():
        assert types.get(col) == attendu_type, f"{spec.model}.{col}: type {types.get(col)} ≠ {attendu_type} (XSD)"
    for col, t in types.items():
        if col.endswith("_kwh"):
            assert t == "BIGINT", f"{spec.model}.{col}: énergie attendue BIGINT (xsd:integer), obtenu {t}"

    golden = json.loads((GOLDEN / f"{spec.golden_stem}.json").read_text())
    assert len(obtenu) == len(golden), f"{spec.model}: {len(obtenu)} lignes vs golden {len(golden)}"

    # Appariement glouton : chaque ligne golden doit trouver une ligne obtenue non
    # encore consommée qui matche tous ses champs (les flux détail ont N lignes/PDL).
    restantes = list(obtenu)
    for attendu in golden:
        match = next((o for o in restantes if _correspond(attendu, o, spec)), None)
        assert match is not None, f"{spec.model}: aucune ligne obtenue ne matche {attendu}"
        restantes.remove(match)
