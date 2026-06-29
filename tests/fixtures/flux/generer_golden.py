"""Régénère les fichiers golden (records attendus) via le chemin dbt (#135).

L'oracle est le chemin de production lui-même : chaque fixture est convertie
(`xml_vers_dict` / `json.loads`), landée en colonne JSON dans une DuckDB jetable,
matérialisée par `dbt build`, et les lignes de la table `flux_enedis.flux_*` sont
figées dans `golden/<cas>.json` — valeurs typées (nombres, horodatages ISO),
sans colonnes nulles, triées pour un diff git stable.

À ne relancer que lorsqu'un changement de comportement est *voulu* — le diff git
des golden est alors la revue du changement de contrat. Toute évolution de flux
Enedis (nouvelle version XSD, nouveau champ exposé) passe par ici.

Usage :
    uv run python tests/fixtures/flux/generer_golden.py
"""

import json
import os
import tempfile
from datetime import date, datetime
from pathlib import Path

ICI = Path(__file__).parent
RACINE = ICI.parents[2]
PROJET_DBT = RACINE / "electricore" / "ingestion" / "dbt"

# (fixture, table brute, modèle dbt, cas) — une entrée par fichier golden.
# `cas` nomme le golden ; None → nom du modèle. Plusieurs fixtures peuvent viser le
# même modèle via des `cas` distincts (échantillon réel anonymisé vs edge-case forgé
# vs fixture XSD maximale).
CAS_GOLDEN: list[tuple[str, str, str, str | None]] = [
    ("c15_avec_releves.xml", "raw_c15", "flux_c15", None),
    ("f12.xml", "raw_f12", "flux_f12_detail", None),
    ("f15.xml", "raw_f15", "flux_f15_detail", None),
    ("r15.xml", "raw_r15", "flux_r15", None),
    ("r15.xml", "raw_r15", "flux_r15_acc", "flux_r15_acc"),
    # Edge-case forgé (schéma-valide R15 v2.3.2) : ACC peuplé, classes 3-6, 2 cadrans.
    ("r15_acc.xml", "raw_r15", "flux_r15_acc", "flux_r15_acc_peuple"),
    ("r151.xml", "raw_r151", "flux_r151", None),
    # Fixtures générées depuis les XSD Enedis (generer_fixtures_xsd.py) : instances
    # maximales (optionnels présents, enums cyclées).
    ("c15_xsd.xml", "raw_c15", "flux_c15", "flux_c15_xsd"),
    ("r15_xsd.xml", "raw_r15", "flux_r15", "flux_r15_xsd"),
    ("r15_xsd.xml", "raw_r15", "flux_r15_acc", "flux_r15_acc_xsd"),
    ("r151_xsd.xml", "raw_r151", "flux_r151", "flux_r151_xsd"),
    ("f12_xsd.xml", "raw_f12", "flux_f12_detail", "flux_f12_detail_xsd"),
    ("f15_xsd.xml", "raw_f15", "flux_f15_detail", "flux_f15_detail_xsd"),
    # R64 : document JSON natif, même landing.
    ("r64.json", "raw_r64", "flux_r64", None),
    # R67 (mesures facturantes, ADR-0047) : fixture = LISTE de documents JSON (un par PDL),
    # landés en plusieurs lignes (un document par fichier) — cf. lignes_via_dbt.
    ("r67.json", "raw_r67", "flux_r67", None),
    # C12 (description contractuelle C2-C4) : fixture hand-crafted (attributs XML requis
    # empêchent le générateur XSD, voir c12_xsd.xml pour le détail).
    ("c12_xsd.xml", "raw_c12", "flux_c12", "flux_c12_xsd"),
    # X12/X13 (affaires SGE) : même source raw_affaires, origine dérivée du file_name.
    ("affaires_X12.xml", "raw_affaires", "flux_affaires", None),
    ("affaires_X13.xml", "raw_affaires", "flux_affaires", "flux_affaires_x13"),
]


def _serialisable(valeur):
    if isinstance(valeur, (datetime, date)):
        return valeur.isoformat()
    return valeur


def lignes_via_dbt(fixture: Path, source: str, model: str) -> list[dict]:
    """Matérialise une fixture par le chemin de production et rend les lignes du modèle.

    Mêmes étapes que la prod (landing JSON → dbt build) ; les colonnes nulles sont
    omises et les lignes triées pour un golden stable.
    """
    import dlt
    import duckdb
    from dbt.cli.main import dbtRunner

    from electricore.ingestion.parsing.xml import xml_vers_dict
    from electricore.ingestion.raw_landing import lander_documents_bruts

    contenu = fixture.read_bytes()
    document = json.loads(contenu) if fixture.suffix == ".json" else xml_vers_dict(contenu)

    # Fixture-LISTE (R67, ADR-0047 : un document JSON par PDL) → une ligne brute par
    # document, comme dlt en production (le grain prod est « un fichier = un PDL »). Les
    # fixtures mono-document (tous les autres flux) restent une seule ligne.
    if isinstance(document, list):
        documents = [
            {"file_name": f"{fixture.stem}_{i}.json", "modification_date": "2026-01-01T00:00:00", "content": doc}
            for i, doc in enumerate(document)
        ]
    else:
        documents = [{"file_name": fixture.name, "modification_date": "2026-01-01T00:00:00", "content": document}]

    dossier = tempfile.mkdtemp(prefix="golden_dbt_")
    db_path = os.path.join(dossier, "flux.duckdb")
    pipeline = dlt.pipeline(
        pipeline_name=f"golden_{model}_{Path(dossier).name}",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(pipeline, source, documents)
    os.environ["DBT_DUCKDB_PATH"] = db_path
    resultat = dbtRunner().invoke(
        [
            "build",
            "--select",
            f"+{model}",
            "--project-dir",
            str(PROJET_DBT),
            "--profiles-dir",
            str(PROJET_DBT),
            "--target-path",
            os.path.join(dossier, "target"),
        ]
    )
    if not resultat.success:
        raise RuntimeError(f"dbt build {model} a échoué sur {fixture.name} : {resultat.exception}")

    con = duckdb.connect(db_path)
    # Fuseau du domaine (Europe/Paris, #393/ADR-0042) épinglé : les TIMESTAMPTZ (instants)
    # sont sérialisés en heure légale française de façon déterministe, quel que soit le fuseau
    # de la machine qui régénère le golden (sinon rendu UTC sur un hôte UTC → churn de golden).
    con.execute("SET TimeZone='Europe/Paris'")
    curseur = con.execute(f"select * from flux_enedis.{model}")
    colonnes = [d[0] for d in curseur.description]
    lignes = [
        {cle: _serialisable(val) for cle, val in zip(colonnes, ligne, strict=True) if val is not None}
        for ligne in curseur.fetchall()
    ]
    con.close()
    return sorted(lignes, key=lambda r: json.dumps(r, sort_keys=True, ensure_ascii=False))


def main() -> None:
    dossier_golden = ICI / "golden"
    dossier_golden.mkdir(exist_ok=True)
    for fixture, source, model, cas in CAS_GOLDEN:
        records = lignes_via_dbt(ICI / fixture, source, model)
        cible = dossier_golden / f"{cas or model}.json"
        cible.write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n")
        print(f"{cible.name}: {len(records)} record(s) depuis {fixture}")


if __name__ == "__main__":
    main()
