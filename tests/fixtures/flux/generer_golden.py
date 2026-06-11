"""Régénère les fichiers golden (records attendus) depuis les fixtures (#121).

Parse chaque fixture avec sa Configuration de flux **réelle** (flux.yaml) et
matérialise les records dans `golden/<table>.json`. À ne relancer que lorsqu'un
changement de comportement du parsing est *voulu* — le diff git des golden est
alors la revue du changement de contrat.

Usage :
    uv run python tests/fixtures/flux/generer_golden.py
"""

import json
from pathlib import Path

import yaml

from electricore.etl.parsing import ConfigFluxXml, TracabiliteFlux, parser_flux_r64, parser_flux_xml

ICI = Path(__file__).parent
RACINE = ICI.parents[2]

# (fixture, flux, index dans xml_configs, cas) — une entrée par fichier golden.
# `cas` nomme le golden ; None → nom de la table (entry['name']). Plusieurs fixtures
# peuvent cibler la même table via des `cas` distincts (cas réel vs edge-case forgé).
FIXTURES: list[tuple[str, str, int, str | None]] = [
    ("c15_avec_releves.xml", "C15", 0, None),
    ("f12.xml", "F12", 0, None),
    ("f15.xml", "F15", 0, None),
    ("r15.xml", "R15", 0, None),
    ("r15.xml", "R15", 1, None),  # flux_r15_acc sur fixture réelle → aucune donnée ACC
    # Edge-case forgé (schéma-valide R15 v2.3.2) : ACC peuplé, classes 3-6, 2 cadrans.
    ("r15_acc.xml", "R15", 1, "flux_r15_acc_peuple"),
    ("r151.xml", "R151", 0, None),
    # Fixtures générées depuis les XSD Enedis (generer_fixtures_xsd.py) : instances
    # maximales (optionnels présents, enums cyclées) — filet « on ne casse pas
    # l'ingestion » sur des champs que les échantillons réels n'exercent pas.
    ("c15_xsd.xml", "C15", 0, "flux_c15_xsd"),
    ("r15_xsd.xml", "R15", 0, "flux_r15_xsd"),
    ("r15_xsd.xml", "R15", 1, "flux_r15_acc_xsd"),
    ("r151_xsd.xml", "R151", 0, "flux_r151_xsd"),
    ("f12_xsd.xml", "F12", 0, "flux_f12_detail_xsd"),
    ("f15_xsd.xml", "F15", 0, "flux_f15_detail_xsd"),
]

# Traçabilité fixe → golden déterministes
TRACABILITE_FIXE = "2026-01-01T00:00:00"


def tracabilite(nom: str, flux: str) -> TracabiliteFlux:
    return TracabiliteFlux(
        source_zip="fixture.zip", nom_fichier=nom, flux_type=flux, modification_date=TRACABILITE_FIXE
    )


def main() -> None:
    config_flux = yaml.safe_load((RACINE / "electricore/etl/config/flux.yaml").read_text())
    dossier_golden = ICI / "golden"
    dossier_golden.mkdir(exist_ok=True)

    for nom, flux, idx, cas in FIXTURES:
        entry = config_flux[flux]["xml_configs"][idx]
        config = ConfigFluxXml.depuis_yaml(entry)
        records = list(parser_flux_xml((ICI / nom).read_bytes(), config, tracabilite(nom, flux)))
        cible = dossier_golden / f"{cas or entry['name']}.json"
        cible.write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n")
        print(f"{cible.name}: {len(records)} record(s) depuis {nom}")

    # R64 : parser JSON spécialisé, pas de ConfigFluxXml
    records = list(parser_flux_r64((ICI / "r64.json").read_bytes(), tracabilite("r64.json", "R64")))
    cible = dossier_golden / "flux_r64.json"
    cible.write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n")
    print(f"{cible.name}: {len(records)} record(s) depuis r64.json")


if __name__ == "__main__":
    main()
