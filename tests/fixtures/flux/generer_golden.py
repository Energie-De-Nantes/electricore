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

# (fixture, flux, index dans xml_configs) — une entrée par table golden
FIXTURES: list[tuple[str, str, int]] = [
    ("c15_avec_releves.xml", "C15", 0),
    ("f12.xml", "F12", 0),
    ("f15.xml", "F15", 0),
    ("r15.xml", "R15", 0),
    ("r15.xml", "R15", 1),  # flux_r15_acc sur le même fichier (aucune donnée ACC → 1 record sans ea_*)
    ("r151.xml", "R151", 0),
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

    for nom, flux, idx in FIXTURES:
        entry = config_flux[flux]["xml_configs"][idx]
        config = ConfigFluxXml.depuis_yaml(entry)
        records = list(parser_flux_xml((ICI / nom).read_bytes(), config, tracabilite(nom, flux)))
        cible = dossier_golden / f"{entry['name']}.json"
        cible.write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n")
        print(f"{cible.name}: {len(records)} record(s) depuis {nom}")

    # R64 : parser JSON spécialisé, pas de ConfigFluxXml
    records = list(parser_flux_r64((ICI / "r64.json").read_bytes(), tracabilite("r64.json", "R64")))
    cible = dossier_golden / "flux_r64.json"
    cible.write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n")
    print(f"{cible.name}: {len(records)} record(s) depuis r64.json")


if __name__ == "__main__":
    main()
