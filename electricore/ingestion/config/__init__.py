# Configuration centralisée

"""Lecture pure de `flux.yaml` — aucune dépendance `dlt` (#535).

`flux_connus()` est la source unique de « quels flux existent » (glossaire :
« Flux connu », `ingestion/CONTEXT.md`). Le runner, l'API et le bot en dérivent
tous leur validation/menu au lieu de recopier la liste à la main — la dérive
observée (c12/affaires ingérables en CLI mais absents d'API/bot) est le
symptôme que cette fonction supprime.
"""

from pathlib import Path

import yaml

_CHEMIN_FLUX_YAML = Path(__file__).parent / "flux.yaml"


def flux_connus() -> frozenset[str]:
    """Clés de `flux.yaml`, en minuscules — les *flux connus* de l'ingestion.

    Lecture YAML pure (pas d'import `dlt`) : l'API (qui embarque l'extra
    `[ingestion]`) en dérive sa validation sans charger dlt. `core`, lui, n'en
    dérive jamais (ADR-0053 mur 2, verrou dans `test_core_purity.py`) — ses
    registres sont *comparés* par le test de parité.
    """
    config = yaml.safe_load(_CHEMIN_FLUX_YAML.read_text())
    return frozenset(cle.lower() for cle in config)
