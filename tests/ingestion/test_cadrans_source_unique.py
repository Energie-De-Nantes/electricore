"""Source unique du fan-out cadran (ADR-0035 §1, #292).

`core/models/cadrans.py` est le propriétaire unique du vocabulaire des 7 cadrans
(`index_*_kwh`). Côté dbt il est *injecté* via la var `cadrans_releve` ; côté cœur
il est *importé* par Pandera (`RelevéIndex` dérive ses colonnes d'index de `CADRANS`).
Ces tests prouvent l'**unicité de la décision** : la liste cesse d'être recopiée à la
main (~7 fois) ; toute dérive entre les deux côtés rougit.
"""

from pathlib import Path

import yaml

from electricore.core.models.cadrans import CADRANS, col_index
from electricore.core.models.releve_index import RelevéIndex

RACINE = Path(__file__).parents[2]
PROJET_DBT = RACINE / "electricore" / "ingestion" / "dbt"


def test_var_dbt_cadrans_egale_cadrans_py():
    """La var dbt `cadrans_releve` (injectée dans la macro de fan-out) est EXACTEMENT
    `CADRANS` — même liste, même ordre. C'est l'unique déclaration côté dbt."""
    projet = yaml.safe_load((PROJET_DBT / "dbt_project.yml").read_text())
    cadrans_dbt = projet["vars"]["cadrans_releve"]

    assert cadrans_dbt == list(CADRANS)


def test_releve_index_derive_ses_index_de_cadrans_py():
    """Le contrat Pandera `RelevéIndex` porte EXACTEMENT les colonnes d'index dérivées
    de `CADRANS` (plus aucune `index_*` hand-listée) — ni plus, ni moins."""
    colonnes = set(RelevéIndex.to_schema().columns)
    index_attendus = {col_index(c) for c in CADRANS}

    assert index_attendus <= colonnes, f"colonnes d'index manquantes : {index_attendus - colonnes}"
    index_presents = {c for c in colonnes if c.startswith("index_") and c.endswith("_kwh")}
    assert index_presents == index_attendus, f"index hors cadrans.py : {index_presents - index_attendus}"
