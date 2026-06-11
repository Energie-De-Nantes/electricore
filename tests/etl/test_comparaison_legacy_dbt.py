"""Parité legacy vs dbt — sur fixtures (CI) et sur le cache réel (local).

Deux étages, même harnais (`etl/tools/comparaison_legacy_dbt`) :

- **Fixtures** (toujours, CI comprise) : les XML anonymisés sont parsés par le legacy
  ET par dbt, budget de divergence **zéro**. Complément des golden : ici on compare
  deux implémentations entre elles, sans JSON figé.
- **Cache réel** (opt-in : `ELECTRICORE_COMPARAISON_REELLE=1`, ~30 s) : ~4400 XML de
  `~/data/flux_enedis_v2` (surchargeable via `ELECTRICORE_FLUX_V2`). Chaque modèle a
  un **budget de divergence** documentant les résidus connus — le test échoue si on
  régresse au-delà ; faire cliqueter le budget vers 0 à mesure des corrections.
  Opt-in pour ne pas alourdir le pytest du hook pre-push ; à lancer avant toute
  bascule ou après un changement de modèle dbt :

      ELECTRICORE_COMPARAISON_REELLE=1 uv run --group test pytest \
          tests/etl/test_comparaison_legacy_dbt.py -q

C'est ce harnais qui a trouvé le bug de grain et le mélange index/conso R15 (PR #130),
invisibles aux golden. Skip si dbt n'est pas installé (`uv sync --extra dbt`).
"""

import os
from pathlib import Path

import pytest

pytest.importorskip("dbt.cli.main", reason="dbt absent — uv sync --extra dbt")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb absent — uv sync --extra dbt")

from electricore.etl.tools.comparaison_legacy_dbt import (  # noqa: E402
    CAS,
    CasComparaison,
    comparer_cas,
    fichiers_du_flux,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "flux"
CACHE_REEL = Path(os.environ.get("ELECTRICORE_FLUX_V2", str(Path.home() / "data" / "flux_enedis_v2")))

# Fixtures XML par flux (R15 : la réelle + l'edge-case ACC forgé).
FIXTURES_PAR_FLUX = {
    "C15": ["c15_avec_releves.xml"],
    "R15": ["r15.xml", "r15_acc.xml"],
    "R151": ["r151.xml"],
    "F12": ["f12.xml"],
    "F15": ["f15.xml"],
}

# Budgets de divergence sur le cache réel (mesurés, cf. PR #130). Résidus connus :
# - flux_r15 : 81×2 — réduction multi-classe résiduelle après le filtre CM1/Sens0
# - flux_r15_acc : 34×2 — ordre du pivot ea_ (multi-classe même cadran)
# - flux_r151 : 383×2 — pas de champ Classe_Mesure ; piste multi-Donnees_Releve
# - flux_f15_detail : 263+2 — Element_Valorise nichés hors du chemin figé
# Faire décroître ces budgets à mesure des corrections ; 0 = parité totale.
BUDGETS_REEL = {
    "flux_c15": 0,
    "flux_r15": 162,
    "flux_r15_acc": 68,
    "flux_r151": 766,
    "flux_f12_detail": 0,
    "flux_f15_detail": 265,
}


@pytest.mark.parametrize("cas", CAS, ids=lambda c: c.model)
def test_parite_legacy_dbt_sur_fixtures(cas: CasComparaison):
    fichiers = [str(FIXTURES / nom) for nom in FIXTURES_PAR_FLUX[cas.flux]]
    resultat = comparer_cas(cas, fichiers)
    assert resultat.divergences == 0, str(resultat)


@pytest.mark.slow
@pytest.mark.skip_ci
@pytest.mark.skipif(
    not os.environ.get("ELECTRICORE_COMPARAISON_REELLE"),
    reason="opt-in : ELECTRICORE_COMPARAISON_REELLE=1 (≈30 s sur ~4400 XML réels)",
)
@pytest.mark.skipif(not CACHE_REEL.is_dir(), reason=f"cache réel absent : {CACHE_REEL}")
@pytest.mark.parametrize("cas", CAS, ids=lambda c: c.model)
def test_parite_legacy_dbt_sur_cache_reel(cas: CasComparaison):
    fichiers = fichiers_du_flux(CACHE_REEL, cas.flux)
    if not fichiers:
        pytest.skip(f"aucun XML sous {CACHE_REEL / cas.flux}")
    resultat = comparer_cas(cas, fichiers)
    budget = BUDGETS_REEL[cas.model]
    assert resultat.divergences <= budget, f"{resultat} — budget {budget} dépassé (régression ?)"
