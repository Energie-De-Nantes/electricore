"""Contrat CLI du runner de production dbt (#134).

L'API (`/etl/run`) subprocess le runner avec un mode : chaque valeur de `ETLMode`
doit être interprétable par `pipeline_dbt`, avec la même sémantique que l'ex-runner
legacy (test = échantillon, all = tout, sélection de flux, reset = re-téléchargement
complet via drop_sources).
"""

from electricore.api.services.etl_service import ETLMode
from electricore.etl.pipeline_dbt import PlanRun, interpreter_flux


def test_les_modes_de_l_api_sont_interpretables():
    plans = {mode.value: interpreter_flux([mode.value], max_files=None) for mode in ETLMode}
    assert plans["all"] == PlanRun(selection=None, max_files=None, refresh=None)
    assert plans["test"] == PlanRun(selection=None, max_files=2, refresh=None)
    assert plans["r151"] == PlanRun(selection=["R151"], max_files=None, refresh=None)
    # reset = repartir de zéro : état incrémental dlt purgé, tout est re-téléchargé.
    assert plans["reset"] == PlanRun(selection=None, max_files=None, refresh="drop_sources")


def test_selection_multi_flux():
    plan = interpreter_flux(["r151", "c15"], max_files=None)
    assert plan.selection == ["R151", "C15"]


def test_l_api_subprocess_le_runner_dbt():
    """La bascule #134 : /etl/run lance le chemin dbt, plus le parseur legacy."""
    from electricore.api.services.etl_service import _build_pipeline_command

    commande = _build_pipeline_command("all")
    assert "electricore.etl.pipeline_dbt" in commande
    assert "electricore.etl.pipeline_production" not in commande
