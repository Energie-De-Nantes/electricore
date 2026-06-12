"""Contrat CLI du runner de production dbt (#134).

L'API (`/etl/run`) subprocess le runner avec un mode : chaque valeur de `ETLMode`
doit être interprétable par `ingestion`, avec la même sémantique que l'ex-runner
legacy (test = échantillon, all = tout, sélection de flux, reset = re-téléchargement
complet via drop_sources).
"""

from electricore.api.services.etl_service import ETLMode
from electricore.etl.ingestion import PlanRun, interpreter_flux


def test_les_modes_de_l_api_sont_interpretables():
    plans = {mode.value: interpreter_flux([mode.value], max_files=None) for mode in ETLMode}
    assert plans["all"] == PlanRun(selection=None, max_files=None, refresh=None, rebuild=False)
    assert plans["test"] == PlanRun(selection=None, max_files=2, refresh=None, rebuild=False)
    assert plans["r151"] == PlanRun(selection=["R151"], max_files=None, refresh=None, rebuild=False)
    # rebuild = re-matérialiser depuis le brut, zéro réseau (~13 s) — le geste
    # standard après un changement de modèle (#140).
    assert plans["rebuild"] == PlanRun(selection=None, max_files=None, refresh=None, rebuild=True)
    # resync = repartir de zéro : état incrémental dlt purgé, tout re-téléchargé
    # (exceptionnel : brut perdu/corrompu).
    assert plans["resync"] == PlanRun(selection=None, max_files=None, refresh="drop_sources", rebuild=False)


def test_reset_est_un_alias_dépréciée_de_resync():
    assert interpreter_flux(["reset"], max_files=None) == interpreter_flux(["resync"], max_files=None)


def test_selection_multi_flux():
    plan = interpreter_flux(["r151", "c15"], max_files=None)
    assert plan.selection == ["R151", "C15"]


def test_la_base_par_defaut_honore_duckdb_path(monkeypatch):
    """En conteneur, DUCKDB_PATH pointe le volume de données (docker-compose) :
    le runner doit l'honorer, comme l'API et les loaders."""
    from electricore.etl.ingestion import chemin_db_defaut

    monkeypatch.setenv("DUCKDB_PATH", "/data/flux_enedis_pipeline.duckdb")
    assert str(chemin_db_defaut()) == "/data/flux_enedis_pipeline.duckdb"
    monkeypatch.delenv("DUCKDB_PATH")
    assert chemin_db_defaut().name == "flux_enedis_pipeline.duckdb"


def test_la_base_par_defaut_honore_le_dotenv(tmp_path, monkeypatch):
    """Même résolution que l'API et les loaders core (issue #146) : un DUCKDB_PATH
    porté par le .env vaut pour le runner aussi, pas seulement pour l'API."""
    from pathlib import Path

    from electricore.etl.ingestion import chemin_db_defaut

    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("DUCKDB_PATH=/data/depuis_dotenv.duckdb\n")

    assert chemin_db_defaut() == Path("/data/depuis_dotenv.duckdb")


def test_l_api_subprocess_le_runner_dbt():
    """La bascule #134 : /etl/run lance le chemin dbt, plus le parseur legacy."""
    from electricore.api.services.etl_service import _build_pipeline_command

    commande = _build_pipeline_command("all")
    assert "electricore.etl.ingestion" in commande
    assert "electricore.etl.pipeline_production" not in commande
