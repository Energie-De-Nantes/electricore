"""Contrat CLI du runner de production dbt (#134).

L'API (`/ingestion/run`) subprocess le runner avec un mode : chaque valeur de `ModeIngestion`
doit être interprétable par `ingestion`, avec la même sémantique que l'ex-runner
legacy (test = échantillon, all = tout, sélection de flux, reset = re-téléchargement
complet via drop_sources).
"""

from electricore.api.services.ingestion_service import ModeIngestion
from electricore.ingestion.runner import PlanRun, interpreter_flux


def test_les_modes_de_l_api_sont_interpretables():
    plans = {mode.value: interpreter_flux([mode.value], max_files=None) for mode in ModeIngestion}
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
    from electricore.ingestion.runner import chemin_db_defaut

    monkeypatch.setenv("DUCKDB_PATH", "/data/flux_enedis_pipeline.duckdb")
    assert str(chemin_db_defaut()) == "/data/flux_enedis_pipeline.duckdb"
    monkeypatch.delenv("DUCKDB_PATH")
    assert chemin_db_defaut().name == "flux_enedis_pipeline.duckdb"


def test_la_base_par_defaut_honore_le_dotenv(tmp_path, monkeypatch):
    """Même résolution que l'API et les loaders core (issue #146) : un DUCKDB_PATH
    porté par le .env vaut pour le runner aussi, pas seulement pour l'API."""
    from pathlib import Path

    from electricore.config import runtime
    from electricore.ingestion.runner import chemin_db_defaut

    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    fichier_env = tmp_path / ".env"
    fichier_env.write_text("DUCKDB_PATH=/data/depuis_dotenv.duckdb\n")
    monkeypatch.setattr(runtime, "FICHIER_ENV", fichier_env)
    runtime.vider_cache()

    assert chemin_db_defaut() == Path("/data/depuis_dotenv.duckdb")


def test_l_api_subprocess_le_runner_dbt():
    """La bascule #134 : /ingestion/run lance le chemin dbt, plus le parseur legacy."""
    from electricore.api.services.ingestion_service import _build_pipeline_command

    commande = _build_pipeline_command("all")
    assert "electricore.ingestion" in commande
    assert "electricore.ingestion.pipeline_production" not in commande


def test_job_en_echec_expose_la_sortie_reelle(monkeypatch):
    """dlt/dbt écrivent leurs diagnostics sur stdout : un job en échec doit exposer cette
    sortie — `error` lisible ET `output` capturé — pas un « exit code 1 » nu (#298)."""
    import subprocess
    from datetime import datetime

    from electricore.api.services import ingestion_service
    from electricore.api.services.ingestion_service import (
        JobIngestion,
        StatutIngestion,
        _run_pipeline,
    )

    erreur_dbt = "Failure in test not_null_flux_affaires_statut — Got 45 results"
    monkeypatch.setattr(
        ingestion_service.subprocess,
        "run",
        lambda *a, **k: subprocess.CompletedProcess(args=a, returncode=1, stdout=f"{erreur_dbt}\n", stderr=""),
    )

    job = JobIngestion(id="j1", mode="test", status=StatutIngestion.running, started_at=datetime.now())
    _run_pipeline(job)

    assert job.status == StatutIngestion.failed
    assert erreur_dbt in (job.error or ""), "l'erreur doit porter la vraie cause (stdout), pas « exit code 1 »"
    assert erreur_dbt in (job.output or ""), "stdout doit être capturé même en échec"
