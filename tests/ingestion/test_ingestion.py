"""Contrat CLI du runner de production dbt (#134).

L'API (`/ingestion/run`) subprocess le runner avec un mode : chaque valeur de `ModeIngestion`
doit être interprétable par `ingestion`, avec la même sémantique que l'ex-runner
legacy (test = échantillon, all = tout, sélection de flux, reset = re-téléchargement
complet via drop_sources).
"""

from electricore.api.services.ingestion_service import ModeIngestion
from electricore.ingestion.runner import PlanRun, flux_aveugles, interpreter_flux
from electricore.ingestion.transformers.chaine import StatsChaine


def test_flux_aveugles_ne_retient_que_les_flux_sans_document():
    """Escalade per-flux généralisée à la chaîne (ADR-0037 ext.) : seuls les flux à 0
    document produit + ≥ 1 échec (quel qu'en soit l'étage) remontent."""
    stats = {
        "R64": StatsChaine(documents=0, echecs_dechiffrement=3),  # aveugle → clé manquante
        "C15": StatsChaine(documents=10, echecs_extraction=1),  # échec isolé toléré
        "R151": StatsChaine(documents=5),  # tout va bien
        "F15": StatsChaine(documents=0),  # aucun document, aucun échec (vide par nature)
    }
    assert flux_aveugles(stats) == ["R64"]


def test_resume_chaine_rend_visibles_documents_et_echecs_par_etage():
    """Observabilité (ADR-0037 ext.) : le bilan par flux remonte le compteur de documents
    (entrée du prédicat) et les échecs par étage — capturés dans job.output pour le bot."""
    from electricore.ingestion.runner import _resume_chaine

    ligne = _resume_chaine(
        "R64",
        StatsChaine(fichiers=3, dechiffres=3, extraits=2, documents=1, echecs_extraction=1, echecs_linearisation=1),
    )
    assert "R64" in ligne
    assert "1 document" in ligne
    assert "extraction=1" in ligne and "linéarisation=1" in ligne


def test_resume_chaine_flux_propre_ne_montre_pas_de_section_echecs():
    """Un flux sans échec n'affiche aucune section d'échecs (bruit minimal)."""
    from electricore.ingestion.runner import _resume_chaine

    ligne = _resume_chaine("R151", StatsChaine(fichiers=5, dechiffres=5, extraits=5, documents=5))
    assert "échec" not in ligne.lower()


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


def test_resync_cible_purge_l_etat_du_seul_flux_selectionne():
    """`--resync` sur une liste de flux → `drop_resources` scopé au run (purge le seul
    curseur des flux sélectionnés, pour rejouer un curseur bloqué). Cas d'usage : R67
    ponctuel dont le curseur a avancé au listing sans atterrissage."""
    plan = interpreter_flux(["r67"], max_files=None, resync=True)
    assert plan.selection == ["R67"]
    assert plan.refresh == "drop_resources"


def test_sans_resync_une_liste_de_flux_ne_purge_rien():
    """Le défaut reste l'incrémental : pas de drapeau → aucun refresh."""
    assert interpreter_flux(["r67"], max_files=None).refresh is None


def test_resync_cible_ne_se_confond_pas_avec_le_mode_resync_global():
    """Deux purges distinctes : le MODE `resync` rejoue TOUS les flux (`drop_sources`) ;
    le DRAPEAU `--resync` sur une liste rejoue les SEULS flux visés (`drop_resources`)."""
    assert interpreter_flux(["resync"], max_files=None).refresh == "drop_sources"
    assert interpreter_flux(["r67"], max_files=None, resync=True).refresh == "drop_resources"


def test_la_base_par_defaut_honore_duckdb_path(monkeypatch):
    """En conteneur, DUCKDB__PATH pointe le volume de données (docker-compose) :
    le runner doit l'honorer, comme l'API et les loaders."""
    from electricore.ingestion.runner import chemin_db_defaut

    monkeypatch.setenv("DUCKDB__PATH", "/data/flux_enedis_pipeline.duckdb")
    assert str(chemin_db_defaut()) == "/data/flux_enedis_pipeline.duckdb"
    monkeypatch.delenv("DUCKDB__PATH")
    assert chemin_db_defaut().name == "flux_enedis_pipeline.duckdb"


def test_la_base_par_defaut_honore_le_dotenv(tmp_path, monkeypatch):
    """Même résolution que l'API et les loaders core (issue #146) : un DUCKDB__PATH
    porté par le .env vaut pour le runner aussi, pas seulement pour l'API."""
    from pathlib import Path

    from electricore.config import runtime
    from electricore.ingestion.runner import chemin_db_defaut

    monkeypatch.delenv("DUCKDB__PATH", raising=False)
    fichier_env = tmp_path / ".env"
    fichier_env.write_text("DUCKDB__PATH=/data/depuis_dotenv.duckdb\n")
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


def test_main_echoue_si_un_flux_est_aveugle(monkeypatch):
    """Escalade per-flux bout-en-bout (ADR-0037, #353) : un flux à 0 déchiffrement réussi
    fait sortir `main` en code ≠ 0 → l'API marque le job `failed` → la surveillance bot
    alerte (chaîne existante, aucun nouveau code bot)."""
    import sys

    import pytest

    from electricore.ingestion import runner
    from electricore.ingestion.transformers.chaine import StatsChaine

    monkeypatch.setattr(runner.runtime, "valider", lambda *a, **k: None)
    monkeypatch.setattr(
        runner, "lander_brut", lambda db, plan: {"R64": StatsChaine(documents=0, echecs_dechiffrement=3)}
    )
    monkeypatch.setattr(runner, "construire_dbt", lambda db: True)  # les flux sains ont buildé
    monkeypatch.setattr(runner, "bilan", lambda db: None)
    monkeypatch.setattr(sys, "argv", ["prog", "all", "--db", "/tmp/peu_importe.duckdb"])

    with pytest.raises(SystemExit) as exc:
        runner.main()
    assert exc.value.code == 1
