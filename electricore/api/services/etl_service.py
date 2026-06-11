"""
Service ETL pour lancer et suivre le pipeline d'ingestion Enedis.
Gestion asynchrone via ThreadPoolExecutor avec job store en mémoire.
"""

import asyncio
import shutil
import subprocess
import sys
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path

# Racine du projet (3 niveaux au-dessus de electricore/api/services/)
_PROJECT_ROOT = Path(__file__).parents[3]


def _build_pipeline_command(mode: str) -> list[str]:
    """
    Construit la commande subprocess pour lancer le pipeline ETL.

    - Si `uv` est disponible (env. dev local), utilise `uv run` pour garantir
      la résolution des dépendances depuis le lockfile.
    - Sinon (env. Docker, ou venv déjà activée), appelle directement l'interpréteur
      Python courant via `sys.executable` — les deps sont déjà installées dans /app/.venv.
    """
    # Une liste de flux ("r151 c15") devient plusieurs arguments CLI (nargs="+").
    module_args = ["-m", "electricore.etl.pipeline_dbt", *mode.split()]
    if shutil.which("uv"):
        return ["uv", "run", "--project", str(_PROJECT_ROOT), "python", *module_args]
    return [sys.executable, *module_args]


class ETLMode(StrEnum):
    test = "test"
    r151 = "r151"
    all = "all"
    rebuild = "rebuild"  # re-matérialise depuis le brut, zéro réseau (#140)
    resync = "resync"  # purge l'état incrémental + re-télécharge tout (brut perdu)
    reset = "reset"  # déprécié : alias de resync


# Clés de flux.yaml, en minuscules — le runner accepte aussi une liste de flux (#152).
FLUX_CONNUS = frozenset({"c15", "f12", "f15", "r15", "r151", "r64"})


def valider_mode(mode: str) -> str | None:
    """Mode runner valide et normalisé (minuscules, espaces simples) — ou None.

    Accepte un membre d'`ETLMode` ou une liste de flux connus (`r151 c15`).
    """
    tokens = mode.lower().split()
    if not tokens:
        return None
    normalise = " ".join(tokens)
    if normalise in ETLMode or all(t in FLUX_CONNUS for t in tokens):
        return normalise
    return None


class ETLStatus(StrEnum):
    running = "running"
    completed = "completed"
    failed = "failed"


# Job store in-memory (max 50 entrées, FIFO)
_jobs: OrderedDict[str, "ETLJob"] = OrderedDict()
_MAX_JOBS = 50


@dataclass(slots=True)
class ETLJob:
    id: str
    mode: str  # membre d'ETLMode ou liste de flux normalisée (cf. valider_mode)
    status: ETLStatus
    started_at: datetime
    finished_at: datetime | None = None
    error: str | None = None
    output: str | None = None


def is_etl_available() -> bool:
    """Vérifie que l'extra [etl] est installé (dlt disponible)."""
    try:
        import dlt  # noqa: F401

        return True
    except ImportError:
        return False


def is_running() -> bool:
    """Retourne True si un job ETL est actuellement en cours."""
    return any(j.status == ETLStatus.running for j in _jobs.values())


def get_job(job_id: str) -> ETLJob | None:
    return _jobs.get(job_id)


def list_jobs(limit: int = 20) -> list[ETLJob]:
    """Liste les jobs les plus récents (ordre anti-chronologique)."""
    all_jobs = list(reversed(list(_jobs.values())))
    return all_jobs[:limit]


async def start_job(mode: str) -> ETLJob:
    """
    Lance le pipeline ETL en arrière-plan et retourne le job immédiatement.
    """
    job = ETLJob(
        id=str(uuid.uuid4()),
        mode=mode,
        status=ETLStatus.running,
        started_at=datetime.now(),
    )

    # Nettoyage FIFO si nécessaire
    while len(_jobs) >= _MAX_JOBS:
        _jobs.popitem(last=False)

    _jobs[job.id] = job

    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _run_pipeline, job)

    return job


def _run_pipeline(job: ETLJob) -> None:
    """Exécute le pipeline ETL via subprocess (isolation des dépendances DLT)."""
    try:
        result = subprocess.run(
            _build_pipeline_command(job.mode),
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or f"exit code {result.returncode}")
        job.status = ETLStatus.completed
        job.output = result.stdout.strip() or None
    except Exception as exc:
        job.status = ETLStatus.failed
        job.error = str(exc)
    finally:
        job.finished_at = datetime.now()
