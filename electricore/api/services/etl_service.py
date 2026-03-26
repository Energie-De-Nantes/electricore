"""
Service ETL pour lancer et suivre le pipeline d'ingestion Enedis.
Gestion asynchrone via ThreadPoolExecutor avec job store en mémoire.
"""

import asyncio
import subprocess
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

# Racine du projet (3 niveaux au-dessus de electricore/api/services/)
_PROJECT_ROOT = Path(__file__).parents[3]


class ETLMode(str, Enum):
    test = "test"
    r151 = "r151"
    all = "all"
    reset = "reset"


class ETLStatus(str, Enum):
    running = "running"
    completed = "completed"
    failed = "failed"


# Job store in-memory (max 50 entrées, FIFO)
_jobs: OrderedDict[str, "ETLJob"] = OrderedDict()
_MAX_JOBS = 50


@dataclass
class ETLJob:
    id: str
    mode: ETLMode
    status: ETLStatus
    started_at: datetime
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    output: Optional[str] = None


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


def get_job(job_id: str) -> Optional[ETLJob]:
    return _jobs.get(job_id)


def list_jobs(limit: int = 20) -> list[ETLJob]:
    """Liste les jobs les plus récents (ordre anti-chronologique)."""
    all_jobs = list(reversed(list(_jobs.values())))
    return all_jobs[:limit]


async def start_job(mode: ETLMode) -> ETLJob:
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
    """Exécute le pipeline ETL via subprocess (isolation + import paths corrects)."""
    try:
        script = _PROJECT_ROOT / "electricore" / "etl" / "pipeline_production.py"
        result = subprocess.run(
            ["uv", "run", "--project", str(_PROJECT_ROOT), "python", str(script), job.mode.value],
            cwd=_PROJECT_ROOT / "electricore" / "etl",
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
