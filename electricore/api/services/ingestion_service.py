"""
Service d'ingestion : lancement et suivi des jobs (flux Enedis → DuckDB).
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
    Construit la commande subprocess pour lancer le pipeline d'ingestion.

    - Si `uv` est disponible (env. dev local), utilise `uv run` pour garantir
      la résolution des dépendances depuis le lockfile.
    - Sinon (env. Docker, ou venv déjà activée), appelle directement l'interpréteur
      Python courant via `sys.executable` — les deps sont déjà installées dans /app/.venv.
    """
    # Une liste de flux ("r151 c15") devient plusieurs arguments CLI (nargs="+").
    module_args = ["-m", "electricore.ingestion", *mode.split()]
    if shutil.which("uv"):
        return ["uv", "run", "--project", str(_PROJECT_ROOT), "python", *module_args]
    return [sys.executable, *module_args]


class ModeIngestion(StrEnum):
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

    Accepte un membre d'`ModeIngestion` ou une liste de flux connus (`r151 c15`).
    """
    tokens = mode.lower().split()
    if not tokens:
        return None
    normalise = " ".join(tokens)
    if normalise in ModeIngestion or all(t in FLUX_CONNUS for t in tokens):
        return normalise
    return None


class StatutIngestion(StrEnum):
    running = "running"
    completed = "completed"
    failed = "failed"


# Job store in-memory (max 50 entrées, FIFO)
_jobs: OrderedDict[str, "JobIngestion"] = OrderedDict()
_MAX_JOBS = 50


@dataclass(slots=True)
class JobIngestion:
    id: str
    mode: str  # membre d'ModeIngestion ou liste de flux normalisée (cf. valider_mode)
    status: StatutIngestion
    started_at: datetime
    finished_at: datetime | None = None
    error: str | None = None
    output: str | None = None


def is_ingestion_available() -> bool:
    """Vérifie que l'extra [ingestion] est installé (dlt disponible)."""
    try:
        import dlt  # noqa: F401

        return True
    except ImportError:
        return False


def is_running() -> bool:
    """Retourne True si un job d'ingestion est actuellement en cours."""
    return any(j.status == StatutIngestion.running for j in _jobs.values())


def get_job(job_id: str) -> JobIngestion | None:
    return _jobs.get(job_id)


def list_jobs(limit: int = 20) -> list[JobIngestion]:
    """Liste les jobs les plus récents (ordre anti-chronologique)."""
    all_jobs = list(reversed(list(_jobs.values())))
    return all_jobs[:limit]


async def start_job(mode: str) -> JobIngestion:
    """
    Lance le pipeline d'ingestion en arrière-plan et retourne le job immédiatement.
    """
    job = JobIngestion(
        id=str(uuid.uuid4()),
        mode=mode,
        status=StatutIngestion.running,
        started_at=datetime.now(),
    )

    # Nettoyage FIFO si nécessaire
    while len(_jobs) >= _MAX_JOBS:
        _jobs.popitem(last=False)

    _jobs[job.id] = job

    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _run_pipeline, job)

    return job


def _tail(texte: str, lignes: int = 40) -> str:
    """Dernières `lignes` lignes non vides : la cause utile d'un échec dbt/dlt est en fin
    de sortie. Garde l'`error` lisible sans y noyer tout le log (qui vit dans `output`)."""
    return "\n".join(texte.strip().splitlines()[-lignes:])


def _run_pipeline(job: JobIngestion) -> None:
    """Exécute le pipeline d'ingestion via subprocess (isolation des dépendances DLT)."""
    try:
        result = subprocess.run(
            _build_pipeline_command(job.mode),
            capture_output=True,
            text=True,
        )
        # dlt/dbt loguent leurs diagnostics sur stdout → capturer la sortie dans TOUS les
        # cas (succès comme échec), sinon la vraie cause d'un échec est jetée (#298).
        job.output = result.stdout.strip() or None
        if result.returncode != 0:
            # stderr est souvent vide (logs sur stdout) → reporter le tail de stdout pour
            # une `error` lisible, pas un « exit code 1 » nu.
            raise RuntimeError(result.stderr.strip() or _tail(result.stdout) or f"exit code {result.returncode}")
        job.status = StatutIngestion.completed
    except Exception as exc:
        job.status = StatutIngestion.failed
        job.error = str(exc)
    finally:
        job.finished_at = datetime.now()
