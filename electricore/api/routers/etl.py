"""Router des endpoints ETL : lancement et suivi du pipeline d'ingestion (issue #82)."""

from fastapi import APIRouter, Depends, HTTPException, Query

from electricore.api.models import ETLJobResponse, ETLRunRequest
from electricore.api.security import get_current_api_key
from electricore.api.services import etl_service

router = APIRouter(tags=["etl"])


@router.post("/etl/run", response_model=ETLJobResponse, status_code=202)
async def run_etl(
    body: ETLRunRequest,
    api_key: str = Depends(get_current_api_key),
):
    """
    Lance le pipeline d'ingestion ETL en arrière-plan.

    **Authentification requise.**

    Modes disponibles :
    - `test` — tous les flux, 2 fichiers chacun (smoke, ~3s)
    - `all` — tous les flux en production
    - `rebuild` — re-matérialise les tables depuis le brut, zéro réseau
    - `resync` — purge l'état incrémental dlt et re-télécharge tout
    - liste de flux (`r151`, `c15`, `r151 c15`…) — ingestion ciblée

    Retourne immédiatement un `job_id` pour suivre l'avancement via `GET /etl/jobs/{job_id}`.

    Codes :
    - 202 : job lancé
    - 409 : un job est déjà en cours
    - 501 : extra [etl] non installé
    """
    if not etl_service.is_etl_available():
        raise HTTPException(501, "Le pipeline ETL n'est pas disponible. Installez l'extra [etl] : uv sync --extra etl")

    mode = etl_service.valider_mode(body.mode)
    if mode is None:
        raise HTTPException(
            422,
            f"Mode invalide : '{body.mode}'. Acceptés : test, all, rebuild, resync, "
            f"ou une liste de flux parmi {', '.join(sorted(etl_service.FLUX_CONNUS))}.",
        )

    if etl_service.is_running():
        raise HTTPException(409, "Un job ETL est déjà en cours d'exécution.")

    job = await etl_service.start_job(mode)
    return ETLJobResponse(
        id=job.id,
        mode=job.mode,
        status=job.status,
        started_at=job.started_at,
        finished_at=job.finished_at,
        error=job.error,
        output=job.output,
    )


@router.get("/etl/jobs", response_model=list[ETLJobResponse])
async def list_etl_jobs(
    limit: int = Query(20, ge=1, le=50, description="Nombre de jobs à retourner"),
    api_key: str = Depends(get_current_api_key),
):
    """
    Liste les jobs ETL récents (ordre anti-chronologique).

    **Authentification requise.**
    """
    jobs = etl_service.list_jobs(limit)
    return [
        ETLJobResponse(
            id=j.id,
            mode=j.mode,
            status=j.status,
            started_at=j.started_at,
            finished_at=j.finished_at,
            error=j.error,
            output=j.output,
        )
        for j in jobs
    ]


@router.get("/etl/jobs/{job_id}", response_model=ETLJobResponse)
async def get_etl_job(
    job_id: str,
    api_key: str = Depends(get_current_api_key),
):
    """
    Retourne le statut d'un job ETL par son identifiant.

    **Authentification requise.**

    Statuts possibles : `running` | `completed` | `failed`
    """
    job = etl_service.get_job(job_id)
    if job is None:
        raise HTTPException(404, f"Job '{job_id}' introuvable.")
    return ETLJobResponse(
        id=job.id,
        mode=job.mode,
        status=job.status,
        started_at=job.started_at,
        finished_at=job.finished_at,
        error=job.error,
        output=job.output,
    )
