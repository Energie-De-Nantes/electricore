"""Router des endpoints d'ingestion : lancement et suivi du pipeline d'ingestion (issue #82)."""

from fastapi import APIRouter, Depends, HTTPException, Query

from electricore.api.models import IngestionJobResponse, IngestionRunRequest
from electricore.api.security import get_current_api_key
from electricore.api.services import ingestion_service

router = APIRouter(tags=["ingestion"])


@router.post("/ingestion/run", response_model=IngestionJobResponse, status_code=202)
async def run_ingestion(
    body: IngestionRunRequest,
    api_key: str = Depends(get_current_api_key),
):
    """
    Lance le l'ingestion en arrière-plan.

    **Authentification requise.**

    Modes disponibles :
    - `test` — tous les flux, 2 fichiers chacun (smoke, ~3s)
    - `all` — tous les flux en production
    - `rebuild` — re-matérialise les tables depuis le brut, zéro réseau
    - `resync` — purge l'état incrémental dlt et re-télécharge tout
    - liste de flux (`r151`, `c15`, `r151 c15`…) — ingestion ciblée

    Retourne immédiatement un `job_id` pour suivre l'avancement via `GET /ingestion/jobs/{job_id}`.

    Codes :
    - 202 : job lancé
    - 409 : un job est déjà en cours
    - 501 : extra [ingestion] non installé
    """
    if not ingestion_service.is_ingestion_available():
        raise HTTPException(
            501, "L'ingestion n'est pas disponible. Installez l'extra [ingestion] : uv sync --extra ingestion"
        )

    mode = ingestion_service.valider_mode(body.mode)
    if mode is None:
        raise HTTPException(
            422,
            f"Mode invalide : '{body.mode}'. Acceptés : test, all, rebuild, resync, "
            f"ou une liste de flux parmi {', '.join(sorted(ingestion_service.FLUX_CONNUS))}.",
        )

    if ingestion_service.is_running():
        raise HTTPException(409, "Un job d'ingestion est déjà en cours d'exécution.")

    job = await ingestion_service.start_job(mode)
    return IngestionJobResponse.model_validate(job)


@router.get("/ingestion/jobs", response_model=list[IngestionJobResponse])
async def list_ingestion_jobs(
    limit: int = Query(20, ge=1, le=50, description="Nombre de jobs à retourner"),
    api_key: str = Depends(get_current_api_key),
):
    """
    Liste les jobs d'ingestion récents (ordre anti-chronologique).

    **Authentification requise.**
    """
    return [IngestionJobResponse.model_validate(j) for j in ingestion_service.list_jobs(limit)]


@router.get("/ingestion/jobs/{job_id}", response_model=IngestionJobResponse)
async def get_ingestion_job(
    job_id: str,
    api_key: str = Depends(get_current_api_key),
):
    """
    Retourne le statut d'un job d'ingestion par son identifiant.

    **Authentification requise.**

    Statuts possibles : `running` | `completed` | `failed`
    """
    job = ingestion_service.get_job(job_id)
    if job is None:
        raise HTTPException(404, f"Job '{job_id}' introuvable.")
    return IngestionJobResponse.model_validate(job)
