"""
API REST sécurisée pour ElectriCore.
Expose les données Enedis via endpoints génériques avec authentification par clé API.
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.responses import Response
from typing import Optional

from electricore.api.services import duckdb_service, etl_service
from electricore.api.services.taxes_service import generer_accise_xlsx, generer_cta_xlsx
from electricore.api.services.facturation_service import generer_facturation_xlsx, generer_documents_facturation
from electricore.api.config import settings
from electricore.api.models import ETLRunRequest, ETLJobResponse
from electricore.api.security import get_current_api_key, get_api_key_info, APIKeyInfo

logger = logging.getLogger(__name__)

_tg_app = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _tg_app
    if settings.telegram_bot_token:
        from electricore.bot.bot import build_application
        _tg_app = build_application(settings.telegram_bot_token)
        await _tg_app.initialize()
        await _tg_app.start()
        await _tg_app.updater.start_polling()
        logger.info("Bot Telegram démarré.")
    yield
    if _tg_app is not None:
        from electricore.bot.bot import _background_tasks
        for task in list(_background_tasks):
            task.cancel()
        if _background_tasks:
            await asyncio.gather(*_background_tasks, return_exceptions=True)
        await _tg_app.updater.stop()
        await _tg_app.stop()
        await _tg_app.shutdown()
        logger.info("Bot Telegram arrêté.")


# Configuration de l'application avec métadonnées de sécurité
app = FastAPI(
    lifespan=lifespan,
    title=settings.api_title,
    version=settings.api_version,
    description=f"{settings.api_description}\n\n"
                "**Authentification requise** : Utilisez une clé API valide via :\n"
                "- Header : `X-API-Key: votre_cle_api`\n\n"
                "Endpoints publics (sans authentification) : /, /health, /docs",
    openapi_tags=[
        {
            "name": "public",
            "description": "Endpoints publics (sans authentification)"
        },
        {
            "name": "flux",
            "description": "Accès aux données flux Enedis (authentification requise)"
        },
        {
            "name": "etl",
            "description": "Lancement et suivi du pipeline d'ingestion (authentification requise)"
        },
        {
            "name": "admin",
            "description": "Endpoints d'administration (authentification requise)"
        },
        {
            "name": "taxes",
            "description": "Calcul des taxes énergétiques CTA et Accise TICFE (authentification requise)"
        },
        {
            "name": "facturation",
            "description": "Réconciliation facturation Odoo ↔ Enedis (authentification requise)"
        }
    ]
)



@app.get("/", tags=["public"])
async def root():
    """
    Page d'accueil de l'API avec informations générales.

    Endpoint public - aucune authentification requise.
    Liste les tables disponibles et montre des exemples d'utilisation.
    """
    try:
        tables = duckdb_service.list_tables()
        return {
            "message": "ElectriCore API - Données flux Enedis sécurisées",
            "version": settings.api_version,
            "authentication": {
                "required": "Clé API requise pour accéder aux données",
                "method": "X-API-Key: votre_cle_api (header uniquement)"
            },
            "available_tables": tables,
            "examples": {
                "get_flux_data": "curl -H 'X-API-Key: VOTRE_CLE' '/flux/r151?limit=10'",
                "filter_by_prm": "curl -H 'X-API-Key: VOTRE_CLE' '/flux/c15?prm=12345678901234'",
                "table_info": "curl -H 'X-API-Key: VOTRE_CLE' '/flux/r64/info'",
                "pagination": "curl -H 'X-API-Key: VOTRE_CLE' '/flux/r151?limit=50&offset=100'"
            },
            "docs": "/docs"
        }
    except Exception as e:
        raise HTTPException(500, f"Erreur lors de l'accès à la base de données: {e}")


@app.get("/flux/c15/entrees/xlsx", tags=["flux"])
async def get_entrees_xlsx(
    limit: int = Query(10000, le=100000, description="Nombre maximum de lignes"),
    api_key: str = Depends(get_current_api_key),
):
    """
    Exporte les **entrées** C15 au format XLSX (PMES, MES, CFNE).

    **Authentification requise.**
    """
    try:
        content = duckdb_service.query_entrees_xlsx(limit)
    except Exception as e:
        raise HTTPException(500, f"Erreur lors de la génération du fichier XLSX: {e}")
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=entrees_c15.xlsx"},
    )


@app.get("/flux/c15/sorties/xlsx", tags=["flux"])
async def get_sorties_xlsx(
    limit: int = Query(10000, le=100000, description="Nombre maximum de lignes"),
    api_key: str = Depends(get_current_api_key),
):
    """
    Exporte les **sorties** C15 au format XLSX (RES, CFNS).

    **Authentification requise.**
    """
    try:
        content = duckdb_service.query_sorties_xlsx(limit)
    except Exception as e:
        raise HTTPException(500, f"Erreur lors de la génération du fichier XLSX: {e}")
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=sorties_c15.xlsx"},
    )


@app.get("/flux/{table_name}", tags=["flux"])
async def get_flux(
    table_name: str,
    prm: Optional[str] = Query(None, description="Filtrer par pdl (Point de Livraison)"),
    limit: int = Query(100, le=1000, description="Nombre maximum de lignes à retourner"),
    offset: int = Query(0, ge=0, description="Nombre de lignes à ignorer (pagination)"),
    api_key: str = Depends(get_current_api_key)
):
    """
    Endpoint générique pour lire n'importe quel flux Enedis.

    **Authentification requise** - Utilisez votre clé API.

    Exemples:
    - /flux/r151 : Relevés quotidiens
    - /flux/c15 : Changements contractuels
    - /flux/r64 : Relevés demandés sur SGE
    - /flux/f15_detail : Facturation Enedis détaillée

    Args:
        table_name: Nom de la table flux (r151, c15, r64, etc.)
        prm: Filtre optionnel par Point de Livraison
        limit: Nombre max de lignes (max 1000)
        offset: Pagination - lignes à ignorer
        api_key: Clé API (automatiquement validée)

    Returns:
        Dict contenant les données filtrées et métadonnées de pagination
    """
    # Vérifier que la table existe
    try:
        available_tables = duckdb_service.list_tables()
    except Exception as e:
        raise HTTPException(500, f"Impossible d'accéder à la base de données: {e}")
        
    if table_name not in available_tables:
        raise HTTPException(
            404, 
            f"Table '{table_name}' non trouvée. Tables disponibles: {available_tables}"
        )
    
    # Construire les filtres
    filters = {}
    if prm:
        # Toutes les tables utilisent 'pdl' pour l'identifiant PRM
        filters["pdl"] = prm
    
    # Récupérer les données
    try:
        data = duckdb_service.query_table(table_name, filters, limit, offset)
        
        return {
            "table": f"flux_{table_name}",
            "filters": filters if filters else None,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "returned": len(data)
            },
            "data": data
        }
    except Exception as e:
        raise HTTPException(500, f"Erreur lors de la lecture des données: {e}")


@app.get("/flux/{table_name}/xlsx", tags=["flux"])
async def get_flux_xlsx(
    table_name: str,
    prm: Optional[str] = Query(None, description="Filtrer par pdl (Point de Livraison)"),
    limit: int = Query(10000, le=100000, description="Nombre maximum de lignes"),
    api_key: str = Depends(get_current_api_key),
):
    """
    Exporte les données d'un flux Enedis au format XLSX (Excel).

    **Authentification requise.**

    Retourne un fichier téléchargeable directement ouvrables dans Excel/LibreOffice.
    Limite par défaut : 10 000 lignes (max 100 000).
    """
    try:
        available_tables = duckdb_service.list_tables()
    except Exception as e:
        raise HTTPException(500, f"Impossible d'accéder à la base de données: {e}")

    if table_name not in available_tables:
        raise HTTPException(
            404,
            f"Table '{table_name}' non trouvée. Tables disponibles: {available_tables}"
        )

    filters = {"pdl": prm} if prm else {}

    try:
        content = duckdb_service.query_table_xlsx(table_name, filters, limit)
    except Exception as e:
        raise HTTPException(500, f"Erreur lors de la génération du fichier XLSX: {e}")

    filename = f"flux_{table_name}.xlsx"
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/flux/{table_name}/info", tags=["flux"])
async def get_table_info(
    table_name: str,
    api_key: str = Depends(get_current_api_key)
):
    """
    Retourne les métadonnées d'une table (colonnes, types, nombre de lignes).

    **Authentification requise** - Utilisez votre clé API.

    Utile pour comprendre la structure des données avant de faire des requêtes.

    Args:
        table_name: Nom de la table flux (r151, c15, r64, etc.)
        api_key: Clé API (automatiquement validée)

    Returns:
        Dict avec les métadonnées de la table (colonnes, types, nombre de lignes)
    """
    try:
        return duckdb_service.get_table_info(table_name)
    except Exception as e:
        available_tables = duckdb_service.list_tables()
        raise HTTPException(
            404, 
            f"Table '{table_name}' non trouvée. Tables disponibles: {available_tables}"
        )


@app.get("/health", tags=["public"])
async def health():
    """
    Endpoint de vérification de santé de l'API.

    Endpoint public - aucune authentification requise.
    Vérifie que l'API et la base de données sont accessibles.

    Returns:
        Dict avec le statut de l'API et des composants critiques
    """
    try:
        # Test de connexion à la base
        tables = duckdb_service.list_tables()
        return {
            "status": "ok",
            "api_version": settings.api_version,
            "database": "accessible",
            "tables_count": len(tables),
            "authentication": {
                "api_keys_configured": len(settings.get_valid_api_keys()) > 0,
                "method": "X-API-Key header"
            }
        }
    except Exception as e:
        raise HTTPException(500, f"Base de données inaccessible: {e}")


@app.post("/etl/run", tags=["etl"], response_model=ETLJobResponse, status_code=202)
async def run_etl(
    body: ETLRunRequest,
    api_key: str = Depends(get_current_api_key),
):
    """
    Lance le pipeline d'ingestion ETL en arrière-plan.

    **Authentification requise.**

    Modes disponibles :
    - `test` — 2 fichiers par flux (~3s), dataset `flux_enedis_test`
    - `r151` — R151 complet, dataset `flux_enedis_r151`
    - `all` — Tous les flux en production, dataset `flux_enedis`
    - `reset` — Reset complet (supprime l'état incrémental), dataset `flux_enedis`

    Retourne immédiatement un `job_id` pour suivre l'avancement via `GET /etl/jobs/{job_id}`.

    Codes :
    - 202 : job lancé
    - 409 : un job est déjà en cours
    - 501 : extra [etl] non installé
    """
    if not etl_service.is_etl_available():
        raise HTTPException(
            501,
            "Le pipeline ETL n'est pas disponible. Installez l'extra [etl] : uv sync --extra etl"
        )

    try:
        mode = etl_service.ETLMode(body.mode)
    except ValueError:
        raise HTTPException(
            422,
            f"Mode invalide : '{body.mode}'. Valeurs acceptées : test, r151, all, reset"
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


@app.get("/etl/jobs", tags=["etl"], response_model=list[ETLJobResponse])
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


@app.get("/etl/jobs/{job_id}", tags=["etl"], response_model=ETLJobResponse)
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


@app.get("/admin/api-keys", tags=["admin"])
async def list_api_keys(
    api_key: str = Depends(get_current_api_key),
    key_info: APIKeyInfo = Depends(get_api_key_info)
):
    """
    Informations sur la configuration des clés API.

    **Authentification requise** - Endpoint d'administration.

    Returns:
        Dict avec les informations sur les clés API configurées
    """
    return {
        "message": "Configuration des clés API",
        "current_key": {
            "preview": key_info.key_preview,
            "source": key_info.source
        },
        "configuration": {
            "total_keys": len(settings.get_valid_api_keys()),
            "method": "X-API-Key header",
            "public_endpoints": settings.public_endpoints
        }
    }


_XLSX_MEDIA = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


@app.get("/taxes/accise/xlsx", tags=["taxes"])
async def export_accise_xlsx(
    trimestre: Optional[str] = Query(
        default=None,
        examples=["2025-T1"],
        description="Filtre par trimestre au format YYYY-TX. Sans filtre : toutes les données.",
    ),
    api_key: str = Depends(get_current_api_key),
):
    """
    Calcule l'Accise TICFE et retourne un fichier XLSX.

    **Authentification requise. Nécessite la configuration Odoo (ODOO_* dans .env).**

    Le fichier contient 3 onglets :
    - **Résumé** : totaux (PDL, énergie MWh, accise €)
    - **Par taux** : répartition par taux réglementaire
    - **Détail** : détail ligne par ligne (PDL, mois, énergie, accise)
    """
    if not settings.is_odoo_configured:
        raise HTTPException(501, f"Odoo [{settings.odoo_env}] non configuré. Définissez ODOO_{settings.odoo_env.upper()}_URL/DB/USERNAME/PASSWORD dans .env")
    try:
        xlsx = generer_accise_xlsx(trimestre)
    except Exception as e:
        raise HTTPException(503, f"Erreur lors du calcul de l'accise : {e}")
    suffix = f"_{trimestre}" if trimestre else ""
    return Response(
        content=xlsx,
        media_type=_XLSX_MEDIA,
        headers={"Content-Disposition": f"attachment; filename=accise{suffix}.xlsx"},
    )


@app.get("/taxes/cta/xlsx", tags=["taxes"])
async def export_cta_xlsx(
    trimestre: Optional[str] = Query(
        default=None,
        examples=["2025-T1"],
        description="Filtre par trimestre au format YYYY-TX. Sans filtre : toutes les données.",
    ),
    api_key: str = Depends(get_current_api_key),
):
    """
    Calcule la CTA (Contribution Tarifaire d'Acheminement) et retourne un fichier XLSX.

    **Authentification requise. Nécessite Odoo (ODOO_*) et DuckDB configurés.**

    Les taux CTA sont chargés depuis electricore/config/cta_rules.csv et
    appliqués au niveau mensuel. Un changement de taux en cours de trimestre
    est géré automatiquement.

    Le fichier contient 3 onglets :
    - **Résumé** : totaux par trimestre (PDL, TURPE fixe €, CTA €)
    - **Par taux** : détail par (trimestre, taux CTA)
    - **Détail** : détail par PDL (pdl, order_name, turpe_fixe_total, cta, taux_cta_appliques)
    """
    if not settings.is_odoo_configured:
        raise HTTPException(501, f"Odoo [{settings.odoo_env}] non configuré. Définissez ODOO_{settings.odoo_env.upper()}_URL/DB/USERNAME/PASSWORD dans .env")
    try:
        xlsx = generer_cta_xlsx(trimestre)
    except Exception as e:
        raise HTTPException(503, f"Erreur lors du calcul de la CTA : {e}")
    suffix = f"_{trimestre}" if trimestre else ""
    return Response(
        content=xlsx,
        media_type=_XLSX_MEDIA,
        headers={"Content-Disposition": f"attachment; filename=cta{suffix}.xlsx"},
    )


@app.get("/facturation/xlsx", tags=["facturation"])
async def export_facturation_xlsx(
    mois: Optional[str] = Query(
        default=None,
        examples=["2025-01-01"],
        description="Mois au format YYYY-MM-DD (défaut : dernier mois disponible dans les données)",
    ),
    api_key: str = Depends(get_current_api_key),
):
    """
    Réconciliation des lignes Odoo à facturer avec les données Enedis.

    **Authentification requise. Nécessite Odoo (ODOO_*) et DuckDB configurés.**

    Le fichier contient 2 onglets :
    - **Lignes fusionnées** : toutes les lignes (`updates_rsc`) avec `quantite_enedis` calculée
    - **Changements puissance** : lignes où `memo_puissance` est renseigné
    """
    if not settings.is_odoo_configured:
        raise HTTPException(501, f"Odoo [{settings.odoo_env}] non configuré. Définissez ODOO_{settings.odoo_env.upper()}_URL/DB/USERNAME/PASSWORD dans .env")
    try:
        xlsx = await asyncio.get_event_loop().run_in_executor(
            None, generer_facturation_xlsx, mois
        )
    except Exception as e:
        logger.exception("Erreur facturation/xlsx")
        raise HTTPException(503, f"Erreur lors de la réconciliation facturation : {e}")
    suffix = f"_{mois}" if mois else ""
    return Response(
        content=xlsx,
        media_type=_XLSX_MEDIA,
        headers={"Content-Disposition": f"attachment; filename=facturation{suffix}.xlsx"},
    )


@app.get("/facturation/documents", tags=["facturation"])
async def export_facturation_documents(
    mois: Optional[str] = Query(
        default=None,
        examples=["2025-01-01"],
        description="Mois au format YYYY-MM-DD (défaut : dernier mois disponible dans les données)",
    ),
    api_key: str = Depends(get_current_api_key),
):
    """
    Export ZIP de tous les documents utiles pour la facturation.

    **Authentification requise. Nécessite Odoo (ODOO_*) et DuckDB configurés.**

    Le ZIP contient :
    - **f15_complet.csv** : flux F15 du mois
    - **f15_prestas.csv** : F15 filtré sur les prestations (unite = 'UNITE')
    - **c15_complet.csv** : flux C15 du mois
    - **c15_sorties.csv** : C15 filtré sur les sorties (RES + CFNS)
    - **reconciliation.csv** : réconciliation Odoo ↔ Enedis
    - **changements_puissance.csv** : lignes avec changement de puissance
    """
    if not settings.is_odoo_configured:
        raise HTTPException(501, f"Odoo [{settings.odoo_env}] non configuré. Définissez ODOO_{settings.odoo_env.upper()}_URL/DB/USERNAME/PASSWORD dans .env")
    try:
        zip_bytes, suffix = await asyncio.get_event_loop().run_in_executor(
            None, generer_documents_facturation, mois
        )
    except Exception as e:
        logger.exception("Erreur facturation/documents")
        raise HTTPException(503, f"Erreur lors de la génération des documents facturation : {e}")
    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=facturation_{suffix}.zip"},
    )