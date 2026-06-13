"""
API REST sécurisée pour ElectriCore.
Expose les données Enedis via endpoints génériques avec authentification par clé API.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from urllib.parse import urlparse

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from electricore.api.config import settings
from electricore.api.routers import admin as admin_router
from electricore.api.routers import facturation as facturation_router
from electricore.api.routers import flux as flux_router
from electricore.api.routers import ingestion as ingestion_router
from electricore.api.routers import taxes as taxes_router
from electricore.api.services import duckdb_service
from electricore.config import runtime
from electricore.core.loaders.duckdb import DuckDBLockError

logger = logging.getLogger(__name__)

_tg_app = None


def _format_sftp_source() -> str:
    """Identifiant lisible de la source SFTP, sans secret (cf. ADR-0015)."""
    try:
        raw = runtime.sftp().url
    except runtime.ConfigurationManquante:
        raw = ""
    if not raw:
        return "(unset)"
    parsed = urlparse(raw)
    if parsed.scheme == "file":
        return raw
    if parsed.hostname:
        return f"{parsed.scheme}://{parsed.hostname}"
    return parsed.scheme or "(unset)"


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _tg_app
    odoo_db = runtime.odoo().db if settings.is_odoo_configured else "(unset)"
    logger.info(
        "Instance=%s, odoo_env=%s, odoo_db=%s, sftp=%s",
        settings.instance_slug or "(unset)",
        settings.odoo_env,
        odoo_db,
        _format_sftp_source(),
    )
    if settings.telegram_bot_token:
        from electricore.bot.app import build_application

        _tg_app = build_application(settings.telegram_bot_token)
        await _tg_app.initialize()
        await _tg_app.start()
        await _tg_app.updater.start_polling()
        logger.info("Bot Telegram démarré.")
    yield
    if _tg_app is not None:
        from electricore.bot.tasks import _background_tasks

        for task in list(_background_tasks):
            task.cancel()
        if _background_tasks:
            await asyncio.gather(*_background_tasks, return_exceptions=True)
        await _tg_app.updater.stop()
        await _tg_app.stop()
        await _tg_app.shutdown()
        logger.info("Bot Telegram arrêté.")


_instance_suffix = f" — {settings.instance_slug.upper()}" if settings.instance_slug else ""

# Configuration de l'application avec métadonnées de sécurité
app = FastAPI(
    lifespan=lifespan,
    title=f"{settings.api_title}{_instance_suffix}",
    version=settings.api_version,
    description=f"{settings.api_description}\n\n"
    "**Authentification requise** : Utilisez une clé API valide via :\n"
    "- Header : `X-API-Key: votre_cle_api`\n\n"
    "Endpoints publics (sans authentification) : /, /health, /docs",
    openapi_tags=[
        {"name": "public", "description": "Endpoints publics (sans authentification)"},
        {"name": "flux", "description": "Accès aux données flux Enedis (authentification requise)"},
        {"name": "ingestion", "description": "Lancement et suivi de l'ingestion des flux (authentification requise)"},
        {"name": "admin", "description": "Endpoints d'administration (authentification requise)"},
        {
            "name": "taxes",
            "description": "Calcul des taxes énergétiques CTA et Accise TICFE (authentification requise)",
        },
        {"name": "facturation", "description": "Réconciliation facturation Odoo ↔ Enedis (authentification requise)"},
    ],
)

DETAIL_INGESTION_EN_COURS = (
    "Ingestion en cours — la base de données est en cours d'écriture. Réessaie dans quelques minutes."
)


@app.exception_handler(DuckDBLockError)
async def verrou_duckdb_en_503(request, exc: DuckDBLockError) -> JSONResponse:
    """Verrou writer (ingestion) sur DuckDB → 503 explicite plutôt qu'erreur brute (#171).

    Conversion centrale : tout endpoint de lecture qui laisse remonter
    `DuckDBLockError` en bénéficie, sans logique par route.
    """
    logger.warning("Lecture refusée, base verrouillée par un writer (%s %s) : %s", request.method, request.url, exc)
    return JSONResponse(status_code=503, content={"detail": DETAIL_INGESTION_EN_COURS})


# Routers per-domaine (issue #82). Chaque router porte son tag OpenAPI et ses endpoints.
app.include_router(admin_router.router)
app.include_router(ingestion_router.router)
app.include_router(flux_router.router)
app.include_router(taxes_router.router)
app.include_router(facturation_router.router)


@app.get("/", tags=["public"])
async def root():
    """
    Page d'accueil de l'API avec informations générales.

    Endpoint public - aucune authentification requise.
    Liste les tables disponibles et montre des exemples d'utilisation.

    Reste accessible même si la base DuckDB n'existe pas encore (premier
    déploiement avant la première ingestion) : `available_tables` est alors vide.
    """
    try:
        tables = duckdb_service.list_tables()
    except Exception as exc:
        logger.warning("Liste des tables indisponible (%s) — affichage dégradé", exc)
        tables = []
    return {
        "message": "ElectriCore API - Données flux Enedis sécurisées",
        "version": settings.api_version,
        "authentication": {
            "required": "Clé API requise pour accéder aux données",
            "method": "X-API-Key: votre_cle_api (header uniquement)",
        },
        "available_tables": tables,
        "examples": {
            "get_flux_data": "curl -H 'X-API-Key: VOTRE_CLE' '/flux/r151?limit=10'",
            "filter_by_prm": "curl -H 'X-API-Key: VOTRE_CLE' '/flux/c15?prm=12345678901234'",
            "table_info": "curl -H 'X-API-Key: VOTRE_CLE' '/flux/r64/info'",
            "pagination": "curl -H 'X-API-Key: VOTRE_CLE' '/flux/r151?limit=50&offset=100'",
        },
        "docs": "/docs",
    }


@app.get("/health", tags=["public"])
async def health():
    """
    Endpoint de vérification de santé de l'API.

    Endpoint public - aucune authentification requise.

    Retourne toujours un 200 avec un payload structuré : ops peut détecter
    les problèmes (base verrouillée, ingestion en retard, bot arrêté) sans avoir
    à parser des erreurs HTTP. Un `database.accessible: false` indique typiquement
    un verrou d'ingestion en cours — l'API se rétablit d'elle-même après le checkpoint.

    Returns:
        Dict avec api_version, database (mtime + tailles tables), bot, authentication
    """
    freshness = duckdb_service.get_freshness()
    return {
        "status": "ok",
        "instance": settings.instance_slug,
        "api_version": settings.api_version,
        "database": freshness,
        "bot": {"running": _tg_app is not None},
        "authentication": {
            "api_keys_configured": len(settings.get_valid_api_keys()) > 0,
            "method": "X-API-Key header",
        },
    }
