"""
API REST sécurisée pour ElectriCore.
Expose les données Enedis via endpoints génériques avec authentification par clé API.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import Response

from electricore.api.config import settings
from electricore.api.decorators import arrow_endpoint, xlsx_endpoint
from electricore.api.models import ETLJobResponse, ETLRunRequest
from electricore.api.security import APIKeyInfo, get_api_key_info, get_current_api_key
from electricore.api.services import duckdb_service, etl_service
from electricore.api.services.check_facturation_service import verifier_odoo
from electricore.api.services.facturation_service import (
    generer_documents_facturation,
    generer_facturation_arrow,
    generer_facturation_xlsx,
)
from electricore.api.services.taxes_service import (
    generer_accise_arrow,
    generer_accise_xlsx,
    generer_cta_arrow,
    generer_cta_xlsx,
)

logger = logging.getLogger(__name__)

_tg_app = None


def _format_sftp_source() -> str:
    """Identifiant lisible de la source SFTP, sans secret (cf. ADR-0015)."""
    raw = os.getenv("SFTP__URL", "")
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
    odoo_db = os.getenv(f"ODOO_{settings.odoo_env.upper()}_DB", "(unset)")
    logger.info(
        "Instance=%s, odoo_env=%s, odoo_db=%s, sftp=%s",
        settings.instance_slug or "(unset)",
        settings.odoo_env,
        odoo_db,
        _format_sftp_source(),
    )
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
        {"name": "etl", "description": "Lancement et suivi du pipeline d'ingestion (authentification requise)"},
        {"name": "admin", "description": "Endpoints d'administration (authentification requise)"},
        {
            "name": "taxes",
            "description": "Calcul des taxes énergétiques CTA et Accise TICFE (authentification requise)",
        },
        {"name": "facturation", "description": "Réconciliation facturation Odoo ↔ Enedis (authentification requise)"},
    ],
)


@app.get("/", tags=["public"])
async def root():
    """
    Page d'accueil de l'API avec informations générales.

    Endpoint public - aucune authentification requise.
    Liste les tables disponibles et montre des exemples d'utilisation.

    Reste accessible même si la base DuckDB n'existe pas encore (premier
    déploiement avant le premier ETL) : `available_tables` est alors vide.
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


@xlsx_endpoint(app, "/flux/c15/entrees/xlsx", filename="entrees_c15.xlsx", error_status=500, tags=["flux"])
def get_entrees_xlsx(
    limit: int = Query(10000, le=100000, description="Nombre maximum de lignes"),
) -> bytes:
    """Exporte les **entrées** C15 au format XLSX (PMES, MES, CFNE)."""
    from electricore.api.serializers import xlsx_multi_sheet
    from electricore.core.loaders.duckdb import c15

    df = c15().entrees().limit(limit).collect()
    return xlsx_multi_sheet({"entrees": df})


@xlsx_endpoint(app, "/flux/c15/sorties/xlsx", filename="sorties_c15.xlsx", error_status=500, tags=["flux"])
def get_sorties_xlsx(
    limit: int = Query(10000, le=100000, description="Nombre maximum de lignes"),
) -> bytes:
    """Exporte les **sorties** C15 au format XLSX (RES, CFNS)."""
    from electricore.api.serializers import xlsx_multi_sheet
    from electricore.core.loaders.duckdb import c15

    df = c15().sorties().limit(limit).collect()
    return xlsx_multi_sheet({"sorties": df})


def _load_flux_df(table_name: str, prm: str | None, limit: int):
    """Charge un flux via DuckDBQuery + serializer. Whitelist déclarative via FLUX_CONFIGS.

    Test seam : monkeypatch ce helper pour court-circuiter l'IO DuckDB dans les
    tests d'endpoint (le SQL est paramétré et la sécurité est testée séparément).
    """
    from electricore.core.loaders.duckdb.helpers import make_query
    from electricore.core.loaders.duckdb.registry import FLUX_CONFIGS

    if table_name not in FLUX_CONFIGS:
        raise HTTPException(
            404,
            f"Table '{table_name}' non trouvée. Tables disponibles: {sorted(FLUX_CONFIGS.keys())}",
        )

    query = make_query(FLUX_CONFIGS[table_name])
    if prm:
        query = query.filter({"pdl": prm})
    return query.limit(limit).collect()


@app.get("/flux/{table_name}", tags=["flux"])
async def get_flux(
    table_name: str,
    prm: str | None = Query(None, description="Filtrer par pdl (Point de Livraison)"),
    limit: int = Query(100, le=1000, description="Nombre maximum de lignes à retourner"),
    offset: int = Query(0, ge=0, description="Nombre de lignes à ignorer (pagination)"),
    api_key: str = Depends(get_current_api_key),
):
    """
    Endpoint générique pour lire n'importe quel flux Enedis.

    **Authentification requise** - Utilisez votre clé API.

    Exemples:
    - /flux/r151 : Relevés quotidiens
    - /flux/c15 : Changements contractuels
    - /flux/r64 : Relevés demandés sur SGE
    - /flux/f15 : Facturation Enedis détaillée
    """
    df = _load_flux_df(table_name, prm, limit + offset)
    rows = df.slice(offset, limit).to_dicts()
    return {
        "table": f"flux_{table_name}",
        "filters": {"pdl": prm} if prm else None,
        "pagination": {"limit": limit, "offset": offset, "returned": len(rows)},
        "data": rows,
    }


@xlsx_endpoint(app, "/flux/{table_name}/xlsx", filename="flux_{table_name}.xlsx", error_status=500, tags=["flux"])
def get_flux_xlsx(
    table_name: str,
    prm: str | None = Query(None, description="Filtrer par pdl (Point de Livraison)"),
    limit: int = Query(10000, le=100000, description="Nombre maximum de lignes"),
) -> bytes:
    """Exporte les données d'un flux Enedis au format XLSX (max 100 000 lignes)."""
    from electricore.api.serializers import xlsx_multi_sheet

    df = _load_flux_df(table_name, prm, limit)
    return xlsx_multi_sheet({table_name: df})


@arrow_endpoint(app, "/flux/{table_name}/arrow", tags=["flux"])
def get_flux_arrow(
    table_name: str,
    prm: str | None = Query(None, description="Filtrer par pdl (Point de Livraison)"),
    limit: int = Query(1_000_000, le=10_000_000, description="Nombre maximum de lignes (défaut 1 000 000)"),
) -> bytes:
    """Exporte les données d'un flux Enedis en flux Arrow IPC (max 10 000 000 lignes).

    Consommable côté client avec `pl.read_ipc_stream(BytesIO(content))` — typage et
    timezones préservés, pas de perte de précision contrairement à XLSX. Cf. `ElectricoreClient.flux`.
    """
    from electricore.api.serializers import arrow_stream

    df = _load_flux_df(table_name, prm, limit)
    return arrow_stream(df)


@app.get("/flux/{table_name}/info", tags=["flux"])
async def get_table_info(table_name: str, api_key: str = Depends(get_current_api_key)):
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
    except Exception:
        available_tables = duckdb_service.list_tables()
        raise HTTPException(404, f"Table '{table_name}' non trouvée. Tables disponibles: {available_tables}")


@app.get("/health", tags=["public"])
async def health():
    """
    Endpoint de vérification de santé de l'API.

    Endpoint public - aucune authentification requise.

    Retourne toujours un 200 avec un payload structuré : ops peut détecter
    les problèmes (base verrouillée, ETL en retard, bot arrêté) sans avoir
    à parser des erreurs HTTP. Un `database.accessible: false` indique typiquement
    un verrou ETL en cours — l'API se rétablit d'elle-même après le checkpoint.

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
        raise HTTPException(501, "Le pipeline ETL n'est pas disponible. Installez l'extra [etl] : uv sync --extra etl")

    try:
        mode = etl_service.ETLMode(body.mode)
    except ValueError:
        raise HTTPException(422, f"Mode invalide : '{body.mode}'. Valeurs acceptées : test, r151, all, reset")

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
async def list_api_keys(api_key: str = Depends(get_current_api_key), key_info: APIKeyInfo = Depends(get_api_key_info)):
    """
    Informations sur la configuration des clés API.

    **Authentification requise** - Endpoint d'administration.

    Returns:
        Dict avec les informations sur les clés API configurées
    """
    return {
        "message": "Configuration des clés API",
        "current_key": {"preview": key_info.key_preview, "source": key_info.source},
        "configuration": {
            "total_keys": len(settings.get_valid_api_keys()),
            "method": "X-API-Key header",
            "public_endpoints": settings.public_endpoints,
        },
    }


@xlsx_endpoint(app, "/taxes/accise/xlsx", filename="accise{trimestre}.xlsx", requires_odoo=True, tags=["taxes"])
def export_accise_xlsx(
    trimestre: str | None = Query(
        default=None,
        examples=["2025-T1"],
        description="Filtre par trimestre au format YYYY-TX. Sans filtre : toutes les données.",
    ),
) -> bytes:
    """Calcule l'Accise TICFE et retourne un fichier XLSX (3 onglets : Résumé, Par taux, Détail)."""
    return generer_accise_xlsx(trimestre)


@arrow_endpoint(app, "/taxes/accise/arrow", requires_odoo=True, tags=["taxes"])
def export_accise_arrow(
    trimestre: str | None = Query(
        default=None,
        examples=["2025-T1"],
        description="Filtre par trimestre au format YYYY-TX. Sans filtre : toutes les données.",
    ),
) -> bytes:
    """Détail Accise TICFE sérialisé en Arrow IPC stream (table PDL × mois)."""
    return generer_accise_arrow(trimestre)


@xlsx_endpoint(app, "/taxes/cta/xlsx", filename="cta{trimestre}.xlsx", requires_odoo=True, tags=["taxes"])
def export_cta_xlsx(
    trimestre: str | None = Query(
        default=None,
        examples=["2025-T1"],
        description="Filtre par trimestre au format YYYY-TX. Sans filtre : toutes les données.",
    ),
) -> bytes:
    """Calcule la CTA et retourne un fichier XLSX (3 onglets : Résumé, Par taux, Détail)."""
    return generer_cta_xlsx(trimestre)


@arrow_endpoint(app, "/taxes/cta/arrow", requires_odoo=True, tags=["taxes"])
def export_cta_arrow(
    trimestre: str | None = Query(
        default=None,
        examples=["2025-T1"],
        description="Filtre par trimestre au format YYYY-TX. Sans filtre : toutes les données.",
    ),
) -> bytes:
    """Détail CTA mensuel sérialisé en Arrow IPC stream (pdl × mois avec cta_eur, taux_cta_pct)."""
    return generer_cta_arrow(trimestre)


@xlsx_endpoint(app, "/facturation/xlsx", filename="facturation{mois}.xlsx", requires_odoo=True, tags=["facturation"])
def export_facturation_xlsx(
    mois: str | None = Query(
        default=None,
        examples=["2025-01-01"],
        description="Mois au format YYYY-MM-DD (défaut : dernier mois disponible dans les données)",
    ),
) -> bytes:
    """Réconciliation des lignes Odoo à facturer avec les données Enedis (2 onglets XLSX)."""
    return generer_facturation_xlsx(mois)


@arrow_endpoint(app, "/facturation/arrow", requires_odoo=True, tags=["facturation"])
def export_facturation_arrow(
    mois: str | None = Query(
        default=None,
        examples=["2025-01-01"],
        description="Mois au format YYYY-MM-DD (défaut : dernier mois disponible dans les données)",
    ),
) -> bytes:
    """Réconciliation Odoo↔Enedis sérialisée en Arrow IPC stream (`lignes_facture_rapprochees`)."""
    return generer_facturation_arrow(mois)


@app.get("/facturation/check/odoo", tags=["facturation"])
async def check_facturation_odoo(api_key: str = Depends(get_current_api_key)):
    """
    Vérifications pré-facturation côté Odoo.

    **Authentification requise. Nécessite Odoo configuré.**

    Pour tous les sale.order avec state='sale', retourne :
    - `rsc_manquante` : liste des sale.order sans `x_ref_situation_contractuelle`
    - `cfne_manquante` : liste des sale.order sans `x_date_cfne`
    - `invoicing_state_counts` : répartition des `x_invoicing_state`
    - `factures_draft` : factures encore en draft (anomalie après campagne)

    Chaque entrée contient un lien direct vers l'enregistrement Odoo (champ `url`).
    """
    if not settings.is_odoo_configured:
        raise HTTPException(
            501,
            f"Odoo [{settings.odoo_env}] non configuré. Définissez ODOO_{settings.odoo_env.upper()}_URL/DB/USERNAME/PASSWORD dans .env",
        )
    try:
        result = await asyncio.get_event_loop().run_in_executor(None, verifier_odoo)
    except Exception as e:
        logger.exception("Erreur facturation/check/odoo")
        raise HTTPException(503, f"Erreur lors de la vérification Odoo : {e}")
    return result


@app.get("/facturation/documents", tags=["facturation"])
async def export_facturation_documents(
    mois: str | None = Query(
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
        raise HTTPException(
            501,
            f"Odoo [{settings.odoo_env}] non configuré. Définissez ODOO_{settings.odoo_env.upper()}_URL/DB/USERNAME/PASSWORD dans .env",
        )
    try:
        zip_bytes, suffix = await asyncio.get_event_loop().run_in_executor(None, generer_documents_facturation, mois)
    except Exception as e:
        logger.exception("Erreur facturation/documents")
        raise HTTPException(503, f"Erreur lors de la génération des documents facturation : {e}")
    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=facturation_{suffix}.zip"},
    )
