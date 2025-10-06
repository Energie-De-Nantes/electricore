"""
API REST sécurisée pour ElectriCore.
Expose les données Enedis via endpoints génériques avec authentification par clé API.
"""

from fastapi import FastAPI, Query, HTTPException, Depends
from typing import Optional

from electricore.api.services import duckdb_service
from electricore.api.config import settings
from electricore.api.security import get_current_api_key, get_api_key_info, APIKeyInfo

# Configuration de l'application avec métadonnées de sécurité
app = FastAPI(
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
            "name": "admin",
            "description": "Endpoints d'administration (authentification requise)"
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