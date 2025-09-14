"""
API REST simple pour ElectriCore.
Expose les données Enedis via endpoints génériques.
"""

from fastapi import FastAPI, Query, HTTPException
from typing import Optional
from electricore.api.services import duckdb_service

app = FastAPI(
    title="ElectriCore API",
    version="0.1.0",
    description="API simple pour accéder aux données flux Enedis"
)


@app.get("/")
async def root():
    """Liste les tables disponibles et montre des exemples d'utilisation."""
    try:
        tables = duckdb_service.list_tables()
        return {
            "message": "ElectriCore API - Données flux Enedis",
            "available_tables": tables,
            "examples": {
                "list_tables": "/",
                "get_flux_data": "/flux/r151?limit=10",
                "filter_by_prm": "/flux/c15?prm=12345678901234",
                "table_info": "/flux/r64/info",
                "pagination": "/flux/r151?limit=50&offset=100"
            },
            "docs": "/docs"
        }
    except Exception as e:
        raise HTTPException(500, f"Erreur lors de l'accès à la base de données: {e}")


@app.get("/flux/{table_name}")
async def get_flux(
    table_name: str,
    prm: Optional[str] = Query(None, description="Filtrer par pdl (Point de Livraison)"),
    limit: int = Query(100, le=1000, description="Nombre maximum de lignes à retourner"),
    offset: int = Query(0, ge=0, description="Nombre de lignes à ignorer (pagination)")
):
    """
    Endpoint générique pour lire n'importe quel flux Enedis.
    
    Exemples:
    - /flux/r151 : Relevés quotidiens
    - /flux/c15 : Changements contractuels  
    - /flux/r64 : Relevés demandés sur SGE
    - /flux/f15_detail : Facturation Enedis détaillée
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


@app.get("/flux/{table_name}/info")
async def get_table_info(table_name: str):
    """
    Retourne les métadonnées d'une table (colonnes, types, nombre de lignes).
    
    Utile pour comprendre la structure des données avant de faire des requêtes.
    """
    try:
        return duckdb_service.get_table_info(table_name)
    except Exception as e:
        available_tables = duckdb_service.list_tables()
        raise HTTPException(
            404, 
            f"Table '{table_name}' non trouvée. Tables disponibles: {available_tables}"
        )


@app.get("/health")
async def health():
    """Endpoint de vérification de santé de l'API."""
    try:
        # Test de connexion à la base
        tables = duckdb_service.list_tables()
        return {
            "status": "ok",
            "database": "accessible",
            "tables_count": len(tables)
        }
    except Exception as e:
        raise HTTPException(500, f"Base de données inaccessible: {e}")