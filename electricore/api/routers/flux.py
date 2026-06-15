"""Router des endpoints flux Enedis (issue #82).

**Ordre des routes important** — les routes `.xlsx`, `.arrow` et `/info`
(sous-ressource) DOIVENT précéder la route catch-all `/flux/{table_name}`,
sinon `{table_name}` capture `c15.xlsx` et la requête est mal routée.
"""

from fastapi import APIRouter, Depends, HTTPException, Query

from electricore.api.decorators import arrow_endpoint, xlsx_endpoint
from electricore.api.security import get_current_api_key
from electricore.api.services import duckdb_service
from electricore.core.loaders.duckdb import DuckDBLockError

router = APIRouter(tags=["flux"])


@xlsx_endpoint(router, "/flux/c15/entrees.xlsx", filename="entrees_c15.xlsx", error_status=500)
def get_entrees_xlsx(
    limit: int = Query(10000, le=100000, description="Nombre maximum de lignes"),
) -> bytes:
    """Exporte les **entrées** C15 au format XLSX (PMES, MES, CFNE)."""
    from electricore.api.serializers import xlsx_multi_sheet
    from electricore.core.loaders.duckdb import c15

    df = c15().entrees().limit(limit).collect()
    return xlsx_multi_sheet({"entrees": df})


@xlsx_endpoint(router, "/flux/c15/sorties.xlsx", filename="sorties_c15.xlsx", error_status=500)
def get_sorties_xlsx(
    limit: int = Query(10000, le=100000, description="Nombre maximum de lignes"),
) -> bytes:
    """Exporte les **sorties** C15 au format XLSX (RES, CFNS)."""
    from electricore.api.serializers import xlsx_multi_sheet
    from electricore.core.loaders.duckdb import c15

    df = c15().sorties().limit(limit).collect()
    return xlsx_multi_sheet({"sorties": df})


def _load_flux_df(table_name: str, prm: str | None, limit: int):
    """Charge un flux via la factory `flux(nom)` du loader + serializer.

    Pur transport : la résolution du registre et l'erreur « flux inconnu »
    vivent derrière l'interface du loader (`flux` / `FluxInconnu`). On se
    contente ici de mapper `FluxInconnu` sur un 404 et d'appliquer prm/limit.

    Test seam : substituer `flux` au seam du loader
    (`electricore.core.loaders.duckdb.flux`) pour court-circuiter l'IO DuckDB.
    """
    from electricore.core.loaders.duckdb import FluxInconnu, flux

    try:
        query = flux(table_name)
    except FluxInconnu as e:
        raise HTTPException(404, str(e))

    if prm:
        query = query.filter({"pdl": prm})
    return query.limit(limit).collect()


# IMPORTANT — ordre de déclaration : les routes `.xlsx` / `.arrow` et `/info`
# (sous-ressource) DOIVENT précéder la route catch-all `/flux/{table_name}`,
# sinon `{table_name}` capture `c15.xlsx` et la requête est mal routée.


@xlsx_endpoint(router, "/flux/{table_name}.xlsx", filename="flux_{table_name}.xlsx", error_status=500)
def get_flux_xlsx(
    table_name: str,
    prm: str | None = Query(None, description="Filtrer par pdl (Point de Livraison)"),
    limit: int = Query(10000, le=100000, description="Nombre maximum de lignes"),
) -> bytes:
    """Exporte les données d'un flux Enedis au format XLSX (max 100 000 lignes)."""
    from electricore.api.serializers import xlsx_multi_sheet

    df = _load_flux_df(table_name, prm, limit)
    return xlsx_multi_sheet({table_name: df})


@arrow_endpoint(router, "/flux/{table_name}.arrow")
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


@router.get("/flux/{table_name}/info")
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
    except DuckDBLockError:
        # Base verrouillée par l'ingestion ≠ table inconnue — laisser le
        # handler d'app répondre 503 « ingestion en cours » (#171).
        raise
    except Exception:
        available_tables = duckdb_service.list_tables()
        raise HTTPException(404, f"Table '{table_name}' non trouvée. Tables disponibles: {available_tables}")


# `/flux/{table_name}` (JSON) déclaré APRÈS les routes plus spécifiques pour que
# `/flux/c15.xlsx` matche bien l'endpoint XLSX et pas le catch-all.
@router.get("/flux/{table_name}")
async def get_flux(
    table_name: str,
    prm: str | None = Query(None, description="Filtrer par pdl (Point de Livraison)"),
    limit: int = Query(100, le=1000, description="Nombre maximum de lignes à retourner"),
    offset: int = Query(0, ge=0, description="Nombre de lignes à ignorer (pagination)"),
    api_key: str = Depends(get_current_api_key),
):
    """
    Endpoint générique pour lire n'importe quel flux Enedis (réponse JSON).

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
