"""Router de l'endpoint relevés canoniques (`/releves*`, ADR-0032).

Expose le *modèle de relevés canonique* (mart dbt `releves`, ADR-0029) — union
arbitrée C15/R64/R151 — hors du namespace `/flux/*` réservé aux flux Enedis
bruts. Adossé au loader `releves()` (et **non** au registre `FLUX_DESCRIPTORS`),
consommé par les notebooks distants via `ElectricoreClient.releves()`.

**Ordre des routes important** — comme `/flux/*`, les routes `.xlsx`, `.arrow`
et `/info` (sous-ressource) DOIVENT précéder la route JSON `/releves`.
"""

from fastapi import APIRouter, Depends, HTTPException, Query

from electricore.api.decorators import arrow_endpoint, xlsx_endpoint
from electricore.api.security import get_current_api_key
from electricore.api.services import duckdb_service
from electricore.core.loaders.duckdb import DuckDBLockError

router = APIRouter(tags=["releves"])

# Sources autorisées du mart `releves` (valeurs de la colonne `source`, ADR-0029).
# Câblées en dur : `source` ne transite jamais comme nom de colonne, et toute
# valeur hors de cet ensemble est rejetée en 422 avant d'atteindre le SQL.
SOURCES_VALIDES = ("flux_R151", "flux_R64", "flux_C15")


def _load_releves_df(
    prm: str | None = None,
    source: str | None = None,
    debut: str | None = None,
    fin: str | None = None,
    limit: int = 1_000_000,
):
    """Charge le mart `releves` via le loader `releves()` (ADR-0029), filtres appliqués.

    Helper partagé par toutes les routes `/releves` (JSON / `.arrow` / `.xlsx`).
    Chaque filtre est mappé à UNE colonne câblée en dur — `prm`→`pdl`, `source`→`source`,
    `debut`/`fin`→`date_releve` (fenêtre inclusive) — jamais une colonne fournie par le
    client ; les valeurs transitent comme paramètres liés via `DuckDBQuery.filter`.

    Test seam : monkeypatch ce helper pour court-circuiter l'IO DuckDB dans les
    tests d'endpoint (le SQL est paramétré et la sécurité est testée séparément).
    """
    from electricore.core.loaders.duckdb import releves

    if source is not None and source not in SOURCES_VALIDES:
        raise HTTPException(422, f"source invalide {source!r} ; valeurs acceptées : {list(SOURCES_VALIDES)}")

    query = releves()
    if prm:
        query = query.filter({"pdl": prm})
    if source:
        query = query.filter({"source": source})
    # Fenêtre inclusive sur date_releve. La valeur est liée en paramètre par
    # `DuckDBQuery.filter` (clause `date_releve >= ?` / `<= ?`), pas interpolée.
    if debut:
        query = query.filter({"date_releve": f">= '{debut}'"})
    if fin:
        query = query.filter({"date_releve": f"<= '{fin}'"})
    return query.limit(limit).collect()


_F_PRM = Query(None, description="Filtrer par PDL (colonne `pdl`)")
_F_SOURCE = Query(None, description="Filtrer par source (`flux_R151` / `flux_R64` / `flux_C15`)")
_F_DEBUT = Query(None, description="Borne basse incluse sur `date_releve` (ex. `2025-01-01`)")
_F_FIN = Query(None, description="Borne haute incluse sur `date_releve` (ex. `2025-12-31`)")


@arrow_endpoint(router, "/releves.arrow")
def get_releves_arrow(
    prm: str | None = _F_PRM,
    source: str | None = _F_SOURCE,
    debut: str | None = _F_DEBUT,
    fin: str | None = _F_FIN,
    limit: int = Query(1_000_000, le=10_000_000, description="Nombre maximum de lignes (défaut 1 000 000)"),
) -> bytes:
    """Exporte le mart `releves` en flux Arrow IPC (max 10 000 000 lignes).

    Consommable côté client avec `pl.read_ipc_stream(BytesIO(content))` — typage et
    timezones `Europe/Paris` préservés. Cf. `ElectricoreClient.releves`.
    """
    from electricore.api.serializers import arrow_stream

    df = _load_releves_df(prm=prm, source=source, debut=debut, fin=fin, limit=limit)
    return arrow_stream(df)


@xlsx_endpoint(router, "/releves.xlsx", filename="releves.xlsx", error_status=500)
def get_releves_xlsx(
    prm: str | None = _F_PRM,
    source: str | None = _F_SOURCE,
    debut: str | None = _F_DEBUT,
    fin: str | None = _F_FIN,
    limit: int = Query(10000, le=100000, description="Nombre maximum de lignes"),
) -> bytes:
    """Exporte le mart `releves` au format XLSX (max 100 000 lignes)."""
    from electricore.api.serializers import xlsx_multi_sheet

    df = _load_releves_df(prm=prm, source=source, debut=debut, fin=fin, limit=limit)
    return xlsx_multi_sheet({"releves": df})


@router.get("/releves/info")
async def get_releves_info(api_key: str = Depends(get_current_api_key)):
    """Métadonnées du mart `releves` : colonnes/types, nombre de lignes, dernière `date_releve`.

    **Authentification requise** — Utilisez votre clé API.

    Le mart est physiquement `flux_enedis.releves` (sans préfixe `flux_`) ; sa colonne
    de date métier est `date_releve`.
    """
    try:
        return duckdb_service.get_table_info("releves", prefix="", date_column="date_releve")
    except DuckDBLockError:
        # Base verrouillée par l'ingestion → 503 via le handler d'app (#171).
        raise
    except Exception:
        raise HTTPException(404, "Mart 'releves' introuvable (base non assemblée ?)")


@router.get("/releves")
async def get_releves(
    prm: str | None = _F_PRM,
    source: str | None = _F_SOURCE,
    debut: str | None = _F_DEBUT,
    fin: str | None = _F_FIN,
    limit: int = Query(100, le=1000, description="Nombre maximum de lignes à retourner"),
    offset: int = Query(0, ge=0, description="Nombre de lignes à ignorer (pagination)"),
    api_key: str = Depends(get_current_api_key),
):
    """Lecture JSON du mart `releves` (réponse enveloppée paginée).

    **Authentification requise** — Utilisez votre clé API.

    Shape calquée sur `/flux/{table_name}` : `{table, filters, pagination, data}`.
    Le mart est physiquement `flux_enedis.releves` (sans préfixe `flux_`).
    """
    df = _load_releves_df(prm=prm, source=source, debut=debut, fin=fin, limit=limit + offset)
    rows = df.slice(offset, limit).to_dicts()
    filtres = {k: v for k, v in (("prm", prm), ("source", source), ("debut", debut), ("fin", fin)) if v}
    return {
        "table": "releves",
        "filters": filtres or None,
        "pagination": {"limit": limit, "offset": offset, "returned": len(rows)},
        "data": rows,
    }
