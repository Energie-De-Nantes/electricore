"""
Service DuckDB pour accès générique aux données flux.
Fonctions pures pour lire les tables de flux Enedis.
"""

import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from electricore.core.loaders.duckdb import duckdb_readonly_conn

logger = logging.getLogger(__name__)

DB_PATH = Path(os.getenv("DUCKDB_PATH", "electricore/etl/flux_enedis_pipeline.duckdb"))
SCHEMA = "flux_enedis"


def get_freshness() -> dict:
    """
    Retourne l'état de fraîcheur de la base DuckDB pour le `/health`.

    Returns:
        Dict avec :
        - accessible: bool, True si la base est lisible
        - last_write: ISO 8601 UTC du dernier mtime du fichier, ou None
        - tables: dict {nom_table: estimated_size}, ou {} si inaccessible
        - error: chaîne descriptive si accessible=False
    """
    payload: dict = {"accessible": False, "last_write": None, "tables": {}}
    try:
        st = DB_PATH.stat()
        payload["last_write"] = datetime.fromtimestamp(st.st_mtime, tz=UTC).isoformat()
    except FileNotFoundError:
        payload["error"] = f"Fichier DuckDB introuvable: {DB_PATH}"
        return payload
    except OSError as exc:
        payload["error"] = f"Erreur accès fichier: {exc}"
        return payload

    try:
        with duckdb_readonly_conn(DB_PATH) as conn:
            rows = conn.execute(
                "SELECT table_name, estimated_size FROM duckdb_tables() "
                "WHERE schema_name = ? AND table_name LIKE 'flux_%' "
                "AND table_name NOT LIKE '_dlt%' ORDER BY table_name",
                [SCHEMA],
            ).fetchall()
        payload["accessible"] = True
        payload["tables"] = {name: int(size) if size is not None else 0 for name, size in rows}
    except duckdb.Error as exc:
        payload["error"] = f"DuckDB inaccessible: {exc}"
    return payload


def query_table(table_name: str, filters: dict | None = None, limit: int = 100, offset: int = 0) -> list[dict]:
    """
    Fonction générique pour lire n'importe quelle table flux.

    Args:
        table_name: Nom de la table (r151, c15, r64, etc.)
        filters: Dict de filtres {colonne: valeur}
        limit: Nombre max de lignes
        offset: Pagination

    Returns:
        Liste de dictionnaires représentant les lignes
    """
    sql = f"SELECT * FROM {SCHEMA}.flux_{table_name}"

    # Ajout des filtres WHERE
    if filters:
        conditions = [f"{col} = '{val}'" for col, val in filters.items()]
        sql += f" WHERE {' AND '.join(conditions)}"

    sql += f" LIMIT {limit} OFFSET {offset}"

    with duckdb_readonly_conn(DB_PATH) as conn:
        result = conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row, strict=False)) for row in result.fetchall()]


def get_table_info(table_name: str) -> dict:
    """
    Retourne les informations sur une table (colonnes, nombre de lignes).

    Args:
        table_name: Nom de la table (sans préfixe flux_)

    Returns:
        Dict avec table, count, columns
    """
    with duckdb_readonly_conn(DB_PATH) as conn:
        # Nombre de lignes
        count = conn.execute(f"SELECT COUNT(*) FROM {SCHEMA}.flux_{table_name}").fetchone()[0]

        # Colonnes avec leurs types
        columns_result = conn.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = '{SCHEMA}' 
            AND table_name = 'flux_{table_name}'
            ORDER BY ordinal_position
        """).fetchall()

        columns = [{"name": col[0], "type": col[1]} for col in columns_result]

        return {"table": f"flux_{table_name}", "schema": SCHEMA, "count": count, "columns": columns}


def _query_table_df(table_name: str, filters: dict | None = None, limit: int = 10000):
    """Charge une table flux dans un DataFrame Polars, avec filtres et limite optionnels."""
    sql = f"SELECT * FROM {SCHEMA}.flux_{table_name}"
    if filters:
        conditions = [f"{col} = '{val}'" for col, val in filters.items()]
        sql += f" WHERE {' AND '.join(conditions)}"
    sql += f" LIMIT {limit}"

    with duckdb_readonly_conn(DB_PATH) as conn:
        return conn.execute(sql).pl()


def query_table_xlsx(
    table_name: str,
    filters: dict | None = None,
    limit: int = 10000,
) -> bytes:
    """Retourne les données d'une table flux au format XLSX (bytes).

    Args:
        table_name: Nom de la table (r151, c15, r64, etc.)
        filters: Dict de filtres {colonne: valeur}
        limit: Nombre max de lignes (défaut 10 000)

    Returns:
        Contenu XLSX en bytes
    """
    from electricore.api.serializers import xlsx_multi_sheet

    df = _query_table_df(table_name, filters, limit)
    return xlsx_multi_sheet({table_name: df})


def query_table_arrow(
    table_name: str,
    filters: dict | None = None,
    limit: int = 1_000_000,
) -> bytes:
    """Retourne les données d'une table flux au format Arrow IPC (bytes).

    Args:
        table_name: Nom de la table (r151, c15, r64, etc.)
        filters: Dict de filtres {colonne: valeur}
        limit: Nombre max de lignes (défaut 1 000 000)

    Returns:
        Flux Arrow IPC en bytes, re-lisible via `pl.read_ipc_stream(BytesIO(...))`.
    """
    from electricore.api.serializers import arrow_stream

    df = _query_table_df(table_name, filters, limit)
    return arrow_stream(df)


_ENTREES = ("PMES", "MES", "CFNE")
_SORTIES = ("RES", "CFNS")


def _query_c15_xlsx(codes: tuple[str, ...], worksheet: str, limit: int) -> bytes:
    from electricore.api.serializers import xlsx_multi_sheet

    placeholders = ", ".join(f"'{c}'" for c in codes)
    sql = f"SELECT * FROM {SCHEMA}.flux_c15 WHERE evenement_declencheur IN ({placeholders}) LIMIT {limit}"
    with duckdb_readonly_conn(DB_PATH) as conn:
        df = conn.execute(sql).pl()
    return xlsx_multi_sheet({worksheet: df})


def query_entrees_xlsx(limit: int = 10000) -> bytes:
    """Export XLSX des entrées C15 (PMES, MES, CFNE)."""
    return _query_c15_xlsx(_ENTREES, "entrees", limit)


def query_sorties_xlsx(limit: int = 10000) -> bytes:
    """Export XLSX des sorties C15 (RES, CFNS)."""
    return _query_c15_xlsx(_SORTIES, "sorties", limit)


def list_tables() -> list[str]:
    """
    Liste toutes les tables flux disponibles.

    Returns:
        Liste des noms de tables (sans préfixe flux_)
    """
    with duckdb_readonly_conn(DB_PATH) as conn:
        tables = conn.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{SCHEMA}' 
            AND table_name LIKE 'flux_%'
            AND table_name NOT LIKE '_dlt%'
            ORDER BY table_name
        """).fetchall()

        return [t[0].replace("flux_", "") for t in tables]
