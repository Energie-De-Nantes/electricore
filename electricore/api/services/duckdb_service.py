"""
Service DuckDB pour accès générique aux données flux.
Fonctions pures pour lire les tables de flux Enedis.
"""

import io
import logging
import os
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

DB_PATH = Path(os.getenv("DUCKDB_PATH", "electricore/etl/flux_enedis_pipeline.duckdb"))
SCHEMA = "flux_enedis"

# Politique de retry lorsque le writer ETL détient le verrou exclusif.
# 3 essais × 1s couvrent les fenêtres typiques de checkpoint DLT.
_LOCK_RETRY_ATTEMPTS = 3
_LOCK_RETRY_BACKOFF_S = 1.0


@contextmanager
def _connect_readonly():
    """
    Ouvre une connexion DuckDB read-only en réessayant si le fichier est verrouillé.

    Le writer ETL (autre conteneur) peut prendre un lock exclusif pendant un
    checkpoint ou pendant `EXPORT DATABASE`. On retente brièvement avant de
    propager l'erreur, pour absorber les pics courts sans casser les requêtes API.
    """
    last_exc: Exception | None = None
    for attempt in range(_LOCK_RETRY_ATTEMPTS):
        try:
            conn = duckdb.connect(str(DB_PATH), read_only=True)
        except duckdb.IOException as exc:
            last_exc = exc
            if attempt < _LOCK_RETRY_ATTEMPTS - 1:
                logger.warning(
                    "DuckDB verrouillé (tentative %d/%d), nouvelle tentative dans %.1fs",
                    attempt + 1,
                    _LOCK_RETRY_ATTEMPTS,
                    _LOCK_RETRY_BACKOFF_S,
                )
                time.sleep(_LOCK_RETRY_BACKOFF_S)
                continue
            raise
        try:
            yield conn
        finally:
            conn.close()
        return
    # Ne devrait jamais être atteint (le `raise` ci-dessus le couvre)
    raise last_exc if last_exc else RuntimeError("Échec ouverture DuckDB")


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
        with _connect_readonly() as conn:
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

    with _connect_readonly() as conn:
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
    with _connect_readonly() as conn:
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


def query_table_xlsx(
    table_name: str,
    filters: dict | None = None,
    limit: int = 10000,
) -> bytes:
    """
    Retourne les données d'une table flux au format XLSX (bytes).

    Args:
        table_name: Nom de la table (r151, c15, r64, etc.)
        filters: Dict de filtres {colonne: valeur}
        limit: Nombre max de lignes (défaut 10 000)

    Returns:
        Contenu XLSX en bytes
    """
    sql = f"SELECT * FROM {SCHEMA}.flux_{table_name}"
    if filters:
        conditions = [f"{col} = '{val}'" for col, val in filters.items()]
        sql += f" WHERE {' AND '.join(conditions)}"
    sql += f" LIMIT {limit}"

    with _connect_readonly() as conn:
        df = conn.execute(sql).pl()

    import xlsxwriter

    buf = io.BytesIO()
    wb = xlsxwriter.Workbook(buf, {"remove_timezone": True})
    df.write_excel(workbook=wb, worksheet=table_name)
    wb.close()
    return buf.getvalue()


_ENTREES = ("PMES", "MES", "CFNE")
_SORTIES = ("RES", "CFNS")


def _query_c15_xlsx(codes: tuple[str, ...], worksheet: str, limit: int) -> bytes:
    placeholders = ", ".join(f"'{c}'" for c in codes)
    sql = f"SELECT * FROM {SCHEMA}.flux_c15 WHERE evenement_declencheur IN ({placeholders}) LIMIT {limit}"
    with _connect_readonly() as conn:
        df = conn.execute(sql).pl()

    import xlsxwriter

    buf = io.BytesIO()
    wb = xlsxwriter.Workbook(buf, {"remove_timezone": True})
    df.write_excel(workbook=wb, worksheet=worksheet)
    wb.close()
    return buf.getvalue()


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
    with _connect_readonly() as conn:
        tables = conn.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{SCHEMA}' 
            AND table_name LIKE 'flux_%'
            AND table_name NOT LIKE '_dlt%'
            ORDER BY table_name
        """).fetchall()

        return [t[0].replace("flux_", "") for t in tables]
