"""
Service DuckDB pour accès générique aux données flux.
Fonctions pures pour lire les tables de flux Enedis.
"""

import logging
from datetime import UTC, datetime

import duckdb

from electricore.config import runtime
from electricore.core.loaders.duckdb import duckdb_readonly_conn

logger = logging.getLogger(__name__)

SCHEMA = "flux_enedis"

# Colonne de date métier par table — la fraîcheur (#158) est le max de cette
# colonne (date de relevé / d'événement / de facture), pas la date d'ingestion.
COLONNE_DATE_METIER = {
    "c15": "date_evenement",
    "f12_detail": "date_facture",
    "f15_detail": "date_facture",
    "r15": "date_releve",
    "r15_acc": "date_releve",
    "r151": "date_releve",
    "r64": "date_releve",
}


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
    db_path = runtime.duckdb().chemin
    payload: dict = {"accessible": False, "last_write": None, "tables": {}}
    try:
        st = db_path.stat()
        payload["last_write"] = datetime.fromtimestamp(st.st_mtime, tz=UTC).isoformat()
    except FileNotFoundError:
        payload["error"] = f"Fichier DuckDB introuvable: {db_path}"
        return payload
    except OSError as exc:
        payload["error"] = f"Erreur accès fichier: {exc}"
        return payload

    try:
        with duckdb_readonly_conn(db_path) as conn:
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


def get_table_info(table_name: str, *, prefix: str = "flux_", date_column: str | None = None) -> dict:
    """
    Retourne les informations sur une table (colonnes, nombre de lignes).

    Args:
        table_name: Nom logique de la table (sans préfixe pour les flux : `c15`, `r151`…).
        prefix: Préfixe de la table physique. `"flux_"` par défaut (flux Enedis bruts) ;
            `""` pour les marts dérivés exposés tels quels (ex. `releves`, #264/ADR-0032).
        date_column: Colonne de date métier pour `derniere_date`. Si `None`, résolue via
            `COLONNE_DATE_METIER[table_name]` (convention flux) ; à fournir pour les marts
            absents de ce registre (ex. `date_releve` pour `releves`).

    Returns:
        Dict avec table, schema, count, columns, derniere_date
    """
    physical = f"{prefix}{table_name}"
    colonne_date = date_column or COLONNE_DATE_METIER.get(table_name)
    with duckdb_readonly_conn(runtime.duckdb().chemin) as conn:
        # Nombre de lignes
        count = conn.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{physical}").fetchone()[0]

        # Colonnes avec leurs types
        columns_result = conn.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{SCHEMA}'
            AND table_name = '{physical}'
            ORDER BY ordinal_position
        """).fetchall()

        columns = [{"name": col[0], "type": col[1]} for col in columns_result]

        derniere_date = None
        if colonne_date and any(c["name"] == colonne_date for c in columns):
            max_val = conn.execute(f"SELECT max({colonne_date}) FROM {SCHEMA}.{physical}").fetchone()[0]
            if max_val is not None:
                derniere_date = str(max_val)[:10]

        return {
            "table": physical,
            "schema": SCHEMA,
            "count": count,
            "columns": columns,
            "derniere_date": derniere_date,
        }


def list_tables() -> list[str]:
    """
    Liste toutes les tables flux disponibles.

    Returns:
        Liste des noms de tables (sans préfixe flux_)
    """
    with duckdb_readonly_conn(runtime.duckdb().chemin) as conn:
        tables = conn.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{SCHEMA}' 
            AND table_name LIKE 'flux_%'
            AND table_name NOT LIKE '_dlt%'
            ORDER BY table_name
        """).fetchall()

        return [t[0].replace("flux_", "") for t in tables]
