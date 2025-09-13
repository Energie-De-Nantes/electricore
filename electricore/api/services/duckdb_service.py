"""
Service DuckDB pour accès générique aux données flux.
Fonctions pures pour lire les tables de flux Enedis.
"""

import duckdb
from pathlib import Path
from typing import Optional

DB_PATH = Path("electricore/etl/flux_enedis_pipeline.duckdb")
SCHEMA = "flux_enedis"


def query_table(
    table_name: str, 
    filters: Optional[dict] = None, 
    limit: int = 100,
    offset: int = 0
) -> list[dict]:
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
    
    with duckdb.connect(str(DB_PATH), read_only=True) as conn:
        result = conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]


def get_table_info(table_name: str) -> dict:
    """
    Retourne les informations sur une table (colonnes, nombre de lignes).
    
    Args:
        table_name: Nom de la table (sans préfixe flux_)
        
    Returns:
        Dict avec table, count, columns
    """
    with duckdb.connect(str(DB_PATH), read_only=True) as conn:
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
        
        return {
            "table": f"flux_{table_name}",
            "schema": SCHEMA,
            "count": count,
            "columns": columns
        }


def list_tables() -> list[str]:
    """
    Liste toutes les tables flux disponibles.
    
    Returns:
        Liste des noms de tables (sans préfixe flux_)
    """
    with duckdb.connect(str(DB_PATH), read_only=True) as conn:
        tables = conn.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{SCHEMA}' 
            AND table_name LIKE 'flux_%'
            AND table_name NOT LIKE '_dlt%'
            ORDER BY table_name
        """).fetchall()
        
        return [t[0].replace('flux_', '') for t in tables]