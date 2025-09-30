"""
Fonctions helpers pour DuckDB - API fonctionnelle.

Ce module fournit des shortcuts pour créer des DuckDBQuery sur les flux
Enedis les plus courants, ainsi que des utilitaires pour interagir avec DuckDB.

Toutes les fonctions factory sont pures : elles prennent des paramètres
optionnels et retournent un DuckDBQuery configuré.
"""

import polars as pl
from pathlib import Path
from typing import Union, Dict, Any, Optional, List

from .config import DuckDBConfig, duckdb_connection
from .query import DuckDBQuery, QueryConfig, make_query
from .registry import FLUX_CONFIGS
from .sql import BASE_QUERY_RELEVES_UNIFIES, BASE_QUERY_RELEVES_HARMONISES, FluxSchema
from .transforms import transform_releves, transform_releves_harmonises

# Imports de validation
from electricore.core.models.releve_index import RelevéIndex


# =============================================================================
# API FLUIDE - FONCTIONS FACTORY PAR FLUX
# =============================================================================

def c15(database_path: Union[str, Path] = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour les données flux C15 (historique périmètre).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour flux C15

    Example:
        >>> # Récupérer les événements récents
        >>> df = c15().filter({"Date_Evenement": ">= '2024-01-01'"}).limit(100).collect()
        >>>
        >>> # Filtrer par PDL spécifiques
        >>> lazy_df = c15().filter({"pdl": ["PDL123", "PDL456"]}).lazy()
    """
    return make_query(FLUX_CONFIGS["c15"], database_path)


def r151(database_path: Union[str, Path] = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour les données flux R151 (relevés périodiques).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour flux R151

    Example:
        >>> # Relevés récents avec limite
        >>> df = r151().filter({"date_releve": ">= '2024-01-01'"}).limit(1000).collect()
    """
    return make_query(FLUX_CONFIGS["r151"], database_path)


def r15(database_path: Union[str, Path] = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour les données flux R15 (relevés avec événements).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour flux R15

    Example:
        >>> # Relevés avec situation contractuelle spécifique
        >>> df = r15().filter({"ref_situation_contractuelle": "REF123"}).collect()
    """
    return make_query(FLUX_CONFIGS["r15"], database_path)


def f15(database_path: Union[str, Path] = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour les données flux F15 (factures détaillées).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour flux F15

    Example:
        >>> # Factures pour un PDL spécifique
        >>> df = f15().filter({"pdl": "PDL123"}).collect()
        >>>
        >>> # Factures sur une période
        >>> df = f15().filter({"date_facture": ">= '2024-01-01'"}).limit(100).collect()
    """
    return make_query(FLUX_CONFIGS["f15"], database_path)


def r64(database_path: Union[str, Path] = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour les données flux R64 (relevés JSON timeseries).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour flux R64

    Example:
        >>> # Relevés R64 récents
        >>> df = r64().filter({"date_releve": ">= '2024-01-01'"}).limit(100).collect()
        >>>
        >>> # Filtrer par PDL et type de relevé
        >>> df = r64().filter({"pdl": "PDL123", "type_releve": "AQ"}).collect()
    """
    return make_query(FLUX_CONFIGS["r64"], database_path)


def releves(database_path: Union[str, Path] = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour les relevés unifiés (R151 + R15).

    Cette fonction combine automatiquement les données des flux R151 et R15
    dans un format unifié.

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour les relevés unifiés

    Example:
        >>> # Tous les relevés récents
        >>> df = releves().filter({"date_releve": ">= '2024-01-01'"}).collect()
        >>>
        >>> # Relevés par source
        >>> r151_only = releves().filter({"source": "flux_R151"}).collect()
    """
    # Schéma factice pour UNION (la requête est déjà complète)
    union_schema = FluxSchema(
        flux_name="RELEVES_UNIFIES",
        table="",  # Pas utilisé (requête CTE)
        columns=()  # Pas utilisé (requête CTE)
    )

    config = QueryConfig(
        schema=union_schema,
        transform=transform_releves,
        validator=RelevéIndex
    )

    # Utiliser base_sql pour la requête CTE pré-construite
    return DuckDBQuery(
        config=config,
        database_path=database_path,
        base_sql=BASE_QUERY_RELEVES_UNIFIES
    )


def releves_harmonises(database_path: Union[str, Path] = None) -> DuckDBQuery:
    """
    Crée un DuckDBQuery pour les relevés harmonisés (R151 + R64).

    Cette fonction unifie les 2 flux de relevés quotidiens :
    - R151 : Relevés périodiques XML (particuliers et petites entreprises)
    - R64 : Relevés JSON timeseries (gros consommateurs industriels)

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        DuckDBQuery configuré pour les relevés harmonisés

    Example:
        >>> # Tous les relevés harmonisés (R151 + R64 seulement)
        >>> df = releves_harmonises().collect()
        >>>
        >>> # Relevés par flux d'origine
        >>> df = releves_harmonises().filter({"flux_origine": "R64"}).collect()
    """
    # Schéma factice pour UNION
    union_schema = FluxSchema(
        flux_name="RELEVES_HARMONISES",
        table="",
        columns=()
    )

    config = QueryConfig(
        schema=union_schema,
        transform=transform_releves_harmonises,
        validator=RelevéIndex
    )

    # Utiliser base_sql pour la requête CTE pré-construite
    return DuckDBQuery(
        config=config,
        database_path=database_path,
        base_sql=BASE_QUERY_RELEVES_HARMONISES
    )


# =============================================================================
# API LEGACY (Compatibilité avec ancien code)
# =============================================================================

def load_historique_perimetre(
    database_path: Union[str, Path] = None,
    filters: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    valider: bool = True
) -> pl.LazyFrame:
    """
    Charge l'historique de périmètre depuis DuckDB.

    .. deprecated::
        Utilisez `c15()` avec l'API fluide à la place.

    Args:
        database_path: Chemin vers la base DuckDB
        filters: Filtres SQL optionnels
        limit: Limite du nombre de lignes
        valider: Active la validation Pandera

    Returns:
        LazyFrame Polars contenant l'historique de périmètre

    Example:
        >>> lf = load_historique_perimetre(
        ...     filters={"Date_Evenement": ">= '2024-01-01'"},
        ...     limit=1000
        ... )
    """
    query_builder = c15(database_path).validate(valider)

    if filters:
        query_builder = query_builder.filter(filters)

    if limit:
        query_builder = query_builder.limit(limit)

    return query_builder.lazy()


def load_releves(
    database_path: Union[str, Path] = None,
    filters: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    valider: bool = True
) -> pl.LazyFrame:
    """
    Charge les relevés d'index depuis DuckDB.

    .. deprecated::
        Utilisez `releves()` avec l'API fluide à la place.

    Args:
        database_path: Chemin vers la base DuckDB
        filters: Filtres SQL optionnels
        limit: Limite du nombre de lignes
        valider: Active la validation Pandera

    Returns:
        LazyFrame Polars contenant les relevés

    Example:
        >>> lf = load_releves(
        ...     filters={"pdl": ["PDL123"], "Source": "flux_R151"},
        ...     limit=1000
        ... )
    """
    query_builder = releves(database_path).validate(valider)

    if filters:
        query_builder = query_builder.filter(filters)

    if limit:
        query_builder = query_builder.limit(limit)

    return query_builder.lazy()


# =============================================================================
# UTILITAIRES
# =============================================================================

def get_available_tables(database_path: Union[str, Path] = None) -> List[str]:
    """
    Liste les tables disponibles dans la base DuckDB.

    Args:
        database_path: Chemin vers la base DuckDB

    Returns:
        Liste des noms de tables avec schéma (ex: ["flux_enedis.flux_c15"])
    """
    config = DuckDBConfig(database_path)

    with duckdb_connection(config.database_path) as conn:
        result = conn.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema != 'information_schema'
            ORDER BY table_schema, table_name
        """).fetchall()

        return [f"{schema}.{table}" for schema, table in result]


def execute_custom_query(
    query: str,
    database_path: Union[str, Path] = None,
    lazy: bool = True
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Exécute une requête SQL personnalisée sur DuckDB.

    Args:
        query: Requête SQL à exécuter
        database_path: Chemin vers la base DuckDB
        lazy: Si True, retourne un LazyFrame, sinon un DataFrame

    Returns:
        DataFrame ou LazyFrame selon le paramètre lazy
    """
    config = DuckDBConfig(database_path)

    with duckdb_connection(config.database_path) as conn:
        if lazy:
            return pl.read_database(
                query=query,
                connection=conn
            ).lazy()
        else:
            return pl.read_database(
                query=query,
                connection=conn
            )
