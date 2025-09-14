"""
Chargeur DuckDB pour pipelines ElectriCore.

Ce module fournit des fonctions pour charger les données depuis DuckDB
vers les pipelines Polars avec validation Pandera.
"""

import polars as pl
from pathlib import Path
from typing import Union, Optional, Dict, Any, List
from contextlib import contextmanager
import yaml

import duckdb


try:
    from ..models_polars.releve_index_polars import RelevéIndexPolars
    from ..models_polars.historique_perimetre_polars import HistoriquePérimètrePolars
    POLARS_MODELS_AVAILABLE = True
except ImportError:
    # Les modèles Polars ne sont pas encore disponibles
    POLARS_MODELS_AVAILABLE = False
    RelevéIndexPolars = None
    HistoriquePérimètrePolars = None


class DuckDBConfig:
    """Configuration pour les connexions DuckDB."""

    def __init__(self, database_path: Union[str, Path] = None):
        """
        Initialise la configuration DuckDB.

        Args:
            database_path: Chemin vers la base DuckDB. Si None, utilise la config par défaut.
        """
        if database_path is None:
            # Utiliser la base par défaut du projet
            self.database_path = Path("electricore/etl/flux_enedis.duckdb")
        else:
            self.database_path = Path(database_path)

        # Mapping des tables DuckDB vers schémas métier
        self.table_mappings = {
            "historique_perimetre": {
                "source_tables": ["enedis_production.flux_c15"],
                "description": "Historique des événements contractuels avec relevés avant/après"
            },
            "releves": {
                "source_tables": ["enedis_production.flux_r151", "enedis_production.flux_r15"],
                "description": "Relevés de compteurs unifiés depuis R151 et R15"
            }
        }


@contextmanager
def duckdb_connection(database_path: Union[str, Path]):
    """
    Context manager pour connexions DuckDB.

    Args:
        database_path: Chemin vers la base DuckDB

    Yields:
        duckdb.DuckDBPyConnection: Connexion active
    """
    conn = None
    try:
        conn = duckdb.connect(str(database_path), read_only=True)
        yield conn
    finally:
        if conn:
            conn.close()


def load_historique_perimetre(
    database_path: Union[str, Path] = None,
    filters: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    valider: bool = True
) -> pl.LazyFrame:
    """
    Charge l'historique de périmètre depuis DuckDB.

    Args:
        database_path: Chemin vers la base DuckDB
        filters: Filtres SQL optionnels (ex: {"pdl": ["PDL123", "PDL456"]})
        limit: Limite du nombre de lignes
        valider: Active la validation Pandera

    Returns:
        LazyFrame Polars contenant l'historique de périmètre

    Example:
        >>> lf = load_historique_perimetre(
        ...     filters={"Date_Evenement": ">= '2024-01-01'"},
        ...     limit=1000
        ... )
        >>> df = lf.collect()
    """
    config = DuckDBConfig(database_path)

    if not config.database_path.exists():
        raise FileNotFoundError(f"Base DuckDB non trouvée : {config.database_path}")

    # Construction de la requête SQL de base pour historique périmètre
    base_query = """
    SELECT
        date_evenement,
        pdl,
        ref_situation_contractuelle,
        segment_clientele,
        etat_contractuel,
        evenement_declencheur,
        type_evenement,
        categorie,
        CAST(puissance_souscrite AS DOUBLE) as puissance_souscrite,
        formule_tarifaire_acheminement,
        type_compteur,
        num_compteur,
        ref_demandeur,
        id_affaire,
        -- Colonnes de relevés "Avant"
        avant_date_releve,
        avant_nature_index,
        avant_id_calendrier_fournisseur,
        avant_id_calendrier_distributeur,
        CAST(avant_hp AS DOUBLE) as avant_HP,
        CAST(avant_hc AS DOUBLE) as avant_HC,
        CAST(avant_hch AS DOUBLE) as avant_HCH,
        CAST(avant_hph AS DOUBLE) as avant_HPH,
        CAST(avant_hpb AS DOUBLE) as avant_HPB,
        CAST(avant_hcb AS DOUBLE) as avant_HCB,
        CAST(avant_base AS DOUBLE) as avant_BASE,
        -- Colonnes de relevés "Après"
        apres_date_releve,
        apres_nature_index,
        apres_id_calendrier_fournisseur,
        apres_id_calendrier_distributeur,
        CAST(apres_hp AS DOUBLE) as apres_HP,
        CAST(apres_hc AS DOUBLE) as apres_HC,
        CAST(apres_hch AS DOUBLE) as apres_HCH,
        CAST(apres_hph AS DOUBLE) as apres_HPH,
        CAST(apres_hpb AS DOUBLE) as apres_HPB,
        CAST(apres_hcb AS DOUBLE) as apres_HCB,
        CAST(apres_base AS DOUBLE) as apres_BASE,
        -- Métadonnées
        'flux_C15' as source
    FROM enedis_production.flux_c15
    """

    # Ajout des filtres
    where_clauses = []
    if filters:
        for column, condition in filters.items():
            if isinstance(condition, list):
                # Liste de valeurs
                values = "', '".join(str(v) for v in condition)
                where_clauses.append(f"{column} IN ('{values}')")
            elif isinstance(condition, str) and any(op in condition for op in ['>=', '<=', '>', '<', '=']):
                # Condition avec opérateur
                where_clauses.append(f"{column} {condition}")
            else:
                # Égalité simple
                where_clauses.append(f"{column} = '{condition}'")

    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)

    if limit:
        base_query += f" LIMIT {limit}"

    # Connexion et exécution
    with duckdb_connection(config.database_path) as conn:
        lazy_frame = pl.read_database(
            query=base_query,
            connection=conn
        ).lazy()

    # Application des transformations pour conformité Pandera
    lazy_frame = _transform_historique_perimetre(lazy_frame)

    # Validation si demandée (seulement si les modèles Polars sont disponibles)
    if valider and POLARS_MODELS_AVAILABLE and HistoriquePérimètrePolars is not None:
        # Note: La validation Pandera se fait sur des DataFrames concrets,
        # donc on collecte temporairement pour valider puis on retourne le LazyFrame
        sample_df = lazy_frame.limit(100).collect()
        HistoriquePérimètrePolars.validate(sample_df)

    return lazy_frame


def load_releves(
    database_path: Union[str, Path] = None,
    filters: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    valider: bool = True
) -> pl.LazyFrame:
    """
    Charge les relevés d'index depuis DuckDB.

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
    config = DuckDBConfig(database_path)

    if not config.database_path.exists():
        raise FileNotFoundError(f"Base DuckDB non trouvée : {config.database_path}")

    # Requête de base pour les relevés R151 et R15
    base_query = """
    WITH releves_unifies AS (
        -- Flux R151 (relevés périodiques)
        SELECT
            CAST(date_releve AS TIMESTAMP) as date_releve,
            pdl,
            CAST(NULL AS VARCHAR) as ref_situation_contractuelle,  -- Pas dans R151
            CAST(NULL AS VARCHAR) as formule_tarifaire_acheminement,
            id_calendrier_fournisseur,
            id_calendrier_distributeur,
            id_affaire,
            CAST(hp AS DOUBLE) as HP,
            CAST(hc AS DOUBLE) as HC,
            CAST(hch AS DOUBLE) as HCH,
            CAST(hph AS DOUBLE) as HPH,
            CAST(hpb AS DOUBLE) as HPB,
            CAST(hcb AS DOUBLE) as HCB,
            CAST(base AS DOUBLE) as BASE,
            'flux_R151' as source,
            FALSE as ordre_index,
            unite,
            unite as precision
        FROM enedis_production.flux_r151
        WHERE date_releve IS NOT NULL

        UNION ALL

        -- Flux R15 (relevés avec événements)
        SELECT
            date_releve,
            pdl,
            ref_situation_contractuelle,
            CAST(NULL AS VARCHAR) as formule_tarifaire_acheminement,  -- Pas dans R15
            CAST(NULL AS VARCHAR) as id_calendrier_fournisseur,  -- Pas dans R15
            id_calendrier as id_calendrier_distributeur,
            id_affaire,
            CAST(hp AS DOUBLE) as HP,
            CAST(hc AS DOUBLE) as HC,
            CAST(hch AS DOUBLE) as HCH,
            CAST(hph AS DOUBLE) as HPH,
            CAST(hpb AS DOUBLE) as HPB,
            CAST(hcb AS DOUBLE) as HCB,
            CAST(base AS DOUBLE) as BASE,
            'flux_R15' as source,
            FALSE as ordre_index,
            'kWh' as unite,
            'kWh' as precision
        FROM enedis_production.flux_r15
        WHERE date_releve IS NOT NULL
    )
    SELECT * FROM releves_unifies
    """

    # Ajout des filtres (même logique que load_historique_perimetre)
    where_clauses = []
    if filters:
        for column, condition in filters.items():
            if isinstance(condition, list):
                values = "', '".join(str(v) for v in condition)
                where_clauses.append(f"{column} IN ('{values}')")
            elif isinstance(condition, str) and any(op in condition for op in ['>=', '<=', '>', '<', '=']):
                where_clauses.append(f"{column} {condition}")
            else:
                where_clauses.append(f"{column} = '{condition}'")

    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)

    if limit:
        base_query += f" LIMIT {limit}"

    # Connexion et exécution
    with duckdb_connection(config.database_path) as conn:
        lazy_frame = pl.read_database(
            query=base_query,
            connection=conn
        ).lazy()

    # Application des transformations
    lazy_frame = _transform_releves(lazy_frame)

    # Validation si demandée (seulement si les modèles Polars sont disponibles)
    if valider and POLARS_MODELS_AVAILABLE and RelevéIndexPolars is not None:
        sample_df = lazy_frame.limit(100).collect()
        RelevéIndexPolars.validate(sample_df)

    return lazy_frame


def _transform_historique_perimetre(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Transforme les données DuckDB pour conformité avec HistoriquePérimètrePolars.

    Args:
        lf: LazyFrame source depuis DuckDB

    Returns:
        LazyFrame transformé conforme au modèle
    """
    return lf.with_columns([
        # Conversion des dates avec timezone Europe/Paris
        pl.col("date_evenement").dt.convert_time_zone("Europe/Paris"),
        pl.col("avant_date_releve").dt.convert_time_zone("Europe/Paris"),
        pl.col("apres_date_releve").dt.convert_time_zone("Europe/Paris"),

        # Ajout de colonnes optionnelles manquantes avec valeurs par défaut
        pl.lit("kWh").alias("unite"),
        pl.lit("kWh").alias("precision"),
    ])


def _transform_releves(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Transforme les données DuckDB pour conformité avec RelevéIndexPolars.

    Args:
        lf: LazyFrame source depuis DuckDB

    Returns:
        LazyFrame transformé conforme au modèle
    """
    return lf.with_columns([
        # Conversion des dates avec timezone Europe/Paris
        pl.col("date_releve").dt.convert_time_zone("Europe/Paris"),

        # Les autres colonnes sont déjà préparées dans la requête SQL
        # ordre_index, Unité, Précision, Source sont déjà définies
    ])


def get_available_tables(database_path: Union[str, Path] = None) -> List[str]:
    """
    Liste les tables disponibles dans la base DuckDB.

    Args:
        database_path: Chemin vers la base DuckDB

    Returns:
        Liste des noms de tables avec schéma (ex: ["enedis_production.flux_c15"])
    """
    config = DuckDBConfig(database_path)

    with duckdb_connection(config.database_path) as conn:
        # Utiliser information_schema qui fonctionne mieux avec les schémas
        result = conn.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema != 'information_schema'
            ORDER BY table_schema, table_name
        """).fetchall()

        # Retourner les noms complets avec schéma
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