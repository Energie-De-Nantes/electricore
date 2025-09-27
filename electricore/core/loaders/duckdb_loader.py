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


# Requêtes SQL de base pour chaque flux
BASE_QUERY_C15 = """
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
    CAST(avant_hp AS DOUBLE) as avant_hp,
    CAST(avant_hc AS DOUBLE) as avant_hc,
    CAST(avant_hch AS DOUBLE) as avant_hch,
    CAST(avant_hph AS DOUBLE) as avant_hph,
    CAST(avant_hpb AS DOUBLE) as avant_hpb,
    CAST(avant_hcb AS DOUBLE) as avant_hcb,
    CAST(avant_base AS DOUBLE) as avant_base,
    -- Colonnes de relevés "Après"
    apres_date_releve,
    apres_nature_index,
    apres_id_calendrier_fournisseur,
    apres_id_calendrier_distributeur,
    CAST(apres_hp AS DOUBLE) as apres_hp,
    CAST(apres_hc AS DOUBLE) as apres_hc,
    CAST(apres_hch AS DOUBLE) as apres_hch,
    CAST(apres_hph AS DOUBLE) as apres_hph,
    CAST(apres_hpb AS DOUBLE) as apres_hpb,
    CAST(apres_hcb AS DOUBLE) as apres_hcb,
    CAST(apres_base AS DOUBLE) as apres_base,
    -- Métadonnées
    'flux_C15' as source
FROM flux_enedis.flux_c15
"""

BASE_QUERY_R151 = """
SELECT
    CAST(date_releve AS TIMESTAMP) as date_releve,
    pdl,
    CAST(NULL AS VARCHAR) as ref_situation_contractuelle,
    CAST(NULL AS VARCHAR) as formule_tarifaire_acheminement,
    id_calendrier_fournisseur,
    id_calendrier_distributeur,
    id_affaire,
    CAST(hp AS DOUBLE) as hp,
    CAST(hc AS DOUBLE) as hc,
    CAST(hch AS DOUBLE) as hch,
    CAST(hph AS DOUBLE) as hph,
    CAST(hpb AS DOUBLE) as hpb,
    CAST(hcb AS DOUBLE) as hcb,
    CAST(base AS DOUBLE) as base,
    'flux_R151' as source,
    FALSE as ordre_index,
    unite,
    unite as precision
FROM flux_enedis.flux_r151
WHERE date_releve IS NOT NULL
"""

BASE_QUERY_R15 = """
SELECT
    date_releve,
    pdl,
    ref_situation_contractuelle,
    CAST(NULL AS VARCHAR) as formule_tarifaire_acheminement,
    CAST(NULL AS VARCHAR) as id_calendrier_fournisseur,
    id_calendrier as id_calendrier_distributeur,
    id_affaire,
    CAST(hp AS DOUBLE) as hp,
    CAST(hc AS DOUBLE) as hc,
    CAST(hch AS DOUBLE) as hch,
    CAST(hph AS DOUBLE) as hph,
    CAST(hpb AS DOUBLE) as hpb,
    CAST(hcb AS DOUBLE) as hcb,
    CAST(base AS DOUBLE) as base,
    'flux_R15' as source,
    FALSE as ordre_index,
    'kWh' as unite,
    'kWh' as precision
FROM flux_enedis.flux_r15
WHERE date_releve IS NOT NULL
"""

BASE_QUERY_F15 = """
SELECT
    CAST(date_facture AS TIMESTAMP) as date_facture,
    pdl,
    num_facture,
    type_facturation,
    ref_situation_contractuelle,
    type_compteur,
    id_ev,
    nature_ev,
    formule_tarifaire_acheminement,
    taux_tva_applicable,
    unite,
    CAST(prix_unitaire AS DOUBLE) as prix_unitaire,
    CAST(quantite AS DOUBLE) as quantite,
    CAST(montant_ht AS DOUBLE) as montant_ht,
    CAST(date_debut AS TIMESTAMP) as date_debut,
    CAST(date_fin AS TIMESTAMP) as date_fin,
    libelle_ev,
    'flux_F15' as source
FROM flux_enedis.flux_f15_detail
"""

BASE_QUERY_RELEVES_UNIFIES = """
WITH releves_unifies AS (
    -- Flux R151 (relevés périodiques)
    SELECT
        CAST(date_releve AS TIMESTAMP) as date_releve,
        pdl,
        CAST(NULL AS VARCHAR) as ref_situation_contractuelle,
        CAST(NULL AS VARCHAR) as formule_tarifaire_acheminement,
        id_calendrier_fournisseur,
        id_calendrier_distributeur,
        id_affaire,
        CAST(hp AS DOUBLE) as hp,
        CAST(hc AS DOUBLE) as hc,
        CAST(hch AS DOUBLE) as hch,
        CAST(hph AS DOUBLE) as hph,
        CAST(hpb AS DOUBLE) as hpb,
        CAST(hcb AS DOUBLE) as hcb,
        CAST(base AS DOUBLE) as base,
        'flux_R151' as source,
        FALSE as ordre_index,
        unite,
        unite as precision
    FROM flux_enedis.flux_r151
    WHERE date_releve IS NOT NULL

    UNION ALL

    -- Flux R15 (relevés avec événements)
    SELECT
        date_releve,
        pdl,
        ref_situation_contractuelle,
        CAST(NULL AS VARCHAR) as formule_tarifaire_acheminement,
        CAST(NULL AS VARCHAR) as id_calendrier_fournisseur,
        id_calendrier as id_calendrier_distributeur,
        id_affaire,
        CAST(hp AS DOUBLE) as hp,
        CAST(hc AS DOUBLE) as hc,
        CAST(hch AS DOUBLE) as hch,
        CAST(hph AS DOUBLE) as hph,
        CAST(hpb AS DOUBLE) as hpb,
        CAST(hcb AS DOUBLE) as hcb,
        CAST(base AS DOUBLE) as base,
        'flux_R15' as source,
        FALSE as ordre_index,
        'kWh' as unite,
        'kWh' as precision
    FROM flux_enedis.flux_r15
    WHERE date_releve IS NOT NULL
)
SELECT * FROM releves_unifies
"""


from ..models.releve_index import RelevéIndex
from ..models.historique_perimetre import HistoriquePérimètre


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
            self.database_path = Path("electricore/etl/flux_enedis_pipeline.duckdb")
        else:
            self.database_path = Path(database_path)

        # Mapping des tables DuckDB vers schémas métier
        self.table_mappings = {
            "historique_perimetre": {
                "source_tables": ["flux_enedis.flux_c15"],
                "description": "Historique des événements contractuels avec relevés avant/après"
            },
            "releves": {
                "source_tables": ["flux_enedis.flux_r151", "flux_enedis.flux_r15"],
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


def _build_where_clauses(filters: Optional[Dict[str, Any]]) -> List[str]:
    """
    Construit les clauses WHERE à partir d'un dictionnaire de filtres.

    Args:
        filters: Dictionnaire de filtres {colonne: condition}

    Returns:
        Liste des clauses WHERE formatées

    Examples:
        >>> _build_where_clauses({"pdl": ["PDL123", "PDL456"]})
        ["pdl IN ('PDL123', 'PDL456')"]

        >>> _build_where_clauses({"Date_Evenement": ">= '2024-01-01'"})
        ["Date_Evenement >= '2024-01-01'"]
    """
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
    return where_clauses


class QueryBuilder:
    """
    Builder fonctionnel pour construire et exécuter des requêtes DuckDB.

    Cette classe suit le pattern builder avec une approche fonctionnelle :
    - Immutabilité : chaque méthode retourne une nouvelle instance
    - Méthodes chainables pour une API fluide
    - Lazy evaluation : la requête n'est exécutée qu'au moment de exec() ou lazy()

    Example:
        >>> # API fluide chainable
        >>> result = c15().filter({"Date_Evenement": ">= '2024-01-01'"}).limit(100).exec()
        >>>
        >>> # Construction progressive
        >>> query = r151().filter({"pdl": ["PDL123", "PDL456"]})
        >>> query = query.limit(1000)
        >>> lazy_df = query.lazy()
    """

    def __init__(self,
                 base_query: str,
                 transform_func: callable,
                 validator_class: type = None,
                 database_path: Union[str, Path] = None,
                 filters: Optional[Dict[str, Any]] = None,
                 limit_value: Optional[int] = None,
                 valider: bool = True):
        """
        Initialise un QueryBuilder.

        Args:
            base_query: Requête SQL de base sans WHERE ni LIMIT
            transform_func: Fonction de transformation du LazyFrame
            validator_class: Classe Pandera pour la validation (optionnel)
            database_path: Chemin vers la base DuckDB
            filters: Filtres à appliquer
            limit_value: Limite du nombre de lignes
            valider: Active la validation Pandera
        """
        self._base_query = base_query
        self._transform_func = transform_func
        self._validator_class = validator_class
        self._database_path = database_path
        self._filters = filters or {}
        self._limit_value = limit_value
        self._valider = valider

    def filter(self, filters: Dict[str, Any]) -> 'QueryBuilder':
        """
        Ajoute des filtres à la requête.

        Args:
            filters: Dictionnaire de filtres {colonne: condition}

        Returns:
            Nouvelle instance QueryBuilder avec les filtres ajoutés

        Example:
            >>> query.filter({"Date_Evenement": ">= '2024-01-01'", "pdl": ["PDL123"]})
        """
        new_filters = {**self._filters, **filters}
        return QueryBuilder(
            self._base_query,
            self._transform_func,
            self._validator_class,
            self._database_path,
            new_filters,
            self._limit_value,
            self._valider
        )

    def where(self, condition: str) -> 'QueryBuilder':
        """
        Ajoute une condition WHERE sous forme de chaîne brute.

        Args:
            condition: Condition SQL brute (ex: "pdl IN ('PDL123', 'PDL456')")

        Returns:
            Nouvelle instance QueryBuilder avec la condition ajoutée

        Example:
            >>> query.where("date_evenement >= '2024-01-01' AND puissance_souscrite > 6")
        """
        # Pour les conditions brutes, on utilise une clé spéciale
        new_filters = {**self._filters, f"__raw_condition_{len(self._filters)}": condition}
        return QueryBuilder(
            self._base_query,
            self._transform_func,
            self._validator_class,
            self._database_path,
            new_filters,
            self._limit_value,
            self._valider
        )

    def limit(self, count: int) -> 'QueryBuilder':
        """
        Ajoute une limite au nombre de lignes retournées.

        Args:
            count: Nombre maximum de lignes

        Returns:
            Nouvelle instance QueryBuilder avec la limite

        Example:
            >>> query.limit(1000)
        """
        return QueryBuilder(
            self._base_query,
            self._transform_func,
            self._validator_class,
            self._database_path,
            self._filters,
            count,
            self._valider
        )

    def validate(self, enable: bool = True) -> 'QueryBuilder':
        """
        Active ou désactive la validation Pandera.

        Args:
            enable: True pour activer la validation

        Returns:
            Nouvelle instance QueryBuilder avec la configuration de validation
        """
        return QueryBuilder(
            self._base_query,
            self._transform_func,
            self._validator_class,
            self._database_path,
            self._filters,
            self._limit_value,
            enable
        )

    def _build_final_query(self) -> str:
        """
        Construit la requête SQL finale avec filtres et limite.

        Returns:
            Requête SQL complète prête à être exécutée
        """
        query = self._base_query

        # Construire les clauses WHERE
        where_clauses = []
        for key, condition in self._filters.items():
            if key.startswith("__raw_condition_"):
                # Condition brute
                where_clauses.append(condition)
            else:
                # Condition structurée
                if isinstance(condition, list):
                    values = "', '".join(str(v) for v in condition)
                    where_clauses.append(f"{key} IN ('{values}')")
                elif isinstance(condition, str) and any(op in condition for op in ['>=', '<=', '>', '<', '=']):
                    where_clauses.append(f"{key} {condition}")
                else:
                    where_clauses.append(f"{key} = '{condition}'")

        if where_clauses:
            # Vérifier si la requête a déjà une clause WHERE
            if "WHERE" in query.upper():
                # Ajouter les conditions avec AND
                query += " AND " + " AND ".join(where_clauses)
            else:
                # Ajouter une nouvelle clause WHERE
                query += " WHERE " + " AND ".join(where_clauses)

        if self._limit_value:
            query += f" LIMIT {self._limit_value}"

        return query

    def lazy(self) -> pl.LazyFrame:
        """
        Exécute la requête et retourne un LazyFrame Polars.

        Returns:
            LazyFrame Polars avec les transformations appliquées
        """
        config = DuckDBConfig(self._database_path)

        if not config.database_path.exists():
            raise FileNotFoundError(f"Base DuckDB non trouvée : {config.database_path}")

        final_query = self._build_final_query()

        # Connexion et exécution
        with duckdb_connection(config.database_path) as conn:
            lazy_frame = pl.read_database(
                query=final_query,
                connection=conn
            ).lazy()

        # Application des transformations
        lazy_frame = self._transform_func(lazy_frame)

        # Validation si demandée
        if self._valider and self._validator_class is not None:
            sample_df = lazy_frame.limit(100).collect()
            self._validator_class.validate(sample_df)

        return lazy_frame

    def exec(self) -> pl.DataFrame:
        """
        Exécute la requête et retourne un DataFrame Polars concret.

        Returns:
            DataFrame Polars collecté avec les transformations appliquées
        """
        return self.lazy().collect()


# ============================================================
# API fluide - Fonctions factory
# ============================================================

def c15(database_path: Union[str, Path] = None) -> QueryBuilder:
    """
    Crée un QueryBuilder pour les données flux C15 (historique périmètre).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        QueryBuilder configuré pour flux C15

    Example:
        >>> # Récupérer les événements récents
        >>> df = c15().filter({"Date_Evenement": ">= '2024-01-01'"}).limit(100).exec()
        >>>
        >>> # Filtrer par PDL spécifiques
        >>> lazy_df = c15().filter({"pdl": ["PDL123", "PDL456"]}).lazy()
    """
    return QueryBuilder(
        base_query=BASE_QUERY_C15,
        transform_func=_transform_historique_perimetre,
        validator_class=HistoriquePérimètre,
        database_path=database_path
    )


def r151(database_path: Union[str, Path] = None) -> QueryBuilder:
    """
    Crée un QueryBuilder pour les données flux R151 (relevés périodiques).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        QueryBuilder configuré pour flux R151

    Example:
        >>> # Relevés récents avec limite
        >>> df = r151().filter({"date_releve": ">= '2024-01-01'"}).limit(1000).exec()
    """
    return QueryBuilder(
        base_query=BASE_QUERY_R151,
        transform_func=_transform_releves,
        validator_class=RelevéIndex,
        database_path=database_path
    )


def r15(database_path: Union[str, Path] = None) -> QueryBuilder:
    """
    Crée un QueryBuilder pour les données flux R15 (relevés avec événements).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        QueryBuilder configuré pour flux R15

    Example:
        >>> # Relevés avec situation contractuelle spécifique
        >>> df = r15().filter({"ref_situation_contractuelle": "REF123"}).exec()
    """
    return QueryBuilder(
        base_query=BASE_QUERY_R15,
        transform_func=_transform_releves,
        validator_class=RelevéIndex,
        database_path=database_path
    )


def f15(database_path: Union[str, Path] = None) -> QueryBuilder:
    """
    Crée un QueryBuilder pour les données flux F15 (factures détaillées).

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        QueryBuilder configuré pour flux F15

    Example:
        >>> # Factures pour un PDL spécifique
        >>> df = f15().filter({"pdl": "PDL123"}).exec()
        >>>
        >>> # Factures sur une période
        >>> df = f15().filter({"date_facture": ">= '2024-01-01'"}).limit(100).exec()
    """
    return QueryBuilder(
        base_query=BASE_QUERY_F15,
        transform_func=_transform_factures,
        validator_class=None,  # Pas encore de modèle Pandera pour les factures
        database_path=database_path
    )


def releves(database_path: Union[str, Path] = None) -> QueryBuilder:
    """
    Crée un QueryBuilder pour les relevés unifiés (R151 + R15).

    Cette fonction combine automatiquement les données des flux R151 et R15
    dans un format unifié, équivalent à load_releves().

    Args:
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        QueryBuilder configuré pour les relevés unifiés

    Example:
        >>> # Tous les relevés récents
        >>> df = releves().filter({"date_releve": ">= '2024-01-01'"}).exec()
        >>>
        >>> # Relevés par source
        >>> r151_only = releves().filter({"source": "flux_R151"}).exec()
    """
    return QueryBuilder(
        base_query=BASE_QUERY_RELEVES_UNIFIES,
        transform_func=_transform_releves,
        validator_class=RelevéIndex,
        database_path=database_path
    )


# ============================================================
# Fonctions de compatibilité (legacy API)
# ============================================================

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
    # Utilise le QueryBuilder en interne pour éviter la duplication
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
    # Utilise le QueryBuilder en interne pour éviter la duplication
    query_builder = releves(database_path).validate(valider)

    if filters:
        query_builder = query_builder.filter(filters)

    if limit:
        query_builder = query_builder.limit(limit)

    return query_builder.lazy()


def _transform_historique_perimetre(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Transforme les données DuckDB pour conformité avec HistoriquePérimètre.

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
    Transforme les données DuckDB pour conformité avec RelevéIndex.

    Inclut la conversion Wh -> kWh avec troncature pour ne compter que les kWh complets.

    Args:
        lf: LazyFrame source depuis DuckDB

    Returns:
        LazyFrame transformé conforme au modèle
    """
    # Colonnes d'index numériques à convertir
    index_cols = ["base", "hp", "hc", "hph", "hpb", "hcb", "hch"]

    return lf.with_columns([
        # Conversion des dates avec timezone Europe/Paris
        pl.col("date_releve").dt.convert_time_zone("Europe/Paris"),

        # Conversion Wh -> kWh avec troncature (comprehension list)
        *[
            pl.when(pl.col("unite") == "Wh")
            .then(
                pl.when(pl.col(col).is_not_null())
                .then((pl.col(col) / 1000).floor())
                .otherwise(pl.col(col))
            )
            .otherwise(pl.col(col))
            .alias(col)
            for col in index_cols
        ],

        # Mettre à jour l'unité après conversion
        pl.when(pl.col("unite") == "Wh")
        .then(pl.lit("kWh"))
        .otherwise(pl.col("unite"))
        .alias("unite"),

        # Mettre à jour la précision après conversion
        pl.when(pl.col("precision") == "Wh")
        .then(pl.lit("kWh"))
        .otherwise(pl.col("precision"))
        .alias("precision")
    ])


def _transform_factures(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Transforme les données DuckDB pour les factures F15.

    Args:
        lf: LazyFrame source depuis DuckDB

    Returns:
        LazyFrame transformé avec dates au timezone Europe/Paris
    """
    return lf.with_columns([
        # Conversion des dates avec timezone Europe/Paris
        pl.col("date_facture").dt.convert_time_zone("Europe/Paris"),
        pl.col("date_debut").dt.convert_time_zone("Europe/Paris"),
        pl.col("date_fin").dt.convert_time_zone("Europe/Paris"),
    ])


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