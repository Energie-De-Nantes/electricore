"""
Query builder fonctionnel immutable pour DuckDB.

Ce module fournit un builder de requêtes suivant les principes fonctionnels :
- Immutabilité totale (dataclass frozen)
- Méthodes chainables retournant de nouvelles instances
- Lazy evaluation (exécution uniquement sur collect/lazy)
- Séparation claire entre construction et exécution
"""

from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pandera.polars as pa
import polars as pl

from .config import DuckDBConfig, duckdb_readonly_conn
from .sql import FluxSchema, build_base_query

# =============================================================================
# CONFIGURATIONS IMMUTABLES
# =============================================================================


@dataclass(frozen=True)  # Immutable
class QueryConfig:
    """
    Configuration immutable d'une requête flux.

    Attributes:
        schema: Schéma SQL du flux
        transform: Fonction de transformation LazyFrame
        validator: Classe Pandera pour validation (optionnel)
    """

    schema: FluxSchema
    transform: Callable[[pl.LazyFrame], pl.LazyFrame]
    validator: type[pa.DataFrameModel] | None = None


# =============================================================================
# QUERY BUILDER IMMUTABLE
# =============================================================================


@dataclass(frozen=True)  # Immutable
class DuckDBQuery:
    """
    Builder fonctionnel immutable pour construire et exécuter des requêtes DuckDB.

    Cette classe suit le pattern builder avec immutabilité totale :
    - Chaque méthode retourne une NOUVELLE instance (pas de mutation)
    - Lazy evaluation : la requête n'est exécutée qu'au moment de collect() ou lazy()
    - API fluide chainable pour une construction progressive

    Example:
        >>> # API fluide chainable
        >>> result = c15().filter({"Date_Evenement": ">= '2024-01-01'"}).limit(100).collect()
        >>>
        >>> # Construction progressive
        >>> query = r151().filter({"pdl": ["PDL123", "PDL456"]})
        >>> query = query.limit(1000)
        >>> lazy_df = query.lazy()
    """

    # Configuration du flux (immutable)
    config: QueryConfig

    # État de la requête (immutable tuples)
    database_path: str | Path | None = None
    filters: tuple[tuple[str, Any], ...] = ()  # Tuple de tuples = immutable
    limit_value: int | None = None
    valider: bool = True
    base_sql: str | None = None  # Pour requêtes CTE/UNION pré-construites

    # =========================================================================
    # MÉTHODES BUILDER (Retournent nouvelles instances)
    # =========================================================================

    def filter(self, filters: dict[str, Any]) -> "DuckDBQuery":
        """
        Ajoute des filtres à la requête.

        Retourne une NOUVELLE instance avec les filtres ajoutés (immutabilité).

        Args:
            filters: Dictionnaire de filtres {colonne: condition}

        Returns:
            Nouvelle instance DuckDBQuery avec les filtres ajoutés

        Example:
            >>> query.filter({"Date_Evenement": ">= '2024-01-01'", "pdl": ["PDL123"]})
        """
        new_filters = self.filters + tuple(filters.items())
        return replace(self, filters=new_filters)

    def where(self, condition: str) -> "DuckDBQuery":
        """
        Ajoute une condition WHERE sous forme de chaîne brute.

        Args:
            condition: Condition SQL brute (ex: "pdl IN ('PDL123', 'PDL456')")

        Returns:
            Nouvelle instance DuckDBQuery avec la condition ajoutée

        Example:
            >>> query.where("date_evenement >= '2024-01-01' AND puissance_souscrite > 6")
        """
        # Utiliser une clé spéciale pour les conditions brutes
        new_filters = self.filters + (("__raw_condition", condition),)
        return replace(self, filters=new_filters)

    def limit(self, count: int) -> "DuckDBQuery":
        """
        Ajoute une limite au nombre de lignes retournées.

        Args:
            count: Nombre maximum de lignes

        Returns:
            Nouvelle instance DuckDBQuery avec la limite

        Example:
            >>> query.limit(1000)
        """
        return replace(self, limit_value=count)

    def validate(self, enable: bool = True) -> "DuckDBQuery":
        """
        Active ou désactive la validation Pandera.

        Args:
            enable: True pour activer la validation

        Returns:
            Nouvelle instance DuckDBQuery avec la configuration de validation
        """
        return replace(self, valider=enable)

    def entrees(self) -> "DuckDBQuery":
        """Filtre sur les codes d'entrée canoniques C15 (PMES, MES, CFNE).

        Raises:
            ValueError: Si le builder n'est pas configuré pour le flux C15.
        """
        from .registry import ENTREES_C15

        self._assert_c15(".entrees()")
        return self.filter({"evenement_declencheur": list(ENTREES_C15)})

    def sorties(self) -> "DuckDBQuery":
        """Filtre sur les codes de sortie canoniques C15 (RES, CFNS).

        Raises:
            ValueError: Si le builder n'est pas configuré pour le flux C15.
        """
        from .registry import SORTIES_C15

        self._assert_c15(".sorties()")
        return self.filter({"evenement_declencheur": list(SORTIES_C15)})

    def _assert_c15(self, method_name: str) -> None:
        """Garde-fou : les groupings entrées/sorties n'existent que pour C15."""
        flux = self.config.schema.flux_name
        if flux != "C15":
            raise ValueError(f"{method_name} ne s'applique qu'au flux C15, reçu : {flux}")

    # =========================================================================
    # CONSTRUCTION SQL (Fonctions pures)
    # =========================================================================

    def _build_filter_clause(self, column: str, condition: Any) -> str:
        """
        Construit une clause WHERE depuis un filtre (fonction pure).

        Args:
            column: Nom de la colonne
            condition: Condition (liste, string avec opérateur, ou valeur simple)

        Returns:
            Clause WHERE formatée
        """
        # Condition brute
        if column == "__raw_condition":
            return condition

        # Liste de valeurs (IN)
        if isinstance(condition, list):
            values = "', '".join(str(v) for v in condition)
            return f"{column} IN ('{values}')"

        # String avec opérateur (>=, <=, etc.)
        if isinstance(condition, str) and any(op in condition for op in [">=", "<=", ">", "<", "="]):
            return f"{column} {condition}"

        # Égalité simple
        return f"{column} = '{condition}'"

    def _build_final_query(self) -> str:
        """
        Construit la requête SQL finale avec filtres et limite (fonction pure).

        Returns:
            Requête SQL complète prête à être exécutée
        """
        # Si base_sql fourni (CTE/UNION), l'utiliser directement
        if self.base_sql:
            query = self.base_sql
        else:
            # Requête de base depuis le schéma
            query = build_base_query(self.config.schema)

        # Ajouter les filtres
        if self.filters:
            where_clauses = [self._build_filter_clause(col, val) for col, val in self.filters]

            # Vérifier si la requête a déjà une clause WHERE
            if "WHERE" in query.upper():
                # Ajouter les conditions avec AND
                query += " AND " + " AND ".join(where_clauses)
            else:
                # Ajouter une nouvelle clause WHERE
                query += "\nWHERE " + " AND ".join(where_clauses)

        # Ajouter la limite
        if self.limit_value:
            query += f"\nLIMIT {self.limit_value}"

        return query

    # =========================================================================
    # EXÉCUTION (Fonctions impures - IO)
    # =========================================================================

    def lazy(self) -> pl.LazyFrame:
        """
        Exécute la requête et retourne un LazyFrame Polars.

        Cette méthode effectue l'IO (impure) et applique les transformations (pures).

        Returns:
            LazyFrame Polars avec les transformations appliquées

        Raises:
            FileNotFoundError: Si la base DuckDB n'existe pas
        """
        # Configuration
        config = DuckDBConfig(self.database_path)

        if not config.database_path.exists():
            raise FileNotFoundError(f"Base DuckDB non trouvée : {config.database_path}")

        # Construction SQL (pure)
        final_query = self._build_final_query()

        # Connexion et exécution (impure - IO)
        with duckdb_readonly_conn(config.database_path) as conn:
            lazy_frame = conn.execute(final_query).pl().lazy()

        # Application des transformations (pure)
        lazy_frame = self.config.transform(lazy_frame)

        # Validation si demandée (impure - side effect)
        if self.valider and self.config.validator is not None:
            sample_df = lazy_frame.limit(100).collect()
            self.config.validator.validate(sample_df)

        return lazy_frame

    def collect(self) -> pl.DataFrame:
        """
        Exécute la requête et retourne un DataFrame Polars concret.

        Returns:
            DataFrame Polars collecté avec les transformations appliquées
        """
        return self.lazy().collect()

    def exec(self) -> pl.DataFrame:
        """
        Exécute la requête et retourne un DataFrame Polars concret.

        .. deprecated::
            Utilisez `.collect()` à la place pour cohérence avec Polars.

        Returns:
            DataFrame Polars collecté avec les transformations appliquées
        """
        return self.collect()


# =============================================================================
# FACTORY GÉNÉRIQUE (Fonction pure)
# =============================================================================


def make_query(config: QueryConfig, database_path: str | Path | None = None) -> DuckDBQuery:
    """
    Factory générique pour créer un DuckDBQuery depuis une configuration.

    Fonction pure : Fn(QueryConfig, Optional[Path]) -> DuckDBQuery

    Args:
        config: Configuration du flux
        database_path: Chemin vers la base DuckDB (optionnel)

    Returns:
        Instance DuckDBQuery configurée

    Example:
        >>> query = make_query(FLUX_CONFIGS["c15"])
    """
    return DuckDBQuery(config=config, database_path=database_path)
