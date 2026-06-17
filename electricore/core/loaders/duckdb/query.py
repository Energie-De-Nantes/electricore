"""
Query builder fonctionnel immutable pour DuckDB.

Ce module fournit un builder de requêtes suivant les principes fonctionnels :
- Immutabilité totale (dataclass frozen)
- Méthodes chainables retournant de nouvelles instances
- Lazy evaluation (exécution uniquement sur collect/lazy)
- Séparation claire entre construction et exécution
"""

import re
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


@dataclass(frozen=True, slots=True)
class QueryConfig:
    """
    Configuration immutable d'une requête flux.

    Attributes:
        schema: Schéma SQL du flux
        transform: Fonction de transformation LazyFrame
        validator: Classe Pandera pour validation (optionnel)
    """

    schema: FluxSchema
    # None = aucune mise en forme (loader `SELECT *` : il trust les types émis par dbt,
    # ADR-0035). Sinon Fn(LazyFrame) -> LazyFrame appliquée après l'IO.
    transform: Callable[[pl.LazyFrame], pl.LazyFrame] | None
    validator: type[pa.DataFrameModel] | None = None


# =============================================================================
# QUERY BUILDER IMMUTABLE
# =============================================================================


@dataclass(frozen=True, slots=True)
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

        Raises:
            ValueError: Si une colonne n'appartient pas au schéma du flux.

        Example:
            >>> query.filter({"Date_Evenement": ">= '2024-01-01'", "pdl": ["PDL123"]})
        """
        allowed = {c.name for c in self.config.schema.columns}
        # Schémas CTE/UNION (base_sql) sans colonnes typées : on saute l'allowlist
        if allowed:
            unknown = [col for col in filters if col not in allowed]
            if unknown:
                raise ValueError(
                    f"colonne inconnue {unknown!r} pour le flux {self.config.schema.flux_name}. "
                    f"Colonnes valides : {sorted(allowed)}"
                )
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

    _OPERATOR_RE = re.compile(r"^\s*(>=|<=|<>|!=|>|<|=)\s*(.+?)\s*$")

    def _build_filter_clause(self, column: str, condition: Any) -> tuple[str, list[Any]]:
        """Construit une clause WHERE paramétrée depuis un filtre (fonction pure).

        Returns:
            (sql_fragment_avec_?, params) — les valeurs ne sont JAMAIS interpolées
            dans le SQL ; elles transitent par le binding `conn.execute(sql, params)`.
        """
        # Condition brute (échappée, non exposée HTTP, validée allowlist au constructeur)
        if column == "__raw_condition":
            return condition, []

        # Liste de valeurs (IN)
        if isinstance(condition, list):
            placeholders = ", ".join(["?"] * len(condition))
            return f"{column} IN ({placeholders})", list(condition)

        # String avec opérateur préfixe : ">= '2024-01-01'", "< 100", etc.
        if isinstance(condition, str):
            match = self._OPERATOR_RE.match(condition)
            if match:
                op, raw_value = match.group(1), match.group(2)
                # Strip enclosing quotes (simple ou double) si présentes
                if len(raw_value) >= 2 and raw_value[0] == raw_value[-1] and raw_value[0] in ("'", '"'):
                    raw_value = raw_value[1:-1]
                return f"{column} {op} ?", [raw_value]

        # Égalité simple
        return f"{column} = ?", [condition]

    def _build_final_query(self) -> tuple[str, list[Any]]:
        """Construit la requête SQL finale + ses paramètres (fonction pure).

        Returns:
            (sql_avec_placeholders, params) — à passer à `conn.execute(sql, params)`.
        """
        # Si base_sql fourni (CTE/UNION), l'utiliser directement
        if self.base_sql:
            query = self.base_sql
        else:
            query = build_base_query(self.config.schema)

        params: list[Any] = []
        if self.filters:
            fragments = []
            for col, val in self.filters:
                fragment, frag_params = self._build_filter_clause(col, val)
                fragments.append(fragment)
                params.extend(frag_params)

            if "WHERE" in query.upper():
                query += " AND " + " AND ".join(fragments)
            else:
                query += "\nWHERE " + " AND ".join(fragments)

        # LIMIT est un entier injecté en dur — pas une donnée utilisateur, sûr
        if self.limit_value:
            query += f"\nLIMIT {self.limit_value}"

        return query, params

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
        config = DuckDBConfig.from_path(self.database_path)

        if not config.database_path.exists():
            raise FileNotFoundError(f"Base DuckDB non trouvée : {config.database_path}")

        # Construction SQL paramétrée (pure)
        final_query, params = self._build_final_query()

        # Connexion et exécution (impure - IO) ; valeurs liées, jamais interpolées
        with duckdb_readonly_conn(config.database_path) as conn:
            lazy_frame = conn.execute(final_query, params).pl().lazy()

        # Application des transformations (pure). transform=None ⟹ identité (`SELECT *`).
        if self.config.transform is not None:
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
