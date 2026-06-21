"""Descripteur unifié d'un flux DuckDB (#389).

Un **seul** type décrit un flux Enedis ou un mart canonique : ses colonnes
(forme SQL via `Column`), sa source (table à projeter **ou** SQL de base
pré-construit pour les marts `SELECT *`), sa transformation Polars résiduelle et
son validateur Pandera.

`FluxDescriptor` remplace l'ancien couple `FluxSchema` (schéma SQL pur) /
`QueryConfig` (appariement schéma + transform + validateur), qui re-déclarait
chaque flux dans **deux registres parallèles** — et que les marts construisaient
en plus *inline* avec une forme différente. Les flux legacy et les marts
(`releves`/`spine`/`chronologie`) utilisent désormais ce même type (ADR-0019).
"""

from collections.abc import Callable
from dataclasses import dataclass

import pandera.polars as pa
import polars as pl

from .sql import Column


@dataclass(frozen=True, slots=True)
class FluxDescriptor:
    """Descripteur immutable complet d'un flux ou d'un mart.

    Attributes:
        flux_name: Nom du flux (ex: "C15", "R151", "RELEVES_CANONIQUES").
        columns: Colonnes projetées (tuple immutable). Vide pour un mart en
            `SELECT *` qui ne se re-déclare pas colonne par colonne.
        table: Table DuckDB à projeter (legacy). `None` pour un mart `base_sql`.
        base_sql: Requête SQL de base pré-construite (CTE/UNION, marts). Si
            renseignée, elle prime sur `table`/`columns` pour le SELECT, et
            l'allowlist de `.filter()` reste ouverte (schéma non re-déclaré).
        where_clause: Clause WHERE optionnelle (legacy, intégrée au SELECT bâti).
        comments: Commentaires SQL optionnels en tête de requête.
        transform: Transformation Polars résiduelle après l'IO. `None` = identité
            (le loader trust les types émis par dbt, ADR-0035).
        validator: Classe Pandera pour validation (optionnel).
    """

    flux_name: str
    columns: tuple[Column, ...] = ()
    table: str | None = None
    base_sql: str | None = None
    where_clause: str | None = None
    comments: str | None = None
    transform: Callable[[pl.LazyFrame], pl.LazyFrame] | None = None
    validator: type[pa.DataFrameModel] | None = None
