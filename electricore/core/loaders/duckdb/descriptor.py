"""Descripteur unifié d'un flux DuckDB (#389).

Depuis la bascule ADR-0042, tous les flux utilisent `SELECT *` — la forme
résiduelle (types, renommages, littéraux) vit dans les modèles dbt. Les champs
`columns` et `comments` (machinerie de projection) ont été retirés (#506).

`FluxDescriptor` décrit : sa source (`table` pour les flux bruts ou `base_sql`
pour les marts CTE/UNION), sa transformation Polars résiduelle et son validateur Pandera.
Les flux legacy et les marts (`releves`/`spine_contrat`/`chronologie_releves`)
utilisent ce même type (ADR-0019).
"""

from collections.abc import Callable
from dataclasses import dataclass

import pandera.polars as pa
import polars as pl


@dataclass(frozen=True, slots=True)
class FluxDescriptor:
    """Descripteur immutable complet d'un flux ou d'un mart.

    Attributes:
        flux_name: Nom du flux (ex: "C15", "R151", "RELEVES_CANONIQUES").
        table: Table DuckDB à projeter. `None` pour un mart `base_sql`.
        base_sql: Requête SQL de base pré-construite (CTE/UNION, marts). Si
            renseignée, elle prime sur `table` pour le SELECT, et l'allowlist de
            `.filter()` reste ouverte (schéma non re-déclaré).
        where_clause: Clause WHERE optionnelle.
        transform: Transformation Polars résiduelle après l'IO. `None` = identité
            (le loader trust les types émis par dbt, ADR-0035).
        validator: Classe Pandera pour validation (optionnel).
    """

    flux_name: str
    table: str | None = None
    base_sql: str | None = None
    where_clause: str | None = None
    transform: Callable[[pl.LazyFrame], pl.LazyFrame] | None = None
    validator: type[pa.DataFrameModel] | None = None
