"""Parité de typage dbt↔cœur (ADR-0035) : table de correspondance SQL↔Polars
et helper de comparaison de schémas.

Garde-fou de frontière : au lieu de *copier* le type dans deux langages (SQL côté
dbt, dtype Polars côté Pandera) et d'espérer qu'ils restent synchronisés, on
*prouve* leur parité. Un test de frontière compare le schéma réellement émis par
dbt au schéma Pandera attendu. C'est un **fil de détente**, pas un oracle : il dit
*qu'*on a divergé, jamais *qui* a raison — la table de propriété d'ADR-0035 tranche.

Voir [ADR-0035](../../../docs/adr/0035-typage-chaine-ingestion-coeur-proprietaire-par-fait.md).
La pièce à conviction : ADR-0034 (index Wh→kWh perdu silencieusement quand
`releves()` est devenu `SELECT *`) faute de garde-fou à cette frontière.
"""

from collections.abc import Iterable

import polars as pl

# Table de correspondance SQL (DuckDB) ↔ Polars. Le **seul** endroit du système qui
# connaît les deux systèmes de types (ADR-0035 §2). La comparaison se fait au niveau
# du *type de base* (`base_type()`) : la nullabilité (axe par couche : dbt `not_null`
# + Pandera `nullable`) et les paramètres temporels (time_unit / time_zone, harmonisés
# au boundary loader) sont hors périmètre — un schéma Polars n'encode pas la
# nullabilité, et le fuseau n'est pas porté par le type SQL `timestamptz`.
SQL_VERS_POLARS: dict[str, pl.DataType] = {
    "BIGINT": pl.Int64,
    "VARCHAR": pl.String,
    "BOOLEAN": pl.Boolean,
    "DOUBLE": pl.Float64,
    "TIMESTAMP WITH TIME ZONE": pl.Datetime,
}


class TypeSQLInconnu(KeyError):
    """Type SQL absent de la table de correspondance : la table doit être l'unique
    endroit qui connaît les deux langages, donc tout type émis par dbt doit y figurer."""


def dtypes_pandera(modele) -> dict[str, pl.DataType]:
    """Extrait les dtypes Polars d'un modèle Pandera (au type de base).

    Args:
        modele: une sous-classe `pandera.polars.DataFrameModel`.

    Returns:
        `{nom_colonne: dtype_polars_de_base}`.
    """
    return {nom: colonne.dtype.type.base_type() for nom, colonne in modele.to_schema().columns.items()}


def ecarts_de_typage(
    schema_sql: dict[str, str], modele, *, ignore: Iterable[str] = ()
) -> dict[str, tuple[pl.DataType, pl.DataType]]:
    """Compare un schéma émis par dbt à un contrat Pandera (dtypes, nullabilité exclue).

    Compare l'**intersection** des colonnes : celles que le contrat cœur déclare ET que
    dbt émet. Une colonne présente d'un seul côté relève de la *présence* (colonne
    tolérée / nullable), pas de la parité de *type* — hors périmètre ici (ADR-0035 §5).

    Args:
        schema_sql: `{nom_colonne: type_sql_dbt}`, tel que renvoyé par `DESCRIBE`.
        modele: contrat Pandera (`pandera.polars.DataFrameModel`).
        ignore: colonnes à ne pas comparer.

    Returns:
        `{colonne: (dtype_dbt, dtype_pandera)}` pour chaque divergence. Vide = parité.

    Raises:
        TypeSQLInconnu: un type SQL d'une colonne comparée n'est pas dans la table.
    """
    attendu = dtypes_pandera(modele)
    ignore = set(ignore)
    ecarts: dict[str, tuple[pl.DataType, pl.DataType]] = {}
    for colonne, type_sql in schema_sql.items():
        if colonne in ignore or colonne not in attendu:
            continue
        if type_sql not in SQL_VERS_POLARS:
            raise TypeSQLInconnu(
                f"Type SQL {type_sql!r} (colonne {colonne!r}) absent de SQL_VERS_POLARS — "
                f"ajouter la correspondance dans parite_typage.py."
            )
        dtype_dbt = SQL_VERS_POLARS[type_sql].base_type()
        dtype_pandera = attendu[colonne]
        if dtype_dbt != dtype_pandera:
            ecarts[colonne] = (dtype_dbt, dtype_pandera)
    return ecarts
