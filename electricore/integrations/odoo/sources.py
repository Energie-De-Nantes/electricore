"""Sources Odoo normalisées pour les taxes énergétiques (ADR-0019, issue #108).

Expose les fonctions d'extraction Odoo qui fournissent les DataFrames attendus
par les builds `core/builds/rapport_taxe.py`. Aucune logique métier — lecture
et normalisation uniquement.
"""

import polars as pl

from .helpers import commandes_lignes, query
from .reader import OdooReader


def lignes_factures_taxe(odoo: OdooReader) -> pl.LazyFrame:
    """Lignes de commandes Odoo normalisées pour le calcul d'accise.

    Equivalent de l'extraction dans l'ancien `accise_par_contrat` :
    retourne le `LazyFrame` brut des lignes de commandes (shape compatible
    avec `pipeline_accise`).

    Args:
        odoo: `OdooReader` déjà ouvert.

    Returns:
        `LazyFrame` des lignes de commandes (colonnes Odoo brutes).
    """
    return commandes_lignes(odoo).lazy()


def mapping_pdl_order(odoo: OdooReader) -> pl.DataFrame:
    """Mapping PDL → nom de commande Odoo (pour enrichissement CTA).

    Extrait les `sale.order` ayant un `x_pdl`, retourne un DataFrame
    `{pdl, order_name}` dédupliqué par PDL.

    Args:
        odoo: `OdooReader` déjà ouvert.

    Returns:
        `DataFrame` avec colonnes `pdl` (str) et `order_name` (str).
    """
    return (
        query(odoo, "sale.order", domain=[("x_pdl", "!=", False)], fields=["name", "x_pdl"])
        .filter(pl.col("x_pdl").is_not_null())
        .select(
            [
                pl.col("x_pdl").str.strip_chars().alias("pdl"),
                pl.col("name").alias("order_name"),
            ]
        )
        .collect()
        .unique("pdl")
    )
