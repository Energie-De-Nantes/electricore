"""
Pipeline de calcul de la CTA (Contribution au Tarif d'Acheminement).

La CTA est une taxe calculée sur le TURPE fixe facturé aux clients.
Elle s'applique par trimestre et est exprimée en pourcentage du TURPE fixe total.

Formule : CTA = TURPE_fixe_total × (taux_cta / 100)
"""

import polars as pl

from electricore.core.pipelines.facturation import expr_calculer_trimestre


def pipeline_cta(
    df_facturation: pl.DataFrame,
    df_pdl: pl.DataFrame,
    taux_cta: float,
    trimestre: str | None = None,
) -> pl.DataFrame:
    """
    Calcule la CTA par PDL à partir du résultat du pipeline facturation.

    Args:
        df_facturation: Sortie de facturation().facturation.
            Doit contenir les colonnes : pdl, debut (datetime), turpe_fixe_eur.
        df_pdl: Mapping PDL → order_name issu d'Odoo (sale.order avec x_pdl).
            Doit contenir les colonnes : pdl, order_name.
        taux_cta: Taux de la CTA en pourcentage (ex: 21.93 pour 21.93%).
        trimestre: Filtre optionnel au format "YYYY-TX" (ex: "2025-T1").
            Si None, retourne tous les trimestres disponibles.

    Returns:
        DataFrame avec les colonnes : pdl, order_name, turpe_fixe_total, cta.
        Trié par cta décroissant.
    """
    df = (
        df_facturation
        .join(df_pdl.select(["pdl", "order_name"]), on="pdl", how="inner")
        .with_columns(expr_calculer_trimestre().alias("trimestre"))
    )

    if trimestre is not None:
        df = df.filter(pl.col("trimestre") == trimestre)

    return (
        df
        .group_by("pdl")
        .agg([
            pl.col("turpe_fixe_eur").sum().alias("turpe_fixe_total"),
            pl.col("order_name").first(),
        ])
        .with_columns(
            (pl.col("turpe_fixe_total") * taux_cta / 100).round(2).alias("cta")
        )
        .sort("cta", descending=True)
    )
