"""
Service de calcul des taxes énergétiques (Accise TICFE et CTA).

Orchestre le chargement des données Odoo/DuckDB, l'exécution des pipelines,
et la génération des fichiers XLSX.
"""

import io
from typing import Optional

import polars as pl
import xlsxwriter

from electricore.api.config import settings
from electricore.core.loaders import (
    OdooReader,
    c15,
    commandes_lignes,
    query,
    releves_harmonises,
)
from electricore.core.pipelines.accise import pipeline_accise
from electricore.core.pipelines.cta import pipeline_cta
from electricore.core.pipelines.facturation import expr_calculer_trimestre
from electricore.core.pipelines.orchestration import facturation


def generer_accise_xlsx(trimestre: Optional[str] = None) -> bytes:
    """
    Calcule l'accise TICFE et retourne un fichier XLSX multi-onglets.

    Args:
        trimestre: Filtre optionnel au format "YYYY-TX" (ex: "2025-T1").
            Si None, retourne toutes les données disponibles.

    Returns:
        Contenu XLSX en bytes avec 3 onglets : Résumé, Par taux, Détail.
    """
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        df_lignes = commandes_lignes(odoo).collect()

    df_accise = pipeline_accise(df_lignes.lazy())

    if trimestre is not None:
        df_accise = df_accise.filter(pl.col("trimestre") == trimestre)

    df_par_taux = (
        df_accise
        .group_by("taux_accise_eur_mwh")
        .agg([
            pl.col("energie_mwh").sum().round(3),
            pl.col("accise_eur").sum().round(2),
            pl.col("pdl").n_unique().alias("nb_pdl"),
        ])
        .sort("taux_accise_eur_mwh", descending=True)
    )

    df_resume = (
        df_accise
        .group_by("trimestre")
        .agg([
            pl.col("pdl").n_unique().alias("Nombre de PDL"),
            pl.col("energie_mwh").sum().round(3).alias("Énergie totale (MWh)"),
            pl.col("accise_eur").sum().round(2).alias("Accise totale (€)"),
        ])
        .sort("trimestre")
    )

    return _construire_xlsx({
        "Résumé": df_resume,
        "Par taux": df_par_taux,
        "Détail": df_accise.sort(["pdl", "mois_consommation"]),
    })


def generer_cta_xlsx(trimestre: Optional[str] = None, taux_cta: float = 21.93) -> bytes:
    """
    Calcule la CTA et retourne un fichier XLSX multi-onglets.

    Args:
        trimestre: Filtre optionnel au format "YYYY-TX" (ex: "2025-T1").
            Si None, retourne toutes les données disponibles.
        taux_cta: Taux de la CTA en pourcentage (défaut 2025 : 21.93%).

    Returns:
        Contenu XLSX en bytes avec 2 onglets : Résumé, Détail.
    """
    odoo_config = settings.get_odoo_config()

    with OdooReader(config=odoo_config) as odoo:
        df_pdl = (
            query(odoo, "sale.order",
                  domain=[("x_pdl", "!=", False)],
                  fields=["name", "x_pdl"])
            .filter(pl.col("x_pdl").is_not_null())
            .select([
                pl.col("x_pdl").str.strip_chars().alias("pdl"),
                pl.col("name").alias("order_name"),
            ])
            .collect()
            .unique("pdl")
        )

    lf_historique = c15().lazy()
    lf_releves = releves_harmonises().lazy()
    _, _, _, df_facturation = facturation(historique=lf_historique, releves=lf_releves)

    df_detail_cta = pipeline_cta(
        df_facturation=df_facturation,
        df_pdl=df_pdl,
        taux_cta=taux_cta,
        trimestre=trimestre,
    )

    df_resume = (
        df_facturation
        .join(df_pdl.select(["pdl"]), on="pdl", how="inner")
        .with_columns(expr_calculer_trimestre().alias("trimestre"))
        .pipe(lambda df: df.filter(pl.col("trimestre") == trimestre) if trimestre is not None else df)
        .group_by("trimestre")
        .agg([
            pl.col("pdl").n_unique().alias("Nombre de PDL"),
            pl.col("turpe_fixe_eur").sum().round(2).alias("TURPE fixe total (€)"),
        ])
        .with_columns(
            (pl.col("TURPE fixe total (€)") * taux_cta / 100).round(2).alias("CTA totale (€)"),
            pl.lit(taux_cta).alias("Taux CTA (%)"),
        )
        .sort("trimestre")
        .select(["trimestre", "Nombre de PDL", "TURPE fixe total (€)", "Taux CTA (%)", "CTA totale (€)"])
    )

    return _construire_xlsx({
        "Résumé": df_resume,
        "Détail": df_detail_cta,
    })


def _construire_xlsx(onglets: dict[str, pl.DataFrame]) -> bytes:
    """
    Construit un fichier XLSX multi-onglets à partir d'un dict {nom: DataFrame}.

    Returns:
        Contenu XLSX en bytes.
    """
    buf = io.BytesIO()
    wb = xlsxwriter.Workbook(buf, {"remove_timezone": True})
    for nom, df in onglets.items():
        df.write_excel(workbook=wb, worksheet=nom)
    wb.close()
    return buf.getvalue()
