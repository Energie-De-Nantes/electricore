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
from electricore.core.pipelines.cta import ajouter_cta
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


def generer_cta_xlsx(trimestre: Optional[str] = None) -> bytes:
    """
    Calcule la CTA et retourne un fichier XLSX multi-onglets.

    Les taux sont chargés automatiquement depuis electricore/config/cta_rules.csv
    et appliqués au niveau mensuel. Un changement de taux en cours de trimestre
    (décret CRE) est donc géré automatiquement dans l'agrégation trimestrielle.

    Args:
        trimestre: Filtre optionnel au format "YYYY-TX" (ex: "2025-T1").
            Si None, retourne toutes les données disponibles.

    Returns:
        Contenu XLSX en bytes avec 3 onglets : Résumé, Par taux, Détail.
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

    df_mensuel = (
        ajouter_cta(
            df_facturation
            .join(df_pdl.select(["pdl", "order_name"]), on="pdl", how="inner")
            .lazy()
        )
        .with_columns(expr_calculer_trimestre().alias("trimestre"))
        .collect()
    )

    if trimestre is not None:
        df_mensuel = df_mensuel.filter(pl.col("trimestre") == trimestre)

    df_detail_cta = (
        df_mensuel.lazy()
        .group_by("pdl")
        .agg([
            pl.col("order_name").first(),
            pl.col("turpe_fixe_eur").sum().round(2).alias("turpe_fixe_total"),
            pl.col("cta_eur").sum().round(2).alias("cta"),
            pl.col("taux_cta_pct").unique().sort().alias("taux_cta_appliques"),
        ])
        .sort("cta", descending=True)
        .collect()
        # Sérialiser la liste des taux en string pour un rendu XLSX lisible
        .with_columns(
            pl.col("taux_cta_appliques")
            .list.eval(pl.element().cast(pl.Utf8))
            .list.join(" ; ")
        )
    )

    df_par_taux = (
        df_mensuel
        .group_by(["trimestre", "taux_cta_pct"])
        .agg([
            pl.col("pdl").n_unique().alias("Nombre de PDL"),
            pl.col("turpe_fixe_eur").sum().round(2).alias("TURPE fixe (€)"),
            pl.col("cta_eur").sum().round(2).alias("CTA (€)"),
        ])
        .sort(["trimestre", "taux_cta_pct"])
        .rename({"taux_cta_pct": "Taux CTA (%)"})
    )

    df_resume = (
        df_mensuel
        .group_by("trimestre")
        .agg([
            pl.col("pdl").n_unique().alias("Nombre de PDL"),
            pl.col("turpe_fixe_eur").sum().round(2).alias("TURPE fixe total (€)"),
            pl.col("cta_eur").sum().round(2).alias("CTA totale (€)"),
        ])
        .sort("trimestre")
    )

    return _construire_xlsx({
        "Résumé": df_resume,
        "Par taux": df_par_taux,
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
