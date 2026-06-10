"""Producteurs de livrables taxes (Accise TICFE, CTA) — builds purs (ADR-0019, issue #108).

`rapport_accise` et `rapport_cta` sont ERP-agnostiques : ils reçoivent leurs
sources en paramètre (LazyFrame Enedis ou DataFrame Odoo-normalisé) et ne
font aucune I/O. Les sources Odoo sont à la charge du caller
(`api/services/taxes_service.py` ou tests).

Les primitives partagées `agreger_par_taux` et `agreger_resume` capturent le
seam commun entre Accise et CTA : même sortie (Résumé / Par taux / Détail),
calculs distincts.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from electricore.core.pipelines.accise import pipeline_accise
from electricore.core.pipelines.cta import ajouter_cta
from electricore.core.pipelines.facturation import expr_calculer_trimestre


@dataclass(frozen=True, slots=True)
class RapportTaxe:
    """Bundle livrable : les 3 onglets XLSX d'un rapport de taxe trimestriel."""

    resume: pl.DataFrame
    par_taux: pl.DataFrame
    detail: pl.DataFrame


def feuilles_rapport_taxe(r: RapportTaxe) -> dict[str, pl.DataFrame]:
    """Mapping onglet → DataFrame pour `xlsx_multi_sheet` (Résumé / Par taux / Détail)."""
    return {"Résumé": r.resume, "Par taux": r.par_taux, "Détail": r.detail}


def agreger_par_taux(
    calc_frame: pl.DataFrame,
    cle_taux: str,
    cle_assiette: str,
    cle_montant: str,
    extra_groupby: tuple[str, ...] = (),
) -> pl.DataFrame:
    """Aggrège un détail de calcul par taux (+ colonnes supplémentaires optionnelles).

    Produit une ligne par combinaison (extra_groupby, cle_taux) avec :
    - somme de l'assiette (arrondie à 3 décimales)
    - somme du montant (arrondie à 2 décimales)
    - nombre de PDL distincts → `nb_pdl`

    Trié croissant par toutes les clés de groupement.
    """
    groupby = list(extra_groupby) + [cle_taux]
    return (
        calc_frame.group_by(groupby)
        .agg(
            [
                pl.col(cle_assiette).sum().round(3),
                pl.col(cle_montant).sum().round(2),
                pl.col("pdl").n_unique().cast(pl.Int64).alias("nb_pdl"),
            ]
        )
        .sort(groupby)
    )


def agreger_resume(
    calc_frame: pl.DataFrame,
    cle_assiette: str,
    cle_montant: str,
    nom_assiette_total: str,
    nom_montant_total: str,
) -> pl.DataFrame:
    """Aggrège un détail de calcul par trimestre (résumé trimestriel).

    Produit une ligne par trimestre avec :
    - nombre de PDL distincts → `nb_pdl`
    - somme de l'assiette renommée en `nom_assiette_total` (arrondie à 3 décimales)
    - somme du montant renommée en `nom_montant_total` (arrondie à 2 décimales)

    Trié croissant par trimestre.
    """
    return (
        calc_frame.group_by("trimestre")
        .agg(
            [
                pl.col("pdl").n_unique().cast(pl.Int64).alias("nb_pdl"),
                pl.col(cle_assiette).sum().round(3).alias(nom_assiette_total),
                pl.col(cle_montant).sum().round(2).alias(nom_montant_total),
            ]
        )
        .sort("trimestre")
    )


def rapport_accise(lignes_factures: pl.LazyFrame, trimestre: str | None = None) -> RapportTaxe:
    """Livrable accise TICFE pur — sans I/O Odoo.

    Args:
        lignes_factures: Sortie de `lignes_factures_taxe(odoo)` (LazyFrame normalisé).
        trimestre: format "YYYY-TX". `None` = tous les trimestres.

    Returns:
        `RapportTaxe(resume, par_taux, detail)` où `detail` est trié
        `(pdl, mois_consommation)`.
    """
    # pipeline_accise retourne déjà un LazyFrame trié (pdl, mois_consommation).
    # Collect au boundary du build (ADR-0019) pour stocker dans RapportTaxe.
    detail = pipeline_accise(lignes_factures).collect()
    if trimestre is not None:
        detail = detail.filter(pl.col("trimestre") == trimestre)

    par_taux = agreger_par_taux(
        detail,
        cle_taux="taux_accise_eur_mwh",
        cle_assiette="energie_mwh",
        cle_montant="accise_eur",
    )
    resume = agreger_resume(
        detail,
        cle_assiette="energie_mwh",
        cle_montant="accise_eur",
        nom_assiette_total="energie_mwh_total",
        nom_montant_total="accise_eur_total",
    )
    return RapportTaxe(resume=resume, par_taux=par_taux, detail=detail)


def rapport_cta(
    facturation_mensuelle: pl.DataFrame,
    pdl_mapping: pl.DataFrame,
    trimestre: str | None = None,
) -> RapportTaxe:
    """Livrable CTA pur — sans I/O Odoo.

    Args:
        facturation_mensuelle: Sortie de `charger(...).facturation_mensuelle`.
        pdl_mapping: DataFrame `{pdl, order_name}` provenant de `mapping_pdl_order(odoo)`.
        trimestre: format "YYYY-TX". `None` = tous les trimestres.

    Returns:
        `RapportTaxe(resume, par_taux, detail)` où `detail` est aggrégé par PDL
        (pas mensuel brut) avec `taux_cta_appliques` (taux successifs string-joined).
    """
    df_mensuel = (
        ajouter_cta(facturation_mensuelle.join(pdl_mapping.select(["pdl", "order_name"]), on="pdl", how="inner").lazy())
        .with_columns(expr_calculer_trimestre().alias("trimestre"))
        .collect()
    )

    if trimestre is not None:
        df_mensuel = df_mensuel.filter(pl.col("trimestre") == trimestre)

    par_taux = agreger_par_taux(
        df_mensuel,
        cle_taux="taux_cta_pct",
        cle_assiette="turpe_fixe_eur",
        cle_montant="cta_eur",
        extra_groupby=("trimestre",),
    )
    resume = agreger_resume(
        df_mensuel,
        cle_assiette="turpe_fixe_eur",
        cle_montant="cta_eur",
        nom_assiette_total="turpe_fixe_total_eur",
        nom_montant_total="cta_total_eur",
    )
    detail = (
        df_mensuel.lazy()
        .group_by("pdl")
        .agg(
            [
                pl.col("order_name").first(),
                pl.col("turpe_fixe_eur").sum().round(2).alias("turpe_fixe_total_eur"),
                pl.col("cta_eur").sum().round(2).alias("cta_total_eur"),
                pl.col("taux_cta_pct").unique().sort().alias("_taux_list"),
            ]
        )
        .sort("cta_total_eur", descending=True)
        .collect()
        .with_columns(
            pl.col("_taux_list").list.eval(pl.element().cast(pl.Utf8)).list.join(" ; ").alias("taux_cta_appliques")
        )
        .drop("_taux_list")
    )

    return RapportTaxe(resume=resume, par_taux=par_taux, detail=detail)
