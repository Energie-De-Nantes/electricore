"""Schémas Pandera pour le livrable `rapport_cta` (issue #63).

Trois `DataFrameModel` qui reflètent les onglets XLSX livrés au facturiste pour
la déclaration trimestrielle CTA.

Les noms de champs restent en snake_case ; le mapping vers les libellés
facturiste (« Nombre de PDL », « CTA (€) »…) se fait à la couche de
sérialisation XLSX, comme pour `rapport_accise`.
"""

import pandera.polars as pa
import polars as pl


class RapportCtaResume(pa.DataFrameModel):
    """Totaux trimestriels : un résumé par trimestre."""

    trimestre: pl.Utf8 = pa.Field(nullable=False)
    nb_pdl: pl.Int64 = pa.Field(nullable=False, ge=0)
    turpe_fixe_total_eur: pl.Float64 = pa.Field(nullable=False)
    cta_total_eur: pl.Float64 = pa.Field(nullable=False)


class RapportCtaParTaux(pa.DataFrameModel):
    """Décomposition par (trimestre, taux CTA applicable).

    Le décret CRE peut faire varier le taux en cours de trimestre ;
    `taux_cta_pct` est dimensionné mensuellement avant agrégation.
    """

    trimestre: pl.Utf8 = pa.Field(nullable=False)
    taux_cta_pct: pl.Float64 = pa.Field(nullable=False)
    nb_pdl: pl.Int64 = pa.Field(nullable=False, ge=0)
    turpe_fixe_eur: pl.Float64 = pa.Field(nullable=False)
    cta_eur: pl.Float64 = pa.Field(nullable=False)


class RapportCtaDetail(pa.DataFrameModel):
    """Détail par PDL (= un PDL × order × totaux annuels) — agrégation du brut mensuel.

    Spécificité CTA : `taux_cta_appliques` est une string joined ` ; ` qui
    liste les taux successifs touchés par le PDL au cours de la période
    (utile quand un changement de décret tombe au milieu du trimestre).
    """

    pdl: pl.Utf8 = pa.Field(nullable=False)
    order_name: pl.Utf8 = pa.Field(nullable=False)
    turpe_fixe_total_eur: pl.Float64 = pa.Field(nullable=False)
    cta_total_eur: pl.Float64 = pa.Field(nullable=False)
    taux_cta_appliques: pl.Utf8 = pa.Field(nullable=False)
