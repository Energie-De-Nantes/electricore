"""Schémas Pandera des onglets du livrable `RapportTaxe` (Accise TICFE, CTA).

Rapatriés d'`integrations/odoo/models/` (issue #116) : ces schémas valident
les sorties des builds ERP-agnostiques `rapport_accise` / `rapport_cta`
(`core/builds/rapport_taxe.py`), qui portent eux-mêmes la validation.

Les noms de champs restent en snake_case ; le mapping vers les libellés
facturiste (« Nombre de PDL », « CTA (€) »…) se fait à la couche de
sérialisation XLSX.

Pas de `RapportAcciseDetail` : l'onglet Détail accise *est* la sortie de
`pipeline_accise`, validée par `AcciseMensuel` (une frame = un schéma).
L'onglet Détail CTA est en revanche un agrégat par PDL distinct du grain
mensuel, d'où `RapportCtaDetail`.
"""

import pandera.polars as pa
import polars as pl

# =============================================================================
# ACCISE TICFE
# =============================================================================


class RapportAcciseResume(pa.DataFrameModel):
    """Totaux trimestriels : un résumé par trimestre."""

    trimestre: pl.Utf8 = pa.Field(nullable=False)
    nb_pdl: pl.Int64 = pa.Field(nullable=False, ge=0)
    energie_mwh_total: pl.Float64 = pa.Field(nullable=False)
    accise_eur_total: pl.Float64 = pa.Field(nullable=False)


class RapportAcciseParTaux(pa.DataFrameModel):
    """Décomposition par taux d'accise applicable."""

    taux_accise_eur_mwh: pl.Float64 = pa.Field(nullable=False)
    energie_mwh: pl.Float64 = pa.Field(nullable=False)
    accise_eur: pl.Float64 = pa.Field(nullable=False)
    nb_pdl: pl.Int64 = pa.Field(nullable=False, ge=0)


# =============================================================================
# CTA
# =============================================================================


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
    """Détail par PDL (= un PDL × order × totaux annuels) — agrégation du grain mensuel.

    Spécificité CTA : `taux_cta_appliques` est une string joined ` ; ` qui
    liste les taux successifs touchés par le PDL au cours de la période
    (utile quand un changement de décret tombe au milieu du trimestre).
    """

    pdl: pl.Utf8 = pa.Field(nullable=False)
    order_name: pl.Utf8 = pa.Field(nullable=False)
    turpe_fixe_total_eur: pl.Float64 = pa.Field(nullable=False)
    cta_total_eur: pl.Float64 = pa.Field(nullable=False)
    taux_cta_appliques: pl.Utf8 = pa.Field(nullable=False)
