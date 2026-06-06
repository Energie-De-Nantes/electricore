"""Schémas Pandera pour le livrable `rapport_accise` (issue #56).

Trois `DataFrameModel` qui reflètent les onglets XLSX livrés au facturiste pour
la déclaration trimestrielle d'accise TICFE.
"""

import pandera.polars as pa
import polars as pl


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


class RapportAcciseDetail(pa.DataFrameModel):
    """Détail PDL × mois (= sortie de `accise_par_contrat`)."""

    pdl: pl.Utf8 = pa.Field(nullable=False)
    mois_consommation: pl.Utf8 = pa.Field(nullable=False)
    trimestre: pl.Utf8 = pa.Field(nullable=False)
    taux_accise_eur_mwh: pl.Float64 = pa.Field(nullable=False)
    energie_mwh: pl.Float64 = pa.Field(nullable=False)
    accise_eur: pl.Float64 = pa.Field(nullable=False)
