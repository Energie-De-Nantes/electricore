"""Schéma Pandera Polars pour le détail Accise TICFE agrégé par PDL × mois.

Sortie de `pipeline_accise` (cf. ADR-0019, issue #110) : une ligne par
(PDL, mois de consommation), avec énergie consommée, taux en vigueur,
et montant de l'accise. La colonne `trimestre` permet de filtrer/agréger
au grain trimestriel pour les déclarations.
"""

import pandera.polars as pa
import polars as pl


class AcciseDetail(pa.DataFrameModel):
    """Détail Accise TICFE agrégé par PDL × mois de consommation."""

    pdl: pl.Utf8 = pa.Field(nullable=False)
    mois_consommation: pl.Utf8 = pa.Field(nullable=False)  # ex: "2025-03"
    trimestre: pl.Utf8 = pa.Field(nullable=False)  # ex: "2025-T1"
    order_name: pl.Utf8 = pa.Field(nullable=False)
    energie_kwh: pl.Float64 = pa.Field(nullable=False, ge=0.0)
    energie_mwh: pl.Float64 = pa.Field(nullable=False, ge=0.0)
    taux_accise_eur_mwh: pl.Float64 = pa.Field(nullable=False, ge=0.0)
    accise_eur: pl.Float64 = pa.Field(nullable=False, ge=0.0)

    class Config:
        strict = False
        coerce = True
