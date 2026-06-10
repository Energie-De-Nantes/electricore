"""Schéma Pandera Polars pour le détail CTA agrégé par PDL."""

import pandera.polars as pa
import polars as pl


class CtaDetail(pa.DataFrameModel):
    """Détail CTA agrégé par PDL pour un trimestre (ou toute la période)."""

    pdl: pl.Utf8 = pa.Field(nullable=False)
    order_name: pl.Utf8 = pa.Field(nullable=False)
    turpe_fixe_total: pl.Float64 = pa.Field(nullable=False, ge=0.0)
    cta: pl.Float64 = pa.Field(nullable=False, ge=0.0)
    # Liste des taux CTA distincts (en pourcent) rencontrés sur la période,
    # triée ascendante. `> 1` valeur ⇒ changement de taux intra-période.
    taux_cta_appliques: pl.List(pl.Float64) = pa.Field(nullable=False)

    class Config:
        strict = False
        coerce = True
