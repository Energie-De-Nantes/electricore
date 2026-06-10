"""Schéma Pandera Polars pour le détail CTA agrégé par PDL.

Sortie de `pipeline_cta` (cf. ADR-0019, issue #110) : une ligne par PDL,
avec sommes mensuelles `turpe_fixe_total` / `cta` et liste des taux distincts
rencontrés sur la période (changement de taux intra-trimestre détectable
quand la liste contient plus d'une valeur).
"""

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
