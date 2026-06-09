"""
Schéma d'un historique versionné de taux réglementé (Accise, CTA, …).

Le nom de la colonne de taux varie selon la taxe (`taux_accise_eur_mwh`,
`taux_cta_pct`, …) ; le schéma est donc construit dynamiquement via
`historique_taux_schema(taux_col)`.
"""

import polars as pl
from pandera.polars import Column, DataFrameSchema


def historique_taux_schema(taux_col: str) -> DataFrameSchema:
    """Schéma Pandera d'un historique de taux versionné par date d'entrée en vigueur."""
    return DataFrameSchema(
        {
            "start": Column(
                pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
                nullable=False,
            ),
            taux_col: Column(pl.Float64, nullable=False),
        },
        strict=False,
    )
