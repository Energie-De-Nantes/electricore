"""
Schéma d'un historique versionné de taux réglementé (Accise, CTA, …).

Le nom de la colonne de taux varie selon la taxe (`taux_accise_eur_mwh`,
`taux_cta_pct`, …) ; le schéma est donc construit dynamiquement via
`HistoriqueTaux.to_schema(taux_col)`.
"""

import polars as pl
from pandera.polars import Column, DataFrameSchema


class HistoriqueTaux:
    """
    Schéma Pandera d'un historique de taux versionné par date d'entrée en vigueur.

    Colonnes attendues :
    - `start` : datetime[μs, Europe/Paris], non-null — date d'entrée en vigueur du taux.
    - `{taux_col}` : Float64, non-null — valeur du taux (nom paramétrable).

    Chaque ligne représente l'entrée en vigueur d'un nouveau taux qui remplace
    le précédent jusqu'à la ligne suivante.
    """

    @staticmethod
    def to_schema(taux_col: str) -> DataFrameSchema:
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
