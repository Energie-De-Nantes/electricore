"""
Modèle Pandera pour les consommations mensuelles agrégées.

Ce modèle représente les consommations électriques agrégées par PDL et par mois,
issues des lignes de factures Odoo.
"""

import pandera.polars as pa
from pandera.typing.polars import DataFrame


class ConsommationMensuelle(pa.DataFrameModel):
    """
    Schéma des consommations mensuelles agrégées.

    Représente la consommation totale par PDL et par mois de facturation,
    avec le mois de consommation réel (décalage -1 mois).
    """

    # Identifiants
    pdl: str = pa.Field(description="Point de livraison")
    order_name: str = pa.Field(description="Référence commande Odoo")

    # Dates
    mois_consommation: str = pa.Field(
        description="Mois de consommation réel = mois_facturation - 1 mois (YYYY-MM)",
        str_matches=r"^\d{4}-\d{2}$"
    )

    # Énergie
    energie_kwh: float = pa.Field(
        ge=0,
        description="Énergie totale en kWh"
    )

    # Métadonnées
    trimestre: str = pa.Field(
        description="Trimestre de consommation (YYYY-QX)",
        str_matches=r"^\d{4}-Q[1-4]$"
    )

    class Config:
        strict = True
        coerce = True
