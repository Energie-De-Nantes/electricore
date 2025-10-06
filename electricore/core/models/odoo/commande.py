"""
Modèles Pandera pour commandes de vente Odoo (sale.order).

Schémas de validation pour les commandes.
"""

import polars as pl
import pandera.polars as pa
from pandera.dtypes import DateTime
from typing import Optional


class CommandeVenteOdoo(pa.DataFrameModel):
    """
    Modèle Pandera pour les commandes de vente Odoo (sale.order).

    Validation des commandes avec champs critiques pour la gestion commerciale.
    """

    # Identifiant
    sale_order_id: pl.Int64 = pa.Field(nullable=False)

    # Référence et dates
    name: pl.Utf8 = pa.Field(nullable=False)  # Numéro de commande
    date_order: Optional[DateTime] = pa.Field(
        nullable=True,
        dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )
    validity_date: Optional[DateTime] = pa.Field(
        nullable=True,
        dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )

    # État
    state: pl.Utf8 = pa.Field(
        nullable=False,
        isin=["draft", "sent", "sale", "done", "cancel"]
    )

    # Montants
    amount_untaxed: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)
    amount_tax: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)
    amount_total: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)

    # Relations (IDs extraits)
    partner_id: Optional[pl.Int64] = pa.Field(nullable=True)
    user_id: Optional[pl.Int64] = pa.Field(nullable=True)  # Commercial

    # Champs métier spécifiques (optionnels)
    x_pdl: Optional[pl.Utf8] = pa.Field(nullable=True)  # PDL pour électricité

    class Config:
        """Configuration du modèle."""
        strict = False  # Permet colonnes supplémentaires
        coerce = True   # Coercition automatique des types