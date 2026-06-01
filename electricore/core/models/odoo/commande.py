"""
Modèles Pandera pour commandes de vente Odoo (sale.order).

Schémas de validation pour les commandes.
"""


import pandera.polars as pa
import polars as pl
from pandera.dtypes import DateTime


class CommandeVenteOdoo(pa.DataFrameModel):
    """
    Modèle Pandera pour les commandes de vente Odoo (sale.order).

    Validation des commandes avec champs critiques pour la gestion commerciale.
    """

    # Identifiant
    sale_order_id: pl.Int64 = pa.Field(nullable=False)

    # Référence et dates
    name: pl.Utf8 = pa.Field(nullable=False)  # Numéro de commande
    date_order: DateTime | None = pa.Field(
        nullable=True,
        dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )
    validity_date: DateTime | None = pa.Field(
        nullable=True,
        dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )

    # État
    state: pl.Utf8 = pa.Field(
        nullable=False,
        isin=["draft", "sent", "sale", "done", "cancel"]
    )

    # Montants
    amount_untaxed: pl.Float64 | None = pa.Field(nullable=True, ge=0.0)
    amount_tax: pl.Float64 | None = pa.Field(nullable=True, ge=0.0)
    amount_total: pl.Float64 | None = pa.Field(nullable=True, ge=0.0)

    # Relations (IDs extraits)
    partner_id: pl.Int64 | None = pa.Field(nullable=True)
    user_id: pl.Int64 | None = pa.Field(nullable=True)  # Commercial

    # Champs métier spécifiques (optionnels)
    x_pdl: pl.Utf8 | None = pa.Field(nullable=True)  # PDL pour électricité

    class Config:
        """Configuration du modèle."""
        strict = False  # Permet colonnes supplémentaires
        coerce = True   # Coercition automatique des types