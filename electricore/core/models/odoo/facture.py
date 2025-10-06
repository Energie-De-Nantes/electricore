"""
Modèles Pandera pour factures Odoo (account.move).

Schémas de validation pour les factures et lignes de factures.
"""

import polars as pl
import pandera.polars as pa
from pandera.dtypes import DateTime
from typing import Optional


class FactureOdoo(pa.DataFrameModel):
    """
    Modèle Pandera pour les factures Odoo (account.move).

    Validation des factures avec champs critiques pour la facturation électrique.
    """

    # Identifiant
    account_move_id: pl.Int64 = pa.Field(nullable=False)

    # Référence et dates
    name: pl.Utf8 = pa.Field(nullable=False)  # Numéro de facture
    invoice_date: Optional[DateTime] = pa.Field(
        nullable=True,
        dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )
    invoice_date_due: Optional[DateTime] = pa.Field(
        nullable=True,
        dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"}
    )

    # État et type
    state: pl.Utf8 = pa.Field(
        nullable=False,
        isin=["draft", "posted", "cancel"]
    )
    move_type: pl.Utf8 = pa.Field(
        nullable=False,
        isin=["entry", "out_invoice", "out_refund", "in_invoice", "in_refund", "out_receipt", "in_receipt"]
    )

    # Montants
    amount_untaxed: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)
    amount_tax: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)
    amount_total: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)
    amount_residual: Optional[pl.Float64] = pa.Field(nullable=True)

    # Relations (IDs extraits)
    partner_id: Optional[pl.Int64] = pa.Field(nullable=True)

    class Config:
        """Configuration du modèle."""
        strict = False  # Permet colonnes supplémentaires
        coerce = True   # Coercition automatique des types


class LigneFactureOdoo(pa.DataFrameModel):
    """
    Modèle Pandera pour les lignes de factures Odoo (account.move.line).

    Validation des lignes avec détails produits et montants.
    """

    # Identifiant
    account_move_line_id: pl.Int64 = pa.Field(nullable=False)

    # Référence
    name: pl.Utf8 = pa.Field(nullable=False)  # Description

    # Quantités et prix
    quantity: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)
    price_unit: Optional[pl.Float64] = pa.Field(nullable=True, ge=0.0)
    price_subtotal: Optional[pl.Float64] = pa.Field(nullable=True)
    price_total: Optional[pl.Float64] = pa.Field(nullable=True)

    # Relations (IDs extraits)
    move_id: Optional[pl.Int64] = pa.Field(nullable=True)
    product_id: Optional[pl.Int64] = pa.Field(nullable=True)
    account_id: Optional[pl.Int64] = pa.Field(nullable=True)

    # Taxes
    tax_ids: Optional[pl.List] = pa.Field(nullable=True)

    class Config:
        """Configuration du modèle."""
        strict = False  # Permet colonnes supplémentaires
        coerce = True   # Coercition automatique des types