"""Modèle Pandera pour `lignes_facture_rapprochees`.

Résultat du *Rapprochement facturation mensuelle*. Voir
`electricore/core/CONTEXT.md` (section "Intégration Odoo").
"""

import pandera.polars as pa
import polars as pl


class LignesFactureRapprochees(pa.DataFrameModel):
    """Lignes de facture Odoo en brouillon enrichies de `quantite_enedis`."""

    invoice_line_ids: pl.Int64 = pa.Field(nullable=False)
    x_pdl: pl.Utf8 = pa.Field(nullable=False)
    x_lisse: pl.Boolean = pa.Field(nullable=False)
    name_account_move: pl.Utf8 = pa.Field(nullable=False)
    name_product_category: pl.Utf8 = pa.Field(nullable=False)
    name_product_product: pl.Utf8 = pa.Field(nullable=False)
    quantity: pl.Float64 = pa.Field(nullable=False)
    quantite_enedis: pl.Float64 | None = pa.Field(nullable=True)
    memo_puissance: pl.Utf8 | None = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
