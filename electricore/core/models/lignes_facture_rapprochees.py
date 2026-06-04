"""Modèle Pandera pour `lignes_facture_rapprochees`.

Résultat du *Rapprochement facturation mensuelle*. Voir
`electricore/core/CONTEXT.md` (section "Intégration Odoo").
"""

import pandera.polars as pa
import polars as pl
from pandera.engines.polars_engine import DateTime


class LignesFactureRapprochees(pa.DataFrameModel):
    """Lignes de facture Odoo enrichies des données Enedis du mois (quantité, méta-période, compteur)."""

    # Identifiants Odoo
    invoice_line_ids: pl.Int64 = pa.Field(nullable=False)
    x_pdl: pl.Utf8 | None = pa.Field(nullable=True)
    x_lisse: pl.Boolean | None = pa.Field(nullable=True)  # défaut Odoo = null, à fill_null(False) côté consommateur
    name_account_move: pl.Utf8 = pa.Field(nullable=False)
    name_product_category: pl.Utf8 | None = pa.Field(nullable=True)
    name_product_product: pl.Utf8 | None = pa.Field(nullable=True)
    quantity: pl.Float64 = pa.Field(nullable=False)

    # Quantité Enedis + mémo
    quantite_enedis: pl.Float64 | None = pa.Field(nullable=True)
    memo_puissance: pl.Utf8 | None = pa.Field(nullable=True)

    # Méta-période Enedis du mois (nullable si pas de match RSC)
    ref_situation_contractuelle: pl.Utf8 | None = pa.Field(nullable=True)
    pdl: pl.Utf8 | None = pa.Field(nullable=True)
    debut: DateTime | None = pa.Field(nullable=True, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    fin: DateTime | None = pa.Field(nullable=True, dtype_kwargs={"time_unit": "us", "time_zone": "Europe/Paris"})
    data_complete: pl.Boolean | None = pa.Field(nullable=True)
    turpe_fixe_eur: pl.Float64 | None = pa.Field(nullable=True)
    turpe_variable_eur: pl.Float64 | None = pa.Field(nullable=True)

    # Identifiants compteur (issus de l'Historique)
    num_compteur: pl.Utf8 | None = pa.Field(nullable=True)
    type_compteur: pl.Utf8 | None = pa.Field(nullable=True)

    # Flags d'état de facturation (cf. ADR-0014, mutuellement exclusifs sur sous-ensemble draft)
    a_facturer: pl.Boolean | None = pa.Field(nullable=True)
    a_supprimer: pl.Boolean | None = pa.Field(nullable=True)

    class Config:
        strict = True
        coerce = True
