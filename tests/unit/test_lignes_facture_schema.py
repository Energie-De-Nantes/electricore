"""Tests du schéma agnostique `LignesFacture`.

Voir `core/CONTEXT.md` (entrée *Ligne de facture*). Le schéma est volontairement
minimal : 4 colonnes requises (les seules sur lesquelles `rapprocher()`
branche), le reste passe en passe-plat via `strict=False`.
"""

import pandera.errors
import polars as pl
import pytest

from electricore.core.models.lignes_facture import LignesFacture


def _frame_valide() -> pl.DataFrame:
    """Frame minimaliste respectant le contrat des 4 colonnes requises."""
    return pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC001"],
            "categorie_produit": ["HP"],
            "quantite": [100.0],
            "est_brouillon": [True],
        }
    )


class TestColonnesRequises:
    """Les 4 colonnes du contrat minimal doivent être présentes et typées."""

    def test_frame_avec_les_quatre_colonnes_passe(self):
        LignesFacture.validate(_frame_valide())


class TestCategorieProduitIsin:
    """`categorie_produit` doit appartenir à {Base, HP, HC, Abonnements}."""

    @pytest.mark.parametrize("categorie", ["Base", "HP", "HC", "Abonnements"])
    def test_categorie_valide_passe(self, categorie):
        df = _frame_valide().with_columns(pl.lit(categorie).alias("categorie_produit"))
        LignesFacture.validate(df)

    def test_categorie_inconnue_echoue(self):
        df = _frame_valide().with_columns(pl.lit("Mystère").alias("categorie_produit"))
        with pytest.raises(pandera.errors.SchemaError):
            LignesFacture.validate(df)


class TestPassePlat:
    """`strict=False` doit laisser passer les colonnes ERP additionnelles."""

    def test_colonnes_supplementaires_acceptees(self):
        df = _frame_valide().with_columns(
            [
                pl.lit("PDL00001").alias("pdl"),  # passe-plat clé non requise
                pl.lit(False).alias("est_lisse"),
                pl.lit(101, dtype=pl.Int64).alias("invoice_line_ids"),  # ERP-passthrough
                pl.lit("INV/2025/0001").alias("name_account_move"),
                pl.lit("Énergie HP").alias("name_product_product"),
            ]
        )
        # Ne lève pas — strict=False préserve les colonnes additionnelles
        LignesFacture.validate(df)
