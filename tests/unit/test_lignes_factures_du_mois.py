"""Tests pour la surface `est_brouillon` côté adaptateur Odoo.

Depuis slice 2 de la refonte Contexte mensuel (#88), l'adaptateur Odoo expose
uniquement `est_brouillon` (= `x_invoicing_state == 'draft' ∧ state_account_move
== 'draft'`). Les flags ADR-0014 (`a_facturer`, `a_supprimer`) sont dérivés en
core par `rapprocher()` à partir de `est_brouillon` + `quantite`.
"""

import polars as pl

from electricore.integrations.odoo.sources import _expr_est_brouillon


class TestSurfaceEstBrouillon:
    """`est_brouillon` = double draft : `x_invoicing_state` ET `account.move.state`."""

    def test_double_draft_donne_est_brouillon_true(self):
        lf = pl.LazyFrame(
            {
                "x_invoicing_state": ["draft"],
                "state_account_move": ["draft"],
            }
        )

        resultat = lf.with_columns(_expr_est_brouillon()).collect()

        assert resultat["est_brouillon"].item() is True

    def test_x_invoicing_state_non_draft_donne_false(self):
        """Commande déjà validée → hors scope, peu importe `state_account_move`."""
        lf = pl.LazyFrame(
            {
                "x_invoicing_state": ["checked", "populated", "checked"],
                "state_account_move": ["draft", "draft", "posted"],
            }
        )

        resultat = lf.with_columns(_expr_est_brouillon()).collect()

        assert resultat["est_brouillon"].to_list() == [False, False, False]

    def test_state_account_move_non_draft_donne_false(self):
        """Facture validée (posted/cancel) → hors scope."""
        lf = pl.LazyFrame(
            {
                "x_invoicing_state": ["draft", "draft"],
                "state_account_move": ["posted", "cancel"],
            }
        )

        resultat = lf.with_columns(_expr_est_brouillon()).collect()

        assert resultat["est_brouillon"].to_list() == [False, False]
