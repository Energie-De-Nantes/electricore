"""Tests pour `lignes_factures_du_mois` et l'utilitaire `flags_etat_facturation`.

Cf. ADR-0014 — la fonction expose toutes les lignes de factures du mois
(quel que soit l'état) avec des flags `a_facturer` et `a_supprimer` calculés.
"""

import polars as pl

from electricore.core.loaders.odoo.helpers import flags_etat_facturation


class TestFlagsEtatFacturation:
    """`a_facturer` et `a_supprimer` dérivent de (x_invoicing_state, state_account_move, quantity)."""

    def test_draft_qty_positive_donne_a_facturer(self):
        lf = pl.LazyFrame(
            {
                "x_invoicing_state": ["draft"],
                "state_account_move": ["draft"],
                "quantity": [100.0],
            }
        )

        resultat = flags_etat_facturation(lf).collect()

        assert resultat["a_facturer"].item() is True
        assert resultat["a_supprimer"].item() is False

    def test_draft_qty_nulle_donne_a_supprimer(self):
        lf = pl.LazyFrame(
            {
                "x_invoicing_state": ["draft"],
                "state_account_move": ["draft"],
                "quantity": [0.0],
            }
        )

        resultat = flags_etat_facturation(lf).collect()

        assert resultat["a_facturer"].item() is False
        assert resultat["a_supprimer"].item() is True

    def test_sale_non_draft_n_est_ni_a_facturer_ni_a_supprimer(self):
        """Une commande déjà validée (x_invoicing_state != 'draft') reste hors scope, peu importe la qty."""
        lf = pl.LazyFrame(
            {
                "x_invoicing_state": ["checked", "populated", "checked"],
                "state_account_move": ["draft", "draft", "posted"],
                "quantity": [100.0, 0.0, 50.0],
            }
        )

        resultat = flags_etat_facturation(lf).collect()

        assert resultat["a_facturer"].to_list() == [False, False, False]
        assert resultat["a_supprimer"].to_list() == [False, False, False]

    def test_move_non_draft_n_est_pas_a_facturer(self):
        """Une facture validée (state_account_move != 'draft') reste hors scope."""
        lf = pl.LazyFrame(
            {
                "x_invoicing_state": ["draft", "draft"],
                "state_account_move": ["posted", "cancel"],
                "quantity": [100.0, 0.0],
            }
        )

        resultat = flags_etat_facturation(lf).collect()

        assert resultat["a_facturer"].to_list() == [False, False]
        assert resultat["a_supprimer"].to_list() == [False, False]
