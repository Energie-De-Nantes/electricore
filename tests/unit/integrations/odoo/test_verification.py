"""Tests pour integrations/odoo/verification.py."""

from unittest.mock import MagicMock, patch

import polars as pl
from polars.testing import assert_frame_equal

from electricore.integrations.odoo.verification import (
    ResultatVerification,
    _cfne_manquante,
    _factures_draft,
    _invoicing_state_counts,
    _lisses_quantite_1,
    _rsc_manquante,
    verifier,
)


def _chain_mock(df: pl.DataFrame) -> MagicMock:
    """Mock OdooQuery chain dont .collect() retourne df."""
    m = MagicMock()
    m.follow.return_value = m
    m.enrich.return_value = m
    m.filter.return_value = m
    m.collect.return_value = df
    return m


MODULE = "electricore.integrations.odoo.verification"


class TestRscManquante:
    @patch(f"{MODULE}.query")
    def test_retourne_dataframe_depuis_odoo(self, mock_query):
        odoo = MagicMock()
        expected = pl.DataFrame({"sale_order_id": [1], "name": ["SO1"], "x_pdl": ["PDL1"]})
        mock_query.return_value = _chain_mock(expected)
        result = _rsc_manquante(odoo)
        assert_frame_equal(result, expected)


class TestCfneManquante:
    @patch(f"{MODULE}.query")
    def test_retourne_dataframe_depuis_odoo(self, mock_query):
        odoo = MagicMock()
        expected = pl.DataFrame({"sale_order_id": [2], "name": ["SO2"], "x_pdl": ["PDL2"]})
        mock_query.return_value = _chain_mock(expected)
        result = _cfne_manquante(odoo)
        assert_frame_equal(result, expected)


class TestInvoicingStateCounts:
    @patch(f"{MODULE}.query")
    def test_groupe_par_state_et_compte(self, mock_query):
        odoo = MagicMock()
        raw = pl.DataFrame({"x_invoicing_state": ["a_facturer", "a_facturer", "facture", None]})
        mock_query.return_value = _chain_mock(raw)
        result = _invoicing_state_counts(odoo)
        assert set(result.columns) == {"state", "n"}
        totals = dict(result.iter_rows())
        assert totals["a_facturer"] == 2
        assert totals["facture"] == 1
        assert totals["(non défini)"] == 1

    @patch(f"{MODULE}.query")
    def test_retourne_dataframe_trie(self, mock_query):
        odoo = MagicMock()
        raw = pl.DataFrame({"x_invoicing_state": ["z_state", "a_state"]})
        mock_query.return_value = _chain_mock(raw)
        result = _invoicing_state_counts(odoo)
        assert result["state"].to_list() == ["a_state", "z_state"]


class TestFacturesDraft:
    @patch(f"{MODULE}.query")
    def test_retourne_dataframe_avec_account_move_id(self, mock_query):
        odoo = MagicMock()
        raw = pl.DataFrame(
            {
                "sale_order_id": [1],
                "name": ["SO1"],
                "invoice_ids": [10],
                "name_account_move": ["INV/001"],
            }
        )
        mock_query.return_value = _chain_mock(raw)
        result = _factures_draft(odoo)
        assert "account_move_id" in result.columns
        assert result["account_move_id"][0] == 10

    @patch(f"{MODULE}.query")
    def test_df_vide_retourne_colonnes_attendues(self, mock_query):
        odoo = MagicMock()
        raw = pl.DataFrame(
            {
                "sale_order_id": pl.Series([], dtype=pl.Int64),
                "name": pl.Series([], dtype=pl.Utf8),
                "invoice_ids": pl.Series([], dtype=pl.Int64),
                "name_account_move": pl.Series([], dtype=pl.Utf8),
            }
        )
        mock_query.return_value = _chain_mock(raw)
        result = _factures_draft(odoo)
        assert result.is_empty()
        assert "account_move_id" in result.columns


class TestLissesQuantite1:
    @patch(f"{MODULE}.query")
    def test_groupe_par_commande_et_agregee_categ_names(self, mock_query):
        odoo = MagicMock()
        raw = pl.DataFrame(
            {
                "sale_order_id": [1, 1, 2],
                "name": ["SO1", "SO1", "SO2"],
                "name_product_category": ["Base", "HP", "HC"],
            }
        )
        mock_query.return_value = _chain_mock(raw)
        result = _lisses_quantite_1(odoo)
        assert "categ_names" in result.columns
        so1 = result.filter(pl.col("sale_order_id") == 1)["categ_names"][0]
        assert set(so1) == {"Base", "HP"}

    @patch(f"{MODULE}.query")
    def test_df_vide_retourne_schema_correct(self, mock_query):
        odoo = MagicMock()
        raw = pl.DataFrame(
            {
                "sale_order_id": pl.Series([], dtype=pl.Int64),
                "name": pl.Series([], dtype=pl.Utf8),
                "name_product_category": pl.Series([], dtype=pl.Utf8),
            }
        )
        mock_query.return_value = _chain_mock(raw)
        result = _lisses_quantite_1(odoo)
        assert result.is_empty()
        assert "categ_names" in result.columns


class TestVerifier:
    @patch(f"{MODULE}._lisses_quantite_1")
    @patch(f"{MODULE}._factures_draft")
    @patch(f"{MODULE}._invoicing_state_counts")
    @patch(f"{MODULE}._cfne_manquante")
    @patch(f"{MODULE}._rsc_manquante")
    def test_compose_les_5_checks(self, mock_rsc, mock_cfne, mock_counts, mock_draft, mock_lisses):
        odoo = MagicMock()
        mock_rsc.return_value = pl.DataFrame({"sale_order_id": [1]})
        mock_cfne.return_value = pl.DataFrame({"sale_order_id": [2]})
        mock_counts.return_value = pl.DataFrame({"state": ["a"], "n": [1]})
        mock_draft.return_value = pl.DataFrame({"account_move_id": [10]})
        mock_lisses.return_value = pl.DataFrame({"sale_order_id": [3]})

        result = verifier(odoo)

        assert isinstance(result, ResultatVerification)
        assert result.rsc_manquante["sale_order_id"][0] == 1
        assert result.cfne_manquante["sale_order_id"][0] == 2
        assert result.factures_draft["account_move_id"][0] == 10
        mock_rsc.assert_called_once_with(odoo)
        mock_cfne.assert_called_once_with(odoo)
