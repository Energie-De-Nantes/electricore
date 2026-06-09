"""Tests pour integrations/odoo/liens.py — helpers URL Odoo."""

import polars as pl

from electricore.integrations.odoo.liens import enrichir_liens, url_pour_enregistrement


class TestUrlPourEnregistrement:
    def test_format_standard(self):
        url = url_pour_enregistrement("https://my.odoo.com", "sale.order", 42)
        assert url == "https://my.odoo.com/web#id=42&model=sale.order&view_type=form"

    def test_trailing_slash_stripped(self):
        url = url_pour_enregistrement("https://my.odoo.com/", "sale.order", 1)
        assert url == "https://my.odoo.com/web#id=1&model=sale.order&view_type=form"

    def test_account_move_model(self):
        url = url_pour_enregistrement("https://erp.example.com", "account.move", 99)
        assert url == "https://erp.example.com/web#id=99&model=account.move&view_type=form"


class TestEnrichirLiens:
    def test_ajoute_colonne_url(self):
        df = pl.DataFrame({"sale_order_id": [1, 2], "name": ["SO1", "SO2"]})
        result = enrichir_liens(df, "https://my.odoo.com", "sale.order")
        assert "url" in result.columns
        assert result["url"][0] == "https://my.odoo.com/web#id=1&model=sale.order&view_type=form"
        assert result["url"][1] == "https://my.odoo.com/web#id=2&model=sale.order&view_type=form"

    def test_colonnes_originales_preservees(self):
        df = pl.DataFrame({"sale_order_id": [1], "name": ["SO1"], "x_pdl": ["123"]})
        result = enrichir_liens(df, "https://my.odoo.com", "sale.order")
        assert set(result.columns) == {"sale_order_id", "name", "x_pdl", "url"}

    def test_df_vide_retourne_df_avec_colonne_url(self):
        df = pl.DataFrame({"sale_order_id": pl.Series([], dtype=pl.Int64)})
        result = enrichir_liens(df, "https://my.odoo.com", "sale.order")
        assert result.is_empty()
        assert "url" in result.columns

    def test_account_move_utilise_account_move_id(self):
        df = pl.DataFrame({"account_move_id": [10], "name": ["INV/001"]})
        result = enrichir_liens(df, "https://erp.example.com", "account.move")
        assert result["url"][0] == "https://erp.example.com/web#id=10&model=account.move&view_type=form"
