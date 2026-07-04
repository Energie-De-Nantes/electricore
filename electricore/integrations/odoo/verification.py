"""Vérifications pré-facturation : détecte les anomalies sur les données Odoo."""

from dataclasses import dataclass

import polars as pl

from .helpers import query
from .reader import OdooReader


@dataclass(frozen=True, slots=True)
class ResultatVerification:
    rsc_manquante: pl.DataFrame
    cfne_manquante: pl.DataFrame
    invoicing_state_counts: pl.DataFrame
    factures_draft: pl.DataFrame
    lisses_quantite_1: pl.DataFrame


# Discriminant commande énergie (#564) : solde les faux positifs hors énergie (ex. S00583).
_ENERGIE = ("x_pdl", "!=", False)


def verifier(odoo: OdooReader) -> ResultatVerification:
    return ResultatVerification(
        rsc_manquante=_rsc_manquante(odoo),
        cfne_manquante=_cfne_manquante(odoo),
        invoicing_state_counts=_invoicing_state_counts(odoo),
        factures_draft=_factures_draft(odoo),
        lisses_quantite_1=_lisses_quantite_1(odoo),
    )


def _rsc_manquante(odoo: OdooReader) -> pl.DataFrame:
    return query(
        odoo,
        "sale.order",
        domain=[("state", "=", "sale"), _ENERGIE, ("x_ref_situation_contractuelle", "=", False)],
        fields=["name", "x_pdl"],
    ).collect()


def _cfne_manquante(odoo: OdooReader) -> pl.DataFrame:
    return query(
        odoo,
        "sale.order",
        domain=[("state", "=", "sale"), _ENERGIE, ("x_date_cfne", "=", False)],
        fields=["name", "x_pdl"],
    ).collect()


def _invoicing_state_counts(odoo: OdooReader) -> pl.DataFrame:
    raw = query(
        odoo,
        "sale.order",
        domain=[("state", "=", "sale"), _ENERGIE],
        fields=["x_invoicing_state"],
    ).collect()
    state_col = pl.col("x_invoicing_state").fill_null("(non défini)")
    return raw.group_by(state_col.alias("state")).agg(pl.len().alias("n")).sort("state")


def _factures_draft(odoo: OdooReader) -> pl.DataFrame:
    raw = (
        query(
            odoo,
            "sale.order",
            domain=[("state", "=", "sale"), _ENERGIE],
            fields=["name", "invoice_ids"],
        )
        .follow("invoice_ids", domain=[("state", "=", "draft")], fields=["name"])
        .collect()
    )
    if raw.is_empty():
        return (
            raw.rename({"invoice_ids": "account_move_id"})
            if "invoice_ids" in raw.columns
            else raw.with_columns(pl.lit(None, dtype=pl.Int64).alias("account_move_id"))
        )
    return raw.rename({"invoice_ids": "account_move_id"})


def _lisses_quantite_1(odoo: OdooReader) -> pl.DataFrame:
    raw = (
        query(
            odoo,
            "sale.order",
            domain=[("state", "=", "sale"), _ENERGIE, ("x_lisse", "=", True)],
            fields=["name", "order_line"],
        )
        .follow("order_line", domain=[("product_uom_qty", "=", 1)], fields=["product_id", "product_uom_qty"])
        .follow("product_id", fields=["name", "categ_id"])
        .enrich("categ_id", fields=["name"])
        .filter(pl.col("name_product_category").is_in(["Base", "HP", "HC"]))
        .collect()
    )
    if raw.is_empty():
        return pl.DataFrame(
            {
                "sale_order_id": pl.Series([], dtype=pl.Int64),
                "name": pl.Series([], dtype=pl.Utf8),
                "categ_names": pl.Series([], dtype=pl.List(pl.Utf8)),
            }
        )
    return (
        raw.group_by(["sale_order_id", "name"])
        .agg(pl.col("name_product_category").unique().sort().alias("categ_names"))
        .sort("name")
    )
