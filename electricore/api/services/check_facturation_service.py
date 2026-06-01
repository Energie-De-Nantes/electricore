"""
Vérifications pré-facturation : détecte les anomalies sur les données Odoo
(et plus tard Enedis, croisé) avant de lancer la campagne mensuelle.
"""

import io

import polars as pl
import xlsxwriter

from electricore.api.config import settings
from electricore.core.loaders import OdooReader
from electricore.core.loaders.odoo import query


def _odoo_link(model: str, record_id: int) -> str:
    base = settings.get_odoo_config()["url"].rstrip("/")
    return f"{base}/web#id={record_id}&model={model}&view_type=form"


def _rows_with_links(df: pl.DataFrame, model: str) -> list[dict]:
    if df.is_empty():
        return []
    id_col = f"{model.replace('.', '_')}_id"
    return [{**row, "url": _odoo_link(model, row[id_col])} for row in df.to_dicts()]


def verifier_odoo() -> dict:
    """
    Vérifie l'état des données Odoo avant facturation.

    Périmètre : tous les sale.order avec state='sale'.

    Returns:
        dict avec 5 clés :
        - rsc_manquante : sale.order sans x_ref_situation_contractuelle
        - cfne_manquante : sale.order sans x_date_cfne
        - invoicing_state_counts : {state: count} pour x_invoicing_state
        - factures_draft : factures encore en draft (anomalie après campagne)
        - lisses_quantite_1 : sale.order lissés avec une ligne énergie (Base/HP/HC) à qty=1
    """
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        rsc_df = query(
            odoo,
            "sale.order",
            domain=[("state", "=", "sale"), ("x_ref_situation_contractuelle", "=", False)],
            fields=["name", "x_pdl"],
        ).collect()

        cfne_df = query(
            odoo,
            "sale.order",
            domain=[("state", "=", "sale"), ("x_date_cfne", "=", False)],
            fields=["name", "x_pdl"],
        ).collect()

        states_df = query(
            odoo,
            "sale.order",
            domain=[("state", "=", "sale")],
            fields=["x_invoicing_state"],
        ).collect()

        factures_df = (
            query(
                odoo,
                "sale.order",
                domain=[("state", "=", "sale")],
                fields=["name", "invoice_ids"],
            )
            .follow("invoice_ids", domain=[("state", "=", "draft")], fields=["name"])
            .collect()
        )

        lisses_qty1_df = (
            query(
                odoo,
                "sale.order",
                domain=[("state", "=", "sale"), ("x_lisse", "=", True)],
                fields=["name", "order_line"],
            )
            .follow("order_line", domain=[("product_uom_qty", "=", 1)], fields=["product_id", "product_uom_qty"])
            .follow("product_id", fields=["name", "categ_id"])
            .enrich("categ_id", fields=["name"])
            .filter(pl.col("name_product_category").is_in(["Base", "HP", "HC"]))
            .collect()
        )

    state_col = pl.col("x_invoicing_state").fill_null("(non défini)")
    counts = dict(states_df.group_by(state_col.alias("state")).agg(pl.len().alias("n")).sort("state").iter_rows())

    factures_records = []
    if not factures_df.is_empty():
        factures_records = [
            {
                "sale_order_id": row["sale_order_id"],
                "sale_order_name": row["name"],
                "invoice_id": row["invoice_ids"],
                "invoice_name": row["name_account_move"],
                "url": _odoo_link("account.move", row["invoice_ids"]),
            }
            for row in factures_df.to_dicts()
        ]

    lisses_qty1_records = []
    if not lisses_qty1_df.is_empty():
        grouped = (
            lisses_qty1_df.group_by(["sale_order_id", "name"])
            .agg(pl.col("name_product_category").unique().sort().alias("categ_names"))
            .sort("name")
        )
        lisses_qty1_records = [
            {
                "sale_order_id": row["sale_order_id"],
                "sale_order_name": row["name"],
                "categ_names": row["categ_names"],
                "url": _odoo_link("sale.order", row["sale_order_id"]),
            }
            for row in grouped.to_dicts()
        ]

    return {
        "rsc_manquante": _rows_with_links(rsc_df, "sale.order"),
        "cfne_manquante": _rows_with_links(cfne_df, "sale.order"),
        "invoicing_state_counts": counts,
        "factures_draft": factures_records,
        "lisses_quantite_1": lisses_qty1_records,
    }


def generer_check_odoo_xlsx(result: dict) -> bytes:
    """XLSX multi-onglets pour les checks dépassant la limite Telegram."""
    buf = io.BytesIO()
    wb = xlsxwriter.Workbook(buf, {"remove_timezone": True})

    if result["rsc_manquante"]:
        pl.DataFrame(result["rsc_manquante"]).write_excel(workbook=wb, worksheet="RSC manquant")
    if result["cfne_manquante"]:
        pl.DataFrame(result["cfne_manquante"]).write_excel(workbook=wb, worksheet="CFNE manquant")
    if result["factures_draft"]:
        pl.DataFrame(result["factures_draft"]).write_excel(workbook=wb, worksheet="Factures draft")
    if result.get("lisses_quantite_1"):
        df = pl.DataFrame(result["lisses_quantite_1"])
        if "categ_names" in df.columns and df["categ_names"].dtype == pl.List(pl.Utf8):
            df = df.with_columns(pl.col("categ_names").list.join(", "))
        df.write_excel(workbook=wb, worksheet="Lissés qty=1")

    counts_rows = [{"x_invoicing_state": k, "n": v} for k, v in result["invoicing_state_counts"].items()]
    if counts_rows:
        pl.DataFrame(counts_rows).write_excel(workbook=wb, worksheet="Invoicing state")

    wb.close()
    return buf.getvalue()
