"""
Vérifications pré-facturation : binding HTTP + sérialisation XLSX.

La logique métier vit dans `integrations.odoo.verification`.
"""

import io

import polars as pl
import xlsxwriter

from electricore.api.config import settings
from electricore.integrations.odoo import OdooReader, enrichir_liens, verifier


def verifier_odoo() -> dict:
    """Vérifie l'état des données Odoo avant facturation et sérialise en dict JSON."""
    odoo_cfg = settings.get_odoo_config()
    base_url = odoo_cfg["url"]
    with OdooReader(config=odoo_cfg) as odoo:
        r = verifier(odoo)
    return {
        "rsc_manquante": enrichir_liens(r.rsc_manquante, base_url, "sale.order").to_dicts(),
        "cfne_manquante": enrichir_liens(r.cfne_manquante, base_url, "sale.order").to_dicts(),
        "invoicing_state_counts": dict(r.invoicing_state_counts.iter_rows()),
        "factures_draft": enrichir_liens(r.factures_draft, base_url, "account.move").to_dicts(),
        "lisses_quantite_1": _serialize_lisses(r.lisses_quantite_1, base_url),
    }


def _serialize_lisses(df: pl.DataFrame, base_url: str) -> list[dict]:
    if df.is_empty():
        return []
    enriched = enrichir_liens(df, base_url, "sale.order")
    return [{**row, "categ_names": list(row["categ_names"])} for row in enriched.to_dicts()]


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
