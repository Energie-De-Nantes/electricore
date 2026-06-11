"""
Vérifications pré-facturation : binding HTTP + forme du livrable XLSX.

La logique métier vit dans `integrations.odoo.verification`. La projection
`feuilles_check_odoo` vit ici — les données sont intrinsèquement Odoo, donc
ni core-able (ADR-0016) ni assemblables en `integrations/` (ADR-0019, règle 3 :
source-only). Voir l'entrée « Feuilles » de core/CONTEXT.md (issue #173).
"""

import polars as pl

from electricore.api.config import settings
from electricore.api.serializers import xlsx_multi_sheet
from electricore.integrations.odoo import OdooReader, ResultatVerification, enrichir_liens, verifier


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


def feuilles_check_odoo(r: ResultatVerification, base_url: str) -> dict[str, pl.DataFrame]:
    """Projette le résultat de vérification en feuilles XLSX — 5 onglets fixes.

    Un onglet vide dit explicitement « rien à signaler » : la forme du livrable
    est stable quelles que soient les anomalies présentes.
    """
    lisses = enrichir_liens(r.lisses_quantite_1, base_url, "sale.order")
    if "categ_names" in lisses.columns and lisses["categ_names"].dtype == pl.List(pl.Utf8):
        lisses = lisses.with_columns(pl.col("categ_names").list.join(", "))
    return {
        "RSC manquant": enrichir_liens(r.rsc_manquante, base_url, "sale.order"),
        "CFNE manquant": enrichir_liens(r.cfne_manquante, base_url, "sale.order"),
        "Factures draft": enrichir_liens(r.factures_draft, base_url, "account.move"),
        "Lissés qty=1": lisses,
        "Invoicing state": r.invoicing_state_counts.rename({"state": "x_invoicing_state"}),
    }


def check_odoo_xlsx() -> bytes:
    """XLSX multi-onglets du check : verifier → feuilles → xlsx_multi_sheet."""
    odoo_cfg = settings.get_odoo_config()
    with OdooReader(config=odoo_cfg) as odoo:
        r = verifier(odoo)
    return xlsx_multi_sheet(feuilles_check_odoo(r, odoo_cfg["url"]))
