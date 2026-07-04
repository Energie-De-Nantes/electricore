"""Vérifications pré-facturation : détecte les anomalies sur les données Odoo."""

from dataclasses import dataclass, field
from datetime import date

import polars as pl

from .helpers import query
from .reader import OdooReader
from .sources import date_ancre


@dataclass(frozen=True, slots=True)
class ResultatVerification:
    rsc_manquante: pl.DataFrame
    cfne_manquante: pl.DataFrame
    invoicing_state_counts: pl.DataFrame
    factures_draft: pl.DataFrame
    lisses_quantite_1: pl.DataFrame
    brouillons_hors_ancre: pl.DataFrame = field(
        default_factory=lambda: pl.DataFrame(
            schema={"account_move_id": pl.Int64, "name": pl.Utf8, "name_account_move": pl.Utf8, "invoice_date": pl.Utf8}
        )
    )


# Discriminant commande énergie (#564) : solde les faux positifs hors énergie (ex. S00583).
_ENERGIE = ("x_pdl", "!=", False)


def verifier(odoo: OdooReader) -> ResultatVerification:
    return ResultatVerification(
        rsc_manquante=_rsc_manquante(odoo),
        cfne_manquante=_cfne_manquante(odoo),
        invoicing_state_counts=_invoicing_state_counts(odoo),
        factures_draft=_factures_draft(odoo),
        lisses_quantite_1=_lisses_quantite_1(odoo),
        brouillons_hors_ancre=_brouillons_hors_ancre(odoo),
    )


def ancre_courante(aujourd_hui: date) -> str:
    """Ancre de la campagne en cours : le 05 du mois courant (#564, ADR-0054).

    = `date_ancre(dernier mois révolu)` — réutilise le calcul de la sélection
    (#561) pour ne jamais diverger. Relative à l'ancre plutôt qu'au mois
    facturé : elle attrape aussi un brouillon périmé d'une campagne antérieure
    qui traînerait encore en draft.
    """
    annee, mois = aujourd_hui.year, aujourd_hui.month - 1
    if mois == 0:
        annee, mois = annee - 1, 12
    dernier_mois_revolu = date(annee, mois, 1).isoformat()
    return date_ancre(dernier_mois_revolu)


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


def _brouillons_hors_ancre(odoo: OdooReader) -> pl.DataFrame:
    """Brouillons de facture d'une commande énergie hors date-ancre courante (#564).

    La convention date-ancre (ADR-0054) pose `invoice_date = 05/(M+1)` sur tous
    les brouillons de la campagne du mois M. Un brouillon énergie encore en
    draft, daté ailleurs qu'à l'ancre courante ou pas daté du tout, est une
    violation de la convention — détectée ici avant le lancement de la
    campagne suivante, plutôt qu'absorbée silencieusement par la sélection
    (`sources.lignes_factures_du_mois`).
    """
    ancre = ancre_courante(date.today())
    raw = (
        query(
            odoo,
            "sale.order",
            domain=[("state", "=", "sale"), _ENERGIE],
            fields=["name", "invoice_ids"],
        )
        .follow(
            "invoice_ids",
            domain=[("state", "=", "draft"), "|", ("invoice_date", "!=", ancre), ("invoice_date", "=", False)],
            fields=["name", "invoice_date"],
        )
        .collect()
    )
    if raw.is_empty():
        return (
            raw.rename({"invoice_ids": "account_move_id"})
            if "invoice_ids" in raw.columns
            else raw.with_columns(pl.lit(None, dtype=pl.Int64).alias("account_move_id"))
        )
    return raw.rename({"invoice_ids": "account_move_id"})
