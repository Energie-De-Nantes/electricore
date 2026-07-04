"""Sources Odoo normalisées consommées par les builds core (ADR-0019, issues #108, #144).

Expose les fonctions d'extraction Odoo qui fournissent les frames attendus par
les builds `core/builds/` (taxes : `rapport_taxe.py` ; facturation :
`rapport_facturation.py`, `contexte_mensuel.py`). Aucune logique métier —
lecture et normalisation uniquement.
"""

from datetime import date

import polars as pl

from .helpers import commandes_lignes, query
from .reader import OdooReader


def lignes_factures_taxe(odoo: OdooReader) -> pl.LazyFrame:
    """Lignes de commandes Odoo normalisées pour le calcul d'accise.

    Equivalent de l'extraction dans l'ancien `accise_par_contrat` :
    retourne le `LazyFrame` brut des lignes de commandes (shape compatible
    avec `pipeline_accise`).

    Args:
        odoo: `OdooReader` déjà ouvert.

    Returns:
        `LazyFrame` des lignes de commandes (colonnes Odoo brutes).
    """
    return commandes_lignes(odoo).lazy()


def mapping_pdl_order(odoo: OdooReader) -> pl.DataFrame:
    """Mapping PDL → nom de commande Odoo (pour enrichissement CTA).

    Extrait les `sale.order` ayant un `x_pdl`, retourne un DataFrame
    `{pdl, order_name}` dédupliqué par PDL.

    Args:
        odoo: `OdooReader` déjà ouvert.

    Returns:
        `DataFrame` avec colonnes `pdl` (str) et `order_name` (str).
    """
    return (
        query(odoo, "sale.order", domain=[("x_pdl", "!=", False)], fields=["name", "x_pdl"])
        .filter(pl.col("x_pdl").is_not_null())
        .select(
            pl.col("x_pdl").str.strip_chars().alias("pdl"),
            pl.col("name").alias("order_name"),
        )
        .collect()
        .unique("pdl")
    )


def _expr_est_brouillon() -> pl.Expr:
    """Expression : `est_brouillon = x_invoicing_state == 'draft' ∧ account.move.state == 'draft'`.

    Surface le statut « brouillon facturable » côté Odoo vers la colonne agnostique
    consommée par `core.builds.contexte_mensuel.rapprocher` (cf. ADR-0014 +
    slice 2 de la refonte Contexte mensuel). Les flags dérivés (`a_facturer`,
    `a_supprimer`) sont calculés en core depuis cet `est_brouillon` et `quantite`.
    """
    return ((pl.col("x_invoicing_state") == "draft") & (pl.col("state_account_move") == "draft")).alias("est_brouillon")


def lignes_factures_du_mois(odoo: OdooReader, mois: str, domain: list | None = None) -> pl.LazyFrame:
    """Lignes de factures Odoo du mois cible : datées du mois **ou brouillon** (#561).

    Les brouillons de campagne naissent sans `invoice_date` (elle se pose à la
    validation) : une fenêtre par date seule ne peut jamais les voir. La sélection
    est donc « `invoice_date` ∈ [mois, mois+1) OU `state = draft` » — la campagne
    courante est toujours visible, l'historique validé reste fenêtré pour l'audit.

    Source du chemin facturation (déménagée de `helpers.py`, #144 — symétrie
    avec les sources taxes ci-dessus). Aucun filtre sur `x_invoicing_state` ni
    `account.move.state` côté Odoo : les distinctions métier sont matérialisées
    en core, à partir de `est_brouillon` et `quantite` (cf. ADR-0014). La sortie
    respecte le schéma agnostique `LignesFacture` : clés métier renommées
    (`ref_situation_contractuelle`, `categorie_produit`, `quantite`,
    `est_brouillon`) et identifiants ERP passe-plat conservés tels quels.

    Args:
        odoo: Instance OdooReader connectée.
        mois: Premier jour du mois cible au format "YYYY-MM-DD".
        domain: Filtres additionnels sur `sale.order`.

    Returns:
        LazyFrame Polars `LignesFacture`-compatible : une ligne par
        `account.move.line` du mois cible ou en brouillon.
    """
    d = date.fromisoformat(mois)
    mois_suivant = (date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)).isoformat()
    base_domain = [("state", "=", "sale")] + (domain or [])

    return (
        query(
            odoo,
            "sale.order",
            domain=base_domain,
            fields=[
                "name",
                "state",  # nécessaire pour forcer le rename de account.move.state → state_account_move
                "x_pdl",
                "x_ref_situation_contractuelle",
                "x_lisse",
                "x_invoicing_state",
                "invoice_ids",
            ],
        )
        .follow(
            "invoice_ids",
            # Notation préfixe Odoo : fenêtre du mois OU brouillon (#561).
            domain=[
                "|",
                "&",
                ("invoice_date", ">=", mois),
                ("invoice_date", "<", mois_suivant),
                ("state", "=", "draft"),
            ],
            fields=["name", "invoice_date", "state", "invoice_line_ids"],
        )
        .follow("invoice_line_ids", fields=["name", "product_id", "quantity"])
        .follow("product_id", fields=["name", "categ_id"])
        .enrich("categ_id", fields=["name"])
        .lazy()
        .with_columns(_expr_est_brouillon())
        .rename(
            {
                "x_ref_situation_contractuelle": "ref_situation_contractuelle",
                "name_product_category": "categorie_produit",
                "quantity": "quantite",
            }
        )
    )
