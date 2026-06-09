"""
Fonctions helpers pour Odoo - API fonctionnelle pure.

Ce module fournit des shortcuts pour créer des OdooQuery sur les modèles
Odoo les plus courants avec des champs prédéfinis.

Toutes les fonctions sont pures : elles prennent un OdooReader en paramètre
et retournent un OdooQuery chainable.
"""

from datetime import date

import polars as pl

from .query import OdooQuery
from .reader import OdooReader


def query(odoo: OdooReader, model: str, domain: list | None = None, fields: list[str] | None = None) -> OdooQuery:
    """
    Crée un OdooQuery depuis un OdooReader connecté.

    Fonction pure qui compose un query builder depuis une connexion active.

    Args:
        odoo: Instance OdooReader connectée (via context manager)
        model: Modèle Odoo à requêter (ex: 'account.move', 'sale.order')
        domain: Filtre Odoo (ex: [('state', '=', 'posted')])
        fields: Champs à récupérer initialement

    Returns:
        OdooQuery chainable pour compositions avancées

    Example:
        >>> with OdooReader(config) as odoo:
        ...     df = (query(odoo, 'account.move', domain=[('state', '=', 'posted')])
        ...         .enrich('partner_id', fields=['name', 'email'])
        ...         .filter(pl.col('amount_total') > 1000)
        ...         .collect())
    """
    df = odoo.search_read(model, domain, fields)
    return OdooQuery(connector=odoo, lazy_frame=df.lazy(), _current_model=model)


def lignes_factures(odoo: OdooReader, domain: list | None = None) -> OdooQuery:
    """
    Query builder pour lignes de factures Odoo (account.move.line).

    Args:
        odoo: Instance OdooReader connectée
        domain: Filtre Odoo initial

    Returns:
        OdooQuery chainable

    Example:
        >>> with OdooReader(config) as odoo:
        ...     df = (lignes_factures(odoo)
        ...         .filter(pl.col('quantity') > 0)
        ...         .enrich('product_id', fields=['name', 'default_code'])
        ...         .collect())
    """
    return query(
        odoo, "account.move.line", domain=domain, fields=["name", "quantity", "price_unit", "product_id", "move_id"]
    )


def commandes(odoo: OdooReader, domain: list | None = None) -> OdooQuery:
    """
    Query builder pour commandes de vente Odoo (sale.order).

    Args:
        odoo: Instance OdooReader connectée
        domain: Filtre Odoo initial (ex: [('state', '=', 'sale')])

    Returns:
        OdooQuery chainable

    Example:
        >>> with OdooReader(config) as odoo:
        ...     df = (commandes(odoo, domain=[('state', '=', 'sale')])
        ...         .enrich('partner_id', fields=['name', 'email'])
        ...         .filter(pl.col('amount_total') > 500)
        ...         .collect())
    """
    return query(
        odoo, "sale.order", domain=domain, fields=["name", "date_order", "amount_total", "state", "partner_id"]
    )


# =============================================================================
# HELPERS AVEC NAVIGATION - Raccourcis pour relations multi-niveaux
# =============================================================================


def commandes_lignes(odoo: OdooReader, domain: list | None = None) -> OdooQuery:
    """
    Query builder pour commandes avec lignes de factures détaillées.

    Navigation complète : sale.order → account.move → account.move.line → product.product → product.category

    Pour le trimestre de consommation, utiliser
    `electricore.core.pipelines.accise::expr_calculer_trimestre_consommation()`
    (calque accise avec décalage M-1).

    Args:
        odoo: Instance OdooReader connectée
        domain: Filtre Odoo initial sur sale.order

    Returns:
        OdooQuery chainable avec navigation pré-configurée

    Example:
        >>> with OdooReader(config) as odoo:
        ...     df = (commandes_lignes(odoo, domain=[('state', '=', 'sale')])
        ...         .collect()
        ...         .filter(pl.col('quantity') > 0)
        ...         .select([
        ...             pl.col('name').alias('order_name'),
        ...             pl.col('name_account_move').alias('invoice_name'),
        ...             pl.col('name_product_product').alias('product_name'),
        ...             pl.col('name_product_category').alias('categorie'),
        ...             pl.col('quantity'),
        ...             pl.col('price_total')
        ...         ]))
    """
    return (
        query(
            odoo,
            "sale.order",
            domain=domain,
            fields=["name", "date_order", "amount_total", "state", "x_pdl", "partner_id", "invoice_ids"],
        )
        .follow("invoice_ids", fields=["name", "invoice_date", "invoice_line_ids"])
        .follow("invoice_line_ids", fields=["name", "product_id", "quantity", "price_unit", "price_total"])
        .follow("product_id", fields=["name", "categ_id"])
        .enrich("categ_id", fields=["name"])
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
    """Toutes les lignes de factures Odoo dont `invoice_date` tombe dans le mois cible.

    Aucun filtre sur `x_invoicing_state` ni `account.move.state` côté Odoo : les
    distinctions métier sont matérialisées en core, à partir de `est_brouillon` et
    `quantite` (cf. ADR-0014). La sortie respecte le schéma agnostique `LignesFacture` :
    clés métier renommées (`ref_situation_contractuelle`, `categorie_produit`,
    `quantite`, `est_brouillon`) et identifiants ERP passe-plat conservés tels quels.

    Args:
        odoo: Instance OdooReader connectée.
        mois: Premier jour du mois cible au format "YYYY-MM-DD".
        domain: Filtres additionnels sur `sale.order`.

    Returns:
        LazyFrame Polars `LignesFacture`-compatible : une ligne par
        `account.move.line` du mois cible.
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
            domain=[("invoice_date", ">=", mois), ("invoice_date", "<", mois_suivant)],
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
