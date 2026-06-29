"""
Fonctions helpers pour Odoo - API fonctionnelle pure.

Ce module fournit des shortcuts pour créer des OdooQuery sur les modèles
Odoo les plus courants avec des champs prédéfinis.

Toutes les fonctions sont pures : elles prennent un OdooReader en paramètre
et retournent un OdooQuery chainable.
"""

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
