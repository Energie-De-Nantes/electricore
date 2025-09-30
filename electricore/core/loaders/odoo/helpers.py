"""
Fonctions helpers pour Odoo - API fonctionnelle pure.

Ce module fournit des shortcuts pour créer des OdooQuery sur les modèles
Odoo les plus courants avec des champs prédéfinis.

Toutes les fonctions sont pures : elles prennent un OdooReader en paramètre
et retournent un OdooQuery chainable.
"""

import polars as pl
from typing import Optional, List

from .reader import OdooReader
from .query import OdooQuery


def query(odoo: OdooReader, model: str, domain: List = None,
          fields: Optional[List[str]] = None) -> OdooQuery:
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


def factures(odoo: OdooReader, domain: List = None) -> OdooQuery:
    """
    Query builder pour factures Odoo (account.move).

    Shortcut pour créer un query builder sur les factures avec champs standards.

    Args:
        odoo: Instance OdooReader connectée
        domain: Filtre Odoo initial (ex: [('state', '=', 'posted')])

    Returns:
        OdooQuery chainable

    Example:
        >>> with OdooReader(config) as odoo:
        ...     df = (factures(odoo, domain=[('state', '=', 'posted')])
        ...         .enrich('partner_id', fields=['name', 'email'])
        ...         .filter(pl.col('amount_total') > 1000)
        ...         .collect())
    """
    return query(odoo, 'account.move', domain=domain,
                fields=['name', 'invoice_date', 'amount_total', 'state', 'partner_id'])


def lignes_factures(odoo: OdooReader, domain: List = None) -> OdooQuery:
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
    return query(odoo, 'account.move.line', domain=domain,
                fields=['name', 'quantity', 'price_unit', 'product_id', 'move_id'])


def commandes(odoo: OdooReader, domain: List = None) -> OdooQuery:
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
    return query(odoo, 'sale.order', domain=domain,
                fields=['name', 'date_order', 'amount_total', 'state', 'partner_id'])


def partenaires(odoo: OdooReader, domain: List = None) -> OdooQuery:
    """
    Query builder pour partenaires Odoo (res.partner).

    Args:
        odoo: Instance OdooReader connectée
        domain: Filtre Odoo initial (ex: [('customer_rank', '>', 0)])

    Returns:
        OdooQuery chainable

    Example:
        >>> with OdooReader(config) as odoo:
        ...     df = (partenaires(odoo, domain=[('customer_rank', '>', 0)])
        ...         .filter(pl.col('active') == True)
        ...         .collect())
    """
    return query(odoo, 'res.partner', domain=domain,
                fields=['name', 'email', 'phone', 'customer_rank', 'active'])


# =============================================================================
# HELPERS AVEC NAVIGATION - Raccourcis pour relations multi-niveaux
# =============================================================================

def commandes_factures(odoo: OdooReader, domain: List = None) -> OdooQuery:
    """
    Query builder pour commandes avec factures.

    Navigation : sale.order → account.move

    Args:
        odoo: Instance OdooReader connectée
        domain: Filtre Odoo initial sur sale.order

    Returns:
        OdooQuery chainable avec navigation pré-configurée

    Example:
        >>> with OdooReader(config) as odoo:
        ...     # Utilisation simple
        ...     df = commandes_factures(odoo, domain=[('state', '=', 'sale')]).collect()
        ...
        ...     # Avec filtres et transformations supplémentaires
        ...     df = (commandes_factures(odoo)
        ...         .filter(pl.col('invoice_date') >= '2024-01-01')
        ...         .select(['name', 'date_order', 'name_account_move', 'invoice_date'])
        ...         .collect())
    """
    return (
        query(odoo, 'sale.order', domain=domain,
              fields=['name', 'date_order', 'amount_total', 'state', 'x_pdl', 'partner_id', 'invoice_ids'])
        .follow('invoice_ids', fields=['name', 'invoice_date', 'invoice_line_ids'])
    )


def commandes_lignes(odoo: OdooReader, domain: List = None) -> OdooQuery:
    """
    Query builder pour commandes avec lignes de factures détaillées.

    Navigation complète : sale.order → account.move → account.move.line → product.product → product.category

    Args:
        odoo: Instance OdooReader connectée
        domain: Filtre Odoo initial sur sale.order

    Returns:
        OdooQuery chainable avec navigation pré-configurée

    Example:
        >>> with OdooReader(config) as odoo:
        ...     # Utilisation simple
        ...     df = commandes_lignes(odoo, domain=[('state', '=', 'sale')]).collect()
        ...
        ...     # Avec filtres et transformations supplémentaires
        ...     df = (commandes_lignes(odoo)
        ...         .filter(pl.col('quantity') > 0)
        ...         .select([
        ...             pl.col('name').alias('order_name'),
        ...             pl.col('name_account_move').alias('invoice_name'),
        ...             pl.col('name_product_product').alias('product_name'),
        ...             pl.col('name_product_category').alias('categorie'),
        ...             pl.col('quantity'),
        ...             pl.col('price_total')
        ...         ])
        ...         .collect())
    """
    return (
        query(odoo, 'sale.order', domain=domain,
              fields=['name', 'date_order', 'amount_total', 'state', 'x_pdl', 'partner_id', 'invoice_ids'])
        .follow('invoice_ids', fields=['name', 'invoice_date', 'invoice_line_ids'])
        .follow('invoice_line_ids', fields=['name', 'product_id', 'quantity', 'price_unit', 'price_total'])
        .follow('product_id', fields=['name', 'categ_id'])
        .enrich('categ_id', fields=['name'])
    )