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

    Pour ajouter le trimestre de consommation (avec décalage -1 mois), utiliser
    expr_calculer_trimestre_facturation() après la collecte.

    Args:
        odoo: Instance OdooReader connectée
        domain: Filtre Odoo initial sur sale.order

    Returns:
        OdooQuery chainable avec navigation pré-configurée

    Example:
        >>> from electricore.core.loaders import commandes_lignes, expr_calculer_trimestre_facturation
        >>>
        >>> with OdooReader(config) as odoo:
        ...     # Utilisation simple
        ...     df = commandes_lignes(odoo, domain=[('state', '=', 'sale')]).collect()
        ...
        ...     # Ajouter le trimestre après collecte et filtrer
        ...     df = (commandes_lignes(odoo)
        ...         .collect()
        ...         .with_columns(expr_calculer_trimestre_facturation().alias('trimestre'))
        ...         .filter(pl.col('trimestre') == '2025-Q1'))
        ...
        ...     # Avec filtres et transformations supplémentaires
        ...     df = (commandes_lignes(odoo)
        ...         .collect()
        ...         .with_columns(expr_calculer_trimestre_facturation().alias('trimestre'))
        ...         .filter(pl.col('quantity') > 0)
        ...         .select([
        ...             pl.col('name').alias('order_name'),
        ...             pl.col('name_account_move').alias('invoice_name'),
        ...             pl.col('name_product_product').alias('product_name'),
        ...             pl.col('name_product_category').alias('categorie'),
        ...             pl.col('trimestre'),
        ...             pl.col('quantity'),
        ...             pl.col('price_total')
        ...         ]))
    """
    return (
        query(odoo, 'sale.order', domain=domain,
              fields=['name', 'date_order', 'amount_total', 'state', 'x_pdl', 'partner_id', 'invoice_ids'])
        .follow('invoice_ids', fields=['name', 'invoice_date', 'invoice_line_ids'])
        .follow('invoice_line_ids', fields=['name', 'product_id', 'quantity', 'price_unit', 'price_total'])
        .follow('product_id', fields=['name', 'categ_id'])
        .enrich('categ_id', fields=['name'])
    )


def consommations_mensuelles(odoo: OdooReader, domain: List = None) -> OdooQuery:
    """
    Query builder pour consommations mensuelles agrégées.

    Récupère les lignes de factures avec les informations nécessaires au calcul
    des consommations mensuelles. Les données doivent être agrégées post-collecte
    avec les pipelines appropriés (voir pipeline_accise).

    Navigation : sale.order → account.move → account.move.line → product.product → product.category

    Args:
        odoo: Instance OdooReader connectée
        domain: Filtre Odoo initial sur sale.order

    Returns:
        OdooQuery chainable retournant les lignes de factures

    Example:
        >>> from electricore.core.loaders import consommations_mensuelles
        >>> from electricore.core.pipelines.accise import pipeline_accise
        >>>
        >>> with OdooReader(config) as odoo:
        ...     # Récupérer les données et appliquer le pipeline
        ...     lignes = consommations_mensuelles(odoo).collect()
        ...     df_accise = pipeline_accise(lignes.lazy())
        ...
        ...     # Ou directement avec commandes_lignes()
        ...     lignes = commandes_lignes(odoo).collect()
        ...     df_accise = pipeline_accise(lignes.lazy())

    Note:
        Ce helper est identique à commandes_lignes() et existe pour cohérence
        sémantique. Pour le calcul d'Accise, utilisez pipeline_accise() qui gère
        l'agrégation et les calculs.
    """
    return commandes_lignes(odoo, domain=domain)


def lignes_a_facturer(odoo: OdooReader, domain: List = None) -> OdooQuery:
    """
    Query builder pour les lignes de factures brouillon à facturer.

    Navigation : sale.order (state=sale) → account.move (state=draft)
                 → account.move.line (qty > 0) → product.product → product.category

    Filtre côté Odoo (XML-RPC) :
    - sale.order : state = 'sale' ET au moins une facture draft
    - account.move : state = 'draft' uniquement
    - account.move.line : quantity > 0 (exclut lignes vides/notes)

    Les sale.order sans facture draft sont exclus dès la requête initiale.

    Args:
        odoo: Instance OdooReader connectée
        domain: Filtres additionnels sur sale.order (ex: [('x_pdl', '!=', False)])

    Returns:
        OdooQuery chainable retournant les lignes à facturer

    Example:
        >>> with OdooReader(config) as odoo:
        ...     df = (lignes_a_facturer(odoo)
        ...         .filter(pl.col('name_product_category').is_in(['Abonnements', 'HP', 'HC', 'Base']))
        ...         .select(['name', 'x_pdl', 'invoice_date', 'quantity', 'price_total',
        ...                  'name_product_product', 'name_product_category'])
        ...         .collect())
    """
    base_domain = [('state', '=', 'sale'), ('invoice_ids.state', '=', 'draft')] + (domain or [])
    return (
        query(odoo, 'sale.order', domain=base_domain,
              fields=['name', 'date_order', 'state', 'x_pdl', 'partner_id', 'invoice_ids'])
        .follow('invoice_ids',
                domain=[('state', '=', 'draft')],
                fields=['name', 'invoice_date', 'invoice_line_ids'])
        .filter(pl.col('name_account_move').is_not_null())
        .follow('invoice_line_ids',
                domain=[('quantity', '>', 0)],
                fields=['name', 'product_id', 'quantity', 'price_unit', 'price_total'])
        .filter(pl.col('quantity').is_not_null())  # exclut les lignes non-matchées du LEFT JOIN
        .follow('product_id', fields=['name', 'categ_id'])
        .enrich('categ_id', fields=['name'])
    )


def lignes_quantite_zero(odoo: OdooReader, domain: List = None) -> OdooQuery:
    """
    Query builder pour les lignes de factures brouillon avec quantité nulle.

    Identifie les account.move.line avec quantity = 0 dans les factures draft,
    candidates à la suppression avant validation (via OdooWriter.unlink).

    Navigation : sale.order (state=sale) → account.move (state=draft)
                 → account.move.line (quantity = 0) → product.product

    Filtre côté Odoo (XML-RPC) :
    - sale.order : state = 'sale' ET au moins une facture draft
    - account.move : state = 'draft' uniquement
    - account.move.line : quantity = 0

    Args:
        odoo: Instance OdooReader connectée
        domain: Filtres additionnels sur sale.order (ex: [('x_pdl', '!=', False)])

    Returns:
        OdooQuery chainable — colonne `invoice_line_ids` contient les IDs
        des account.move.line à supprimer

    Example:
        >>> with OdooReader(config) as odoo:
        ...     df = lignes_quantite_zero(odoo).collect()
        ...     ids_a_supprimer = df['invoice_line_ids'].drop_nulls().to_list()
    """
    base_domain = [('state', '=', 'sale'), ('invoice_ids.state', '=', 'draft')] + (domain or [])
    return (
        query(odoo, 'sale.order', domain=base_domain,
              fields=['name', 'state', 'x_pdl', 'partner_id', 'invoice_ids'])
        .follow('invoice_ids',
                domain=[('state', '=', 'draft')],
                fields=['name', 'invoice_date', 'invoice_line_ids'])
        .filter(pl.col('name_account_move').is_not_null())
        .follow('invoice_line_ids',
                domain=[('quantity', '=', 0)],
                fields=['name', 'product_id', 'quantity', 'price_unit'])
        .filter(pl.col('quantity').is_not_null())  # exclut les lignes non-matchées du LEFT JOIN
        .follow('product_id', fields=['name'])
    )