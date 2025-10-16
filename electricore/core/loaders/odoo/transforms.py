"""
Transformations Polars réutilisables pour données Odoo.

Ce module fournit des fonctions de transformation communes
pour normaliser et enrichir les données extraites depuis Odoo.
"""

import polars as pl
from typing import List, Optional


def normalize_many2one_fields(lf: pl.LazyFrame, fields: List[str]) -> pl.LazyFrame:
    """
    Normalise les champs many2one qui retournent [id, name] depuis Odoo.

    Extrait l'ID et le nom dans des colonnes séparées pour faciliter
    les jointures et analyses.

    Args:
        lf: LazyFrame contenant des champs many2one
        fields: Liste des noms de champs many2one à normaliser

    Returns:
        LazyFrame avec colonnes normalisées (field_id, field_name)

    Example:
        >>> lf = normalize_many2one_fields(lf, ['partner_id', 'user_id'])
        >>> # Crée: partner_id_id, partner_id_name, user_id_id, user_id_name
    """
    for field in fields:
        if field in lf.columns:
            lf = lf.with_columns([
                pl.col(field).list.get(0).cast(pl.Int64).alias(f"{field}_id"),
                pl.col(field).list.get(1).cast(pl.Utf8).alias(f"{field}_name")
            ])
    return lf


def convert_odoo_dates(lf: pl.LazyFrame, date_fields: List[str],
                       timezone: str = "Europe/Paris") -> pl.LazyFrame:
    """
    Convertit les champs date/datetime Odoo en DateTime Polars avec timezone.

    Args:
        lf: LazyFrame contenant des dates
        date_fields: Liste des noms de champs date à convertir
        timezone: Timezone cible (défaut: Europe/Paris)

    Returns:
        LazyFrame avec dates converties en DateTime avec timezone

    Example:
        >>> lf = convert_odoo_dates(lf, ['invoice_date', 'create_date'])
    """
    for field in date_fields:
        if field in lf.columns:
            lf = lf.with_columns([
                pl.col(field)
                .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                .dt.replace_time_zone(timezone)
                .alias(field)
            ])
    return lf


def add_computed_columns(lf: pl.LazyFrame,
                        computations: dict[str, pl.Expr]) -> pl.LazyFrame:
    """
    Ajoute des colonnes calculées au LazyFrame.

    Args:
        lf: LazyFrame source
        computations: Dict {nom_colonne: expression_polars}

    Returns:
        LazyFrame avec colonnes calculées ajoutées

    Example:
        >>> lf = add_computed_columns(lf, {
        ...     'amount_ttc': pl.col('amount_ht') * 1.2,
        ...     'is_draft': pl.col('state') == 'draft'
        ... })
    """
    exprs = [expr.alias(name) for name, expr in computations.items()]
    return lf.with_columns(exprs)


def filter_active_records(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filtre uniquement les enregistrements actifs (active = True).

    Args:
        lf: LazyFrame contenant une colonne 'active'

    Returns:
        LazyFrame filtré sur active = True

    Example:
        >>> lf = filter_active_records(lf)
    """
    if 'active' in lf.columns:
        return lf.filter(pl.col('active') == True)
    return lf


def explode_one2many_field(lf: pl.LazyFrame, field: str,
                          related_model: str, related_fields: List[str],
                          connector: 'OdooReader') -> pl.LazyFrame:
    """
    Explode un champ one2many et enrichit avec les données liées.

    Args:
        lf: LazyFrame source
        field: Nom du champ one2many à exploder
        related_model: Modèle Odoo lié (ex: 'account.move.line')
        related_fields: Champs à récupérer du modèle lié
        connector: Instance OdooReader pour récupération

    Returns:
        LazyFrame avec lignes explodées et enrichies

    Example:
        >>> lf = explode_one2many_field(
        ...     lf, 'invoice_line_ids', 'account.move.line',
        ...     ['product_id', 'quantity', 'price_unit'], odoo_reader
        ... )
    """
    # Collecter pour exploiter
    df = lf.collect()

    if field not in df.columns:
        return lf

    # Explode
    df = df.explode(field)

    # Récupérer IDs uniques
    unique_ids = [int(id) for id in df[field].unique().to_list() if id is not None]

    if not unique_ids:
        return df.lazy()

    # Fetch related data
    related_df = connector.search_read(
        related_model,
        [('id', 'in', unique_ids)],
        related_fields
    )

    # Join
    target_alias = related_model.replace('.', '_')
    id_column = f'{target_alias}_id'

    if 'id' in related_df.columns:
        related_df = related_df.rename({'id': id_column})

    result = df.join(related_df, left_on=field, right_on=id_column, how='left')

    return result.lazy()


def aggregate_by_period(lf: pl.LazyFrame, date_column: str,
                       period: str = 'month',
                       agg_exprs: Optional[List[pl.Expr]] = None) -> pl.LazyFrame:
    """
    Agrège les données par période temporelle.

    Args:
        lf: LazyFrame source
        date_column: Colonne contenant les dates
        period: Période d'agrégation ('day', 'week', 'month', 'year')
        agg_exprs: Expressions d'agrégation Polars (défaut: count)

    Returns:
        LazyFrame agrégé par période

    Example:
        >>> lf = aggregate_by_period(
        ...     lf, 'invoice_date', period='month',
        ...     agg_exprs=[pl.col('amount_total').sum().alias('total')]
        ... )
    """
    if agg_exprs is None:
        agg_exprs = [pl.count().alias('count')]

    return lf.group_by_dynamic(date_column, every=f'1{period[0]}').agg(agg_exprs)


# =============================================================================
# EXPRESSIONS POLARS UTILITAIRES POUR FACTURATION
# =============================================================================

def expr_calculer_trimestre_facturation() -> pl.Expr:
    """
    Expression pour calculer le trimestre de consommation à partir de invoice_date.

    Applique un décalage de -1 mois car la facture du mois M concerne
    les consommations du mois M-1.

    Gère automatiquement le parsing de invoice_date si c'est une string (format ISO).

    Logique :
    - invoice_date = "2025-10-09" → mois facturé = septembre 2025 → trimestre = 2025-Q3
    - invoice_date = "2025-01-15" → mois facturé = décembre 2024 → trimestre = 2024-Q4

    Returns:
        Expression Polars retournant le trimestre au format "YYYY-QX"

    Example:
        >>> # Dans un helper ou query builder
        >>> df = (commandes_lignes(odoo)
        ...     .collect()
        ...     .with_columns(expr_calculer_trimestre_facturation().alias('trimestre'))
        ...     .filter(pl.col('trimestre') == '2025-Q1'))

        >>> # Directement sur un DataFrame
        >>> df = df.with_columns(expr_calculer_trimestre_facturation().alias('trimestre'))
    """
    # Parser invoice_date si c'est une string, sinon utiliser directement
    date_parsed = pl.col('invoice_date').str.to_date('%Y-%m-%d')

    # Décaler de -1 mois pour obtenir le mois de consommation réel
    date_conso = date_parsed.dt.offset_by('-1mo')

    # Extraire année et calculer trimestre du mois de consommation
    annee = date_conso.dt.year().cast(pl.Utf8)
    quarter = ((date_conso.dt.month() - 1) // 3 + 1).cast(pl.Utf8)

    return annee + pl.lit('-T') + quarter


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    'normalize_many2one_fields',
    'convert_odoo_dates',
    'add_computed_columns',
    'filter_active_records',
    'explode_one2many_field',
    'aggregate_by_period',
    'expr_calculer_trimestre_facturation',
]