"""
Expressions Polars pour le calcul de l'Accise sur l'électricité.

Ce module unifie toute la logique Accise en suivant l'architecture
fonctionnelle Polars avec des expressions composables et des pipelines optimisés.

L'Accise est calculée sur la base de la consommation mensuelle en appliquant
les taux réglementaires en vigueur selon la période.
"""

import polars as pl
from pathlib import Path
from typing import Optional


# =============================================================================
# CHARGEMENT DES RÈGLES ACCISE
# =============================================================================

def load_accise_rules() -> pl.LazyFrame:
    """
    Charge les règles tarifaires Accise depuis le fichier CSV.

    Returns:
        LazyFrame Polars contenant toutes les règles Accise avec types correctement définis

    Example:
        >>> regles = load_accise_rules()
        >>> regles.collect()
    """
    file_path = Path(__file__).parent.parent.parent / "config" / "accise_rules.csv"

    return (
        pl.scan_csv(file_path)
        # Conversion des colonnes de dates avec timezone Europe/Paris
        .with_columns([
            pl.col("start").str.to_datetime().dt.replace_time_zone("Europe/Paris"),
            pl.col("end").str.to_datetime().dt.replace_time_zone("Europe/Paris")
        ])
        # Conversion du taux en Float64
        .with_columns(
            pl.col("taux_accise_eur_mwh").cast(pl.Float64)
        )
    )


# =============================================================================
# EXPRESSIONS DE PRÉPARATION DES DONNÉES
# =============================================================================

def expr_calculer_mois_consommation() -> pl.Expr:
    """
    Expression pour calculer le mois de consommation à partir de invoice_date.

    Applique un décalage de -1 mois car la facture du mois M concerne
    les consommations du mois M-1.

    Returns:
        Expression Polars retournant le mois de consommation au format "YYYY-MM"

    Example:
        >>> df.with_columns(expr_calculer_mois_consommation().alias('mois_consommation'))
    """
    # Parser invoice_date, décaler -1 mois, formater en YYYY-MM
    date_conso = pl.col('invoice_date').str.to_date('%Y-%m-%d').dt.offset_by('-1mo')
    return date_conso.dt.strftime('%Y-%m')


def expr_calculer_trimestre_consommation() -> pl.Expr:
    """
    Expression pour calculer le trimestre de consommation à partir de invoice_date.

    Applique un décalage de -1 mois car la facture du mois M concerne
    les consommations du mois M-1.

    Returns:
        Expression Polars retournant le trimestre au format "YYYY-TX"

    Example:
        >>> df.with_columns(expr_calculer_trimestre_consommation().alias('trimestre'))
    """
    # Parser invoice_date et décaler -1 mois
    date_conso = pl.col('invoice_date').str.to_date('%Y-%m-%d').dt.offset_by('-1mo')

    # Extraire année et calculer trimestre
    annee = date_conso.dt.year().cast(pl.Utf8)
    quarter = ((date_conso.dt.month() - 1) // 3 + 1).cast(pl.Utf8)

    return annee + pl.lit('-T') + quarter


def agreger_consommations_mensuelles(
    lignes_factures: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Agrège les lignes de factures Odoo par PDL et mois de consommation.

    Filtre sur les catégories d'énergie (Base, HP, HC) et agrège la quantité
    totale par PDL et mois de consommation.

    Args:
        lignes_factures: LazyFrame issu de commandes_lignes() ou équivalent

    Returns:
        LazyFrame agrégé avec colonnes: pdl, order_name, mois_consommation,
        trimestre, energie_kwh

    Example:
        >>> from electricore.core.loaders import commandes_lignes
        >>> lignes = commandes_lignes(odoo).collect().lazy()
        >>> consos = agreger_consommations_mensuelles(lignes)
    """
    return (
        lignes_factures
        # Filtrer sur les catégories d'énergie
        .filter(pl.col('name_product_category').is_in(['Base', 'HP', 'HC']))

        # Calculer mois et trimestre de consommation
        .with_columns([
            expr_calculer_mois_consommation().alias('mois_consommation'),
            expr_calculer_trimestre_consommation().alias('trimestre')
        ])

        # Agréger par PDL et mois de consommation
        .group_by(['x_pdl', 'mois_consommation', 'trimestre'])
        .agg([
            pl.col('quantity').sum().alias('energie_kwh'),
            pl.col('name').first().alias('order_name')
        ])

        # Renommer x_pdl en pdl
        .rename({'x_pdl': 'pdl'})
    )


# =============================================================================
# EXPRESSIONS DE FILTRAGE TEMPOREL
# =============================================================================

def expr_filtrer_regles_temporelles() -> pl.Expr:
    """
    Expression pour filtrer les règles Accise applicables temporellement.

    Vérifie que le mois de consommation est compris dans la plage de validité
    de la règle (start <= mois_consommation < end).

    Returns:
        Expression booléenne pour filtrer les règles applicables

    Example:
        >>> df_joint.filter(expr_filtrer_regles_temporelles())
    """
    # Convertir mois_consommation string vers date (1er du mois)
    date_conso = pl.col('mois_consommation').str.to_date('%Y-%m')

    return (
        (date_conso >= pl.col("start")) &
        (
            date_conso < pl.col("end").fill_null(
                pl.datetime(2100, 1, 1, time_zone="Europe/Paris")
            )
        )
    )


# =============================================================================
# PIPELINE PRINCIPAL
# =============================================================================

def ajouter_accise(
    consommations: pl.LazyFrame,
    regles: Optional[pl.LazyFrame] = None
) -> pl.LazyFrame:
    """
    Ajoute le calcul de l'Accise aux consommations mensuelles.

    Cette fonction joint les consommations avec les règles Accise et calcule
    le montant de l'Accise pour chaque ligne selon le taux applicable.

    Args:
        consommations: LazyFrame des consommations mensuelles agrégées
        regles: LazyFrame des règles Accise (optionnel, sera chargé si None)

    Returns:
        LazyFrame avec les colonnes Accise ajoutées

    Example:
        >>> from electricore.core.loaders import commandes_lignes
        >>> lignes = commandes_lignes(odoo).collect().lazy()
        >>> consos = agreger_consommations_mensuelles(lignes)
        >>> consos_avec_accise = ajouter_accise(consos)
        >>> df = consos_avec_accise.collect()
    """
    if regles is None:
        regles = load_accise_rules()

    # Récupérer la liste des colonnes originales
    colonnes_originales = consommations.collect_schema().names()

    # Convertir mois_consommation en datetime pour la jointure (alignement avec start/end)
    consommations_avec_date = consommations.with_columns(
        pl.col('mois_consommation')
        .str.to_datetime('%Y-%m')
        .dt.replace_time_zone('Europe/Paris')
        .alias('date_conso_temp')
    )

    # Trier les consommations par date_conso_temp (requis pour join_asof)
    consommations_triees = consommations_avec_date.sort('date_conso_temp')

    # Trier les règles par start (requis pour join_asof)
    regles_triees = regles.collect().sort('start').lazy()

    return (
        consommations_triees
        # Jointure asof : prend la règle la plus récente avant date_conso
        .join_asof(
            regles_triees,
            left_on='date_conso_temp',
            right_on='start',
            strategy='backward'
        )

        # Validation des règles présentes
        .filter(pl.col("start").is_not_null())

        # Filtrage temporel des règles applicables
        .filter(expr_filtrer_regles_temporelles())

        # Supprimer les colonnes temporaires et de règles avant calculs
        .drop('date_conso_temp', 'start', 'end')

        # Calcul de l'Accise
        .with_columns([
            (pl.col('energie_kwh') / 1000).alias('energie_mwh'),
            ((pl.col('energie_kwh') / 1000) * pl.col('taux_accise_eur_mwh')).round(2).alias('accise_eur')
        ])

        # Sélection des colonnes finales
        .select([
            *colonnes_originales,
            'taux_accise_eur_mwh',
            'energie_mwh',
            'accise_eur'
        ])
    )


def pipeline_accise(
    lignes_factures: pl.LazyFrame,
    regles: Optional[pl.LazyFrame] = None
) -> pl.DataFrame:
    """
    Pipeline complet de calcul de l'Accise depuis les lignes de factures.

    Pipeline complet :
    1. Filtrage sur les catégories d'énergie (Base, HP, HC)
    2. Agrégation par PDL et mois de consommation
    3. Jointure avec les règles Accise
    4. Calcul de l'Accise selon le taux applicable

    Args:
        lignes_factures: LazyFrame issu de commandes_lignes() ou équivalent
        regles: LazyFrame des règles Accise (optionnel, sera chargé si None)

    Returns:
        DataFrame avec toutes les consommations et leur Accise calculée

    Example:
        >>> from electricore.core.loaders import OdooReader, commandes_lignes
        >>> from electricore.core.pipelines.accise import pipeline_accise
        >>>
        >>> with OdooReader(config) as odoo:
        ...     lignes = commandes_lignes(odoo).collect().lazy()
        ...     df_accise = pipeline_accise(lignes)
    """
    # Étape 1 : Agrégation mensuelle
    consos_mensuelles = agreger_consommations_mensuelles(lignes_factures)

    # Étape 2 : Calcul de l'Accise
    consos_avec_accise = ajouter_accise(consos_mensuelles, regles)

    # Étape 3 : Collecte et tri
    return (
        consos_avec_accise
        .sort(['pdl', 'mois_consommation'])
        .collect()
    )


# Export des fonctions principales
__all__ = [
    'load_accise_rules',
    'expr_calculer_mois_consommation',
    'expr_calculer_trimestre_consommation',
    'agreger_consommations_mensuelles',
    'ajouter_accise',
    'pipeline_accise',
]
