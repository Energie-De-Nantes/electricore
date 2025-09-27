"""
Expressions Polars pour le pipeline abonnements.

Ce module contient des expressions composables suivant la philosophie
fonctionnelle de Polars. Les expressions sont des transformations pures
qui peuvent être composées entre elles pour générer les périodes d'abonnement.
"""

import polars as pl
from typing import Optional

# =============================================================================
# EXPRESSIONS PURES ATOMIQUES
# =============================================================================

def expr_bornes_periode(over: str = "ref_situation_contractuelle") -> list[pl.Expr]:
    """
    Calcule les bornes de début et fin de période pour chaque contrat.

    Cette expression utilise shift(-1) pour déterminer la fin de chaque période
    en prenant la date d'événement suivante dans la partition.

    Args:
        over: Colonne(s) définissant les partitions pour la window function

    Returns:
        Liste d'expressions pour debut et fin

    Example:
        >>> df.with_columns(expr_bornes_periode())
    """
    return [
        pl.col("date_evenement").alias("debut"),
        pl.col("date_evenement").shift(-1).over(over).alias("fin")
    ]


def expr_nb_jours() -> pl.Expr:
    """
    Calcule le nombre de jours entre les bornes de période.

    Cette expression calcule la différence en jours entre fin et debut,
    en normalisant les timestamps pour éviter les problèmes d'heures.

    Returns:
        Expression Polars retournant le nombre de jours

    Example:
        >>> df.with_columns(expr_nb_jours().alias("nb_jours"))
    """
    return (
        pl.col("fin").dt.date() - pl.col("debut").dt.date()
    ).dt.total_days().cast(pl.Int32)


def expr_date_formatee_fr(col: str, format_type: str = "complet") -> pl.Expr:
    """
    Formate une colonne de date en français.

    Cette expression formate les dates selon différents formats français
    en utilisant les capacités de formatage de Polars avec des remplacements multiples.

    Args:
        col: Nom de la colonne à formater
        format_type: Type de format ("complet", "mois_annee")

    Returns:
        Expression Polars retournant la date formatée

    Example:
        >>> df.with_columns(expr_date_formatee_fr("debut", "complet").alias("debut_lisible"))
    """
    # Dictionnaire de correspondance anglais -> français
    mois_mapping = {
        "January": "janvier",
        "February": "février",
        "March": "mars",
        "April": "avril",
        "May": "mai",
        "June": "juin",
        "July": "juillet",
        "August": "août",
        "September": "septembre",
        "October": "octobre",
        "November": "novembre",
        "December": "décembre"
    }

    if format_type == "complet":
        # Format "1 mars 2025"
        expr = pl.col(col).dt.strftime("%d %B %Y")

        # Appliquer les remplacements séquentiellement
        for en_mois, fr_mois in mois_mapping.items():
            expr = expr.str.replace_all(en_mois, fr_mois)

        return expr

    elif format_type == "mois_annee":
        # Format "mars 2025"
        expr = pl.col(col).dt.strftime("%B %Y")

        # Appliquer les remplacements séquentiellement
        for en_mois, fr_mois in mois_mapping.items():
            expr = expr.str.replace_all(en_mois, fr_mois)

        return expr

    else:
        raise ValueError(f"Format non supporté : {format_type}")


def expr_fin_lisible() -> pl.Expr:
    """
    Formate la date de fin avec gestion du cas "en cours".

    Cette expression formate la fin en français ou retourne "en cours"
    si la date de fin est nulle.

    Returns:
        Expression Polars retournant la fin formatée

    Example:
        >>> df.with_columns(expr_fin_lisible().alias("fin_lisible"))
    """
    return (
        pl.when(pl.col("fin").is_null())
        .then(pl.lit("en cours"))
        .otherwise(expr_date_formatee_fr("fin", "complet"))
    )


def expr_periode_valide() -> pl.Expr:
    """
    Détermine si une période est valide (durée positive et fin définie).

    Une période est valide si :
    - Elle a une date de fin (pas null)
    - Sa durée est supérieure à 0 jour

    Returns:
        Expression Polars retournant True si la période est valide

    Example:
        >>> df.filter(expr_periode_valide())
    """
    return pl.col("fin").is_not_null() & (pl.col("nb_jours") > 0)


# =============================================================================
# FONCTIONS DE TRANSFORMATION LAZYFRAME
# =============================================================================

def calculer_periodes_abonnement(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Pipeline de calcul des périodes d'abonnement homogènes.

    Cette fonction applique l'ensemble des transformations pour générer
    des périodes d'abonnement à partir des événements impactant l'abonnement.

    Étapes du pipeline :
    1. Tri par contrat et date d'événement
    2. Calcul des bornes de période avec shift
    3. Calcul du nombre de jours
    4. Formatage des dates en français
    5. Filtrage des périodes valides
    6. Sélection des colonnes finales

    Args:
        lf: LazyFrame contenant les événements filtrés (impacte_abonnement=True)

    Returns:
        LazyFrame avec les périodes d'abonnement calculées

    Example:
        >>> periodes = (
        ...     historique
        ...     .filter(pl.col("impacte_abonnement"))
        ...     .pipe(calculer_periodes_abonnement)
        ... )
    """
    return (
        lf
        # 1. Tri pour assurer l'ordre chronologique par contrat
        .sort(["ref_situation_contractuelle", "date_evenement"])

        # 2. Calcul des bornes de période avec window functions
        .with_columns(expr_bornes_periode())

        # 3-4. Calcul des colonnes dérivées qui dépendent des bornes
        .with_columns([
            # Durée en jours (dépend de debut/fin)
            expr_nb_jours().alias("nb_jours"),
            # Formatage des dates en français
            expr_date_formatee_fr("debut", "complet").alias("debut_lisible"),
            expr_fin_lisible().alias("fin_lisible"),
            expr_date_formatee_fr("debut", "mois_annee").alias("mois_annee")
        ])

        # 5. Filtrage des périodes valides
        .filter(expr_periode_valide())

        # 6. Sélection des colonnes finales
        .select([
            "ref_situation_contractuelle",
            "pdl",
            "mois_annee",
            "debut_lisible",
            "fin_lisible",
            "formule_tarifaire_acheminement",
            "puissance_souscrite",
            "nb_jours",
            "debut",
            "fin"
        ])
    )


def generer_periodes_abonnement(historique: pl.LazyFrame) -> pl.LazyFrame:
    """
    Génère les périodes homogènes d'abonnement à partir de l'historique enrichi.

    Cette fonction filtre les événements pertinents puis applique le pipeline
    de calcul des périodes d'abonnement.

    Args:
        historique: LazyFrame contenant l'historique des événements contractuels enrichi

    Returns:
        LazyFrame avec les périodes d'abonnement homogènes

    Example:
        >>> periodes = generer_periodes_abonnement(historique_enrichi)
    """
    return (
        historique
        # Filtrer les événements qui impactent l'abonnement
        .filter(
            pl.col("impacte_abonnement") &
            pl.col("ref_situation_contractuelle").is_not_null()
        )
        # Appliquer le pipeline de calcul des périodes
        .pipe(calculer_periodes_abonnement)
    )


def pipeline_abonnements(historique: pl.LazyFrame) -> pl.LazyFrame:
    """
    Pipeline principal pour générer les périodes d'abonnement avec TURPE fixe.

    Ce pipeline orchestre :
    1. La génération des périodes d'abonnement
    2. L'ajout du TURPE fixe

    Args:
        historique: LazyFrame contenant l'historique des événements contractuels

    Returns:
        LazyFrame avec les périodes d'abonnement enrichies du TURPE fixe

    Example:
        >>> abonnements = pipeline_abonnements(historique_enrichi)
        >>> df = abonnements.collect()
    """
    from .turpe import ajouter_turpe_fixe

    return (
        historique
        .pipe(generer_periodes_abonnement)
        .pipe(ajouter_turpe_fixe)
    )


# =============================================================================
# FONCTIONS DE VALIDATION ET SÉLECTION
# =============================================================================

def selectionner_colonnes_abonnement(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Sélectionne et réordonne les colonnes finales pour les périodes d'abonnement.

    Cette fonction assure un ordre cohérent des colonnes dans la sortie finale.

    Args:
        lf: LazyFrame avec toutes les colonnes calculées

    Returns:
        LazyFrame avec les colonnes dans l'ordre final
    """
    colonnes_finales = [
        "ref_situation_contractuelle",
        "pdl",
        "mois_annee",
        "debut_lisible",
        "fin_lisible",
        "formule_tarifaire_acheminement",
        "puissance_souscrite",
        "nb_jours",
        "debut",
        "fin",
        # Colonnes TURPE (optionnelles)
        "turpe_fixe_journalier",
        "turpe_fixe"
    ]

    # Sélectionner uniquement les colonnes qui existent
    available_columns = lf.collect_schema().names()
    columns_to_select = [col for col in colonnes_finales if col in available_columns]

    return lf.select(columns_to_select)