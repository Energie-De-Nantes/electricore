"""
Expressions Polars pour le pipeline abonnements.

Ce module contient des expressions composables suivant la philosophie
fonctionnelle de Polars. Les expressions sont des transformations pures
qui peuvent être composées entre elles pour générer les périodes d'abonnement.
"""

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import LazyFrame

from electricore.core.models.historique import Historique
from electricore.core.models.periode_abonnement import PeriodeAbonnement

# Formules partagées entre pipelines de périodes (issue #178, ADR-0023)
from electricore.core.pipelines.periodes import (
    expr_date_formatee_fr,
    expr_fin_lisible,
    expr_mois_annee,
    expr_nb_jours,
)

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
    return [pl.col("date_evenement").alias("debut"), pl.col("date_evenement").shift(-1).over(over).alias("fin")]


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
        .with_columns(
            [
                # Durée en jours (dépend de debut/fin)
                expr_nb_jours().alias("nb_jours"),
                # Formatage des dates en français (champs d'affichage)
                expr_date_formatee_fr("debut").alias("debut_lisible"),
                expr_fin_lisible().alias("fin_lisible"),
                # Clé calculable YYYY-MM, triable (issue #115)
                expr_mois_annee(),
            ]
        )
        # 5. Filtrage des périodes valides
        .filter(expr_periode_valide())
        # 6. Sélection des colonnes finales
        .select(
            [
                "ref_situation_contractuelle",
                "pdl",
                "mois_annee",
                "debut_lisible",
                "fin_lisible",
                "formule_tarifaire_acheminement",
                "puissance_souscrite_kva",
                "nb_jours",
                "debut",
                "fin",
            ]
        )
    )


@pa.check_types(lazy=True)
def generer_periodes_abonnement(historique: LazyFrame[Historique]) -> LazyFrame[PeriodeAbonnement]:
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
        .filter(pl.col("impacte_abonnement") & pl.col("ref_situation_contractuelle").is_not_null())
        # Appliquer le pipeline de calcul des périodes
        .pipe(calculer_periodes_abonnement)
    )


@pa.check_types(lazy=True)
def pipeline_abonnements(historique: LazyFrame[Historique]) -> LazyFrame[PeriodeAbonnement]:
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

    return historique.pipe(generer_periodes_abonnement).pipe(ajouter_turpe_fixe)


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
        "puissance_souscrite_kva",
        "nb_jours",
        "debut",
        "fin",
        # Colonnes TURPE (optionnelles)
        "turpe_fixe_journalier_eur",
        "turpe_fixe_eur",
    ]

    # Sélectionner uniquement les colonnes qui existent
    available_columns = lf.collect_schema().names()
    columns_to_select = [col for col in colonnes_finales if col in available_columns]

    return lf.select(columns_to_select)
