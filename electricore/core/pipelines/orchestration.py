"""
Module d'orchestration des pipelines Polars de facturation.

Fournit des fonctions d'orchestration qui composent les pipelines purs Polars
et retournent des ResultatFacturationPolars immutables.

Ce module centralise l'orchestration de tous les pipelines Polars, garantissant
que pipeline_perimetre n'est appelé qu'une seule fois et que les résultats
intermédiaires sont accessibles via le container ResultatFacturationPolars.
"""

from typing import NamedTuple, Optional
import polars as pl

from electricore.core.pipelines.perimetre import pipeline_perimetre
from electricore.core.pipelines.abonnements import pipeline_abonnements
from electricore.core.pipelines.energie import pipeline_energie
from electricore.core.pipelines.facturation import pipeline_facturation


class ResultatFacturationPolars(NamedTuple):
    """
    Container immutable pour tous les résultats du pipeline de facturation Polars.

    Ce NamedTuple permet d'accéder facilement aux résultats intermédiaires
    et finaux du pipeline de facturation, tout en maintenant l'immutabilité
    et la possibilité d'unpacking. Utilise des LazyFrames pour l'évaluation paresseuse.

    Attributes:
        historique_enrichi: LazyFrame avec détection ruptures + événements facturation
        abonnements: LazyFrame périodes d'abonnement avec TURPE fixe (optionnel)
        energie: LazyFrame périodes d'énergie avec TURPE variable (optionnel)
        facturation: DataFrame méta-périodes mensuelles agrégées (optionnel, collecté)

    Examples:
        # Accès par attributs
        result = facturation(historique_lf, releves_lf)
        abonnements_lf = result.abonnements

        # Unpacking complet
        hist, abo, ener, fact = result

        # Unpacking partiel avec collecte paresseuse
        hist, abo, *_ = result
        abonnements_df = abo.collect()
    """
    historique_enrichi: pl.LazyFrame
    abonnements: Optional[pl.LazyFrame] = None
    energie: Optional[pl.LazyFrame] = None
    facturation: Optional[pl.DataFrame] = None  # Collecté pour l'agrégation finale


def calculer_historique_enrichi(
    historique: pl.LazyFrame,
    date_limite: pl.Expr | None = None
) -> ResultatFacturationPolars:
    """
    Calcule uniquement l'historique enrichi (pipeline périmètre) - Version Polars.

    Utile quand on veut juste préparer l'historique avec la détection
    des ruptures et l'insertion des événements de facturation.

    Args:
        historique: LazyFrame contenant l'historique des événements contractuels
        date_limite: Expression Polars pour filtrer les événements après cette date
                    (défaut: 1er du mois courant)

    Returns:
        ResultatFacturationPolars avec historique_enrichi seulement
    """
    historique_enrichi = pipeline_perimetre(historique, date_limite=date_limite)
    return ResultatFacturationPolars(historique_enrichi=historique_enrichi)


def calculer_abonnements(
    historique: pl.LazyFrame,
    date_limite: pl.Expr | None = None
) -> ResultatFacturationPolars:
    """
    Calcule les abonnements avec leur contexte (historique enrichi) - Version Polars.

    Orchestre pipeline_perimetre + pipeline_abonnements pour obtenir
    les périodes d'abonnement avec TURPE fixe.

    Args:
        historique: LazyFrame contenant l'historique des événements contractuels
        date_limite: Expression Polars pour filtrer les événements après cette date
                    (défaut: 1er du mois courant)

    Returns:
        ResultatFacturationPolars avec historique_enrichi + abonnements
    """
    historique_enrichi = pipeline_perimetre(historique, date_limite=date_limite)
    abonnements = pipeline_abonnements(historique_enrichi)

    return ResultatFacturationPolars(
        historique_enrichi=historique_enrichi,
        abonnements=abonnements
    )


def calculer_energie(
    historique: pl.LazyFrame,
    releves: pl.LazyFrame,
    date_limite: pl.Expr | None = None
) -> ResultatFacturationPolars:
    """
    Calcule l'énergie avec son contexte (historique enrichi) - Version Polars.

    Orchestre pipeline_perimetre + pipeline_energie pour obtenir
    les périodes d'énergie avec TURPE variable.

    Args:
        historique: LazyFrame contenant l'historique des événements contractuels
        releves: LazyFrame contenant les relevés d'index R151
        date_limite: Expression Polars pour filtrer les événements après cette date
                    (défaut: 1er du mois courant)

    Returns:
        ResultatFacturationPolars avec historique_enrichi + energie
    """
    historique_enrichi = pipeline_perimetre(historique, date_limite=date_limite)
    energie = pipeline_energie(historique_enrichi, releves)

    return ResultatFacturationPolars(
        historique_enrichi=historique_enrichi,
        energie=energie
    )


def facturation(
    historique: pl.LazyFrame,
    releves: pl.LazyFrame,
    date_limite: pl.Expr | None = None
) -> ResultatFacturationPolars:
    """
    Pipeline complet de facturation avec méta-périodes mensuelles - Version Polars.

    Orchestre toute la chaîne de traitement en appelant pipeline_perimetre
    une seule fois puis en composant tous les autres pipelines :
    1. Détection des points de rupture et événements de facturation
    2. Génération des périodes d'abonnement avec TURPE fixe
    3. Génération des périodes d'énergie avec TURPE variable
    4. Agrégation mensuelle en méta-périodes

    Args:
        historique: LazyFrame contenant l'historique des événements contractuels
        releves: LazyFrame contenant les relevés d'index R151
        date_limite: Expression Polars pour filtrer les événements après cette date
                    (défaut: 1er du mois courant)

    Returns:
        ResultatFacturationPolars avec tous les résultats (historique_enrichi,
        abonnements, energie, facturation)

    Examples:
        # Usage complet
        result = facturation(historique_lf, releves_lf)

        # Accès à la facturation mensuelle (déjà collectée)
        factures_mensuelles = result.facturation

        # Accès aux résultats intermédiaires (LazyFrames)
        abonnements_lf = result.abonnements
        periodes_energie_lf = result.energie

        # Collecte paresseuse
        abonnements_df = result.abonnements.collect()

        # Unpacking
        hist, abo, ener, fact = result
    """
    # Une seule fois pipeline_perimetre - évite la duplication
    historique_enrichi = pipeline_perimetre(historique, date_limite=date_limite)

    # Calculs en parallèle possibles (même historique enrichi)
    abonnements = pipeline_abonnements(historique_enrichi)
    energie = pipeline_energie(historique_enrichi, releves)

    # Agrégation finale - nécessite la collecte pour l'agrégation
    facturation_mensuelle = pipeline_facturation(abonnements, energie)

    return ResultatFacturationPolars(
        historique_enrichi=historique_enrichi,
        abonnements=abonnements,
        energie=energie,
        facturation=facturation_mensuelle
    )


# Export des fonctions principales
__all__ = [
    'ResultatFacturationPolars',
    'calculer_historique_enrichi',
    'calculer_abonnements',
    'calculer_energie',
    'facturation'
]