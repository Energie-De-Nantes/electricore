"""
Module d'orchestration des pipelines Polars de facturation.

Fournit des fonctions d'orchestration qui composent les pipelines purs Polars
et retournent des ResultatFacturationPolars immutables.

Ce module centralise l'orchestration de tous les pipelines Polars, garantissant
que pipeline_historique n'est appelé qu'une seule fois et que les résultats
intermédiaires sont accessibles via le container ResultatFacturationPolars.
"""

from typing import NamedTuple

import polars as pl

from electricore.core.pipelines.abonnements import pipeline_abonnements
from electricore.core.pipelines.energie import pipeline_energie
from electricore.core.pipelines.facturation import pipeline_facturation
from electricore.core.pipelines.historique import pipeline_historique


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
    abonnements: pl.LazyFrame
    energie: pl.LazyFrame
    facturation: pl.DataFrame  # Collecté pour l'agrégation finale


def facturation(
    historique: pl.LazyFrame, releves: pl.LazyFrame, date_limite: pl.Expr | None = None
) -> ResultatFacturationPolars:
    """
    Pipeline complet de facturation avec méta-périodes mensuelles - Version Polars.

    Orchestre toute la chaîne de traitement en appelant pipeline_historique
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
    # Une seule fois pipeline_historique - évite la duplication
    historique_enrichi = pipeline_historique(historique, date_limite=date_limite)

    # Calculs en parallèle possibles (même historique enrichi)
    abonnements = pipeline_abonnements(historique_enrichi)
    energie = pipeline_energie(historique_enrichi, releves)

    # Agrégation finale - nécessite la collecte pour l'agrégation
    # pipeline_facturation attend des LazyFrame typés Pandera, mais pipeline_abonnements/energie
    # retournent des pl.LazyFrame génériques (les schémas sont validés à l'exécution via @pa.check_types)
    facturation_mensuelle = pipeline_facturation(abonnements, energie)  # type: ignore[arg-type]

    return ResultatFacturationPolars(
        historique_enrichi=historique_enrichi,
        abonnements=abonnements,
        energie=energie,
        facturation=facturation_mensuelle,
    )


# Export des fonctions principales
__all__ = [
    "ResultatFacturationPolars",
    "facturation",
]
