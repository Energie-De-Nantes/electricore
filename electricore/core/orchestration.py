"""
Module d'orchestration des pipelines de facturation.

Fournit des fonctions d'orchestration qui composent les pipelines purs
et retournent des ResultatFacturation immutables.

Ce module centralise l'orchestration de tous les pipelines, garantissant
que pipeline_commun n'est appelé qu'une seule fois et que les résultats
intermédiaires sont accessibles via le container ResultatFacturation.
"""

from typing import NamedTuple, Optional
import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame

from electricore.core.périmètre import HistoriquePérimètre
from electricore.core.relevés import RelevéIndex
from electricore.core.pipeline_commun import pipeline_commun
from electricore.core.pipeline_abonnements import pipeline_abonnement
from electricore.core.pipeline_energie import pipeline_energie
from electricore.core.pipeline_facturation import pipeline_facturation


class ResultatFacturation(NamedTuple):
    """
    Container immutable pour tous les résultats du pipeline de facturation.
    
    Ce NamedTuple permet d'accéder facilement aux résultats intermédiaires
    et finaux du pipeline de facturation, tout en maintenant l'immutabilité
    et la possibilité d'unpacking.
    
    Attributes:
        historique_enrichi: Historique avec détection ruptures + événements facturation
        abonnements: Périodes d'abonnement avec TURPE fixe (optionnel)
        energie: Périodes d'énergie avec TURPE variable (optionnel)  
        facturation: Méta-périodes mensuelles agrégées (optionnel)
        
    Examples:
        # Accès par attributs
        result = facturation(historique, relevés)
        abonnements_df = result.abonnements
        
        # Unpacking complet
        hist, abo, ener, fact = result
        
        # Unpacking partiel
        hist, abo, *_ = result
    """
    historique_enrichi: DataFrame[HistoriquePérimètre]
    abonnements: Optional[DataFrame] = None  # [PeriodeAbonnement] 
    energie: Optional[DataFrame] = None      # [PeriodeEnergie]
    facturation: Optional[DataFrame] = None  # [PeriodeMeta]


def calculer_historique_enrichi(
    historique: DataFrame[HistoriquePérimètre],
    date_limite: Optional[pd.Timestamp] = None
) -> ResultatFacturation:
    """
    Calcule uniquement l'historique enrichi (pipeline commun).
    
    Utile quand on veut juste préparer l'historique avec la détection
    des ruptures et l'insertion des événements de facturation.
    
    Args:
        historique: DataFrame contenant l'historique des événements contractuels
        date_limite: Si fourni, exclut tous les événements après cette date
                    (défaut: 1er du mois courant)
        
    Returns:
        ResultatFacturation avec historique_enrichi seulement
    """
    historique_enrichi = pipeline_commun(historique, date_limite=date_limite)
    return ResultatFacturation(historique_enrichi=historique_enrichi)


@pa.check_types
def calculer_abonnements(
    historique: DataFrame[HistoriquePérimètre],
    date_limite: Optional[pd.Timestamp] = None
) -> ResultatFacturation:
    """
    Calcule les abonnements avec leur contexte (historique enrichi).
    
    Orchestre pipeline_commun + pipeline_abonnement pour obtenir
    les périodes d'abonnement avec TURPE fixe.
    
    Args:
        historique: DataFrame contenant l'historique des événements contractuels
        date_limite: Si fourni, exclut tous les événements après cette date
                    (défaut: 1er du mois courant)
        
    Returns:
        ResultatFacturation avec historique_enrichi + abonnements
    """
    historique_enrichi = pipeline_commun(historique, date_limite=date_limite)
    abonnements = pipeline_abonnement(historique_enrichi)
    
    return ResultatFacturation(
        historique_enrichi=historique_enrichi,
        abonnements=abonnements
    )


@pa.check_types
def calculer_energie(
    historique: DataFrame[HistoriquePérimètre], 
    relevés: DataFrame[RelevéIndex],
    date_limite: Optional[pd.Timestamp] = None
) -> ResultatFacturation:
    """
    Calcule l'énergie avec son contexte (historique enrichi).
    
    Orchestre pipeline_commun + pipeline_energie pour obtenir
    les périodes d'énergie avec TURPE variable.
    
    Args:
        historique: DataFrame contenant l'historique des événements contractuels
        relevés: DataFrame contenant les relevés d'index R151
        date_limite: Si fourni, exclut tous les événements après cette date
                    (défaut: 1er du mois courant)
        
    Returns:
        ResultatFacturation avec historique_enrichi + energie
    """
    historique_enrichi = pipeline_commun(historique, date_limite=date_limite)
    energie = pipeline_energie(historique_enrichi, relevés)
    
    return ResultatFacturation(
        historique_enrichi=historique_enrichi,
        energie=energie
    )


@pa.check_types
def facturation(
    historique: DataFrame[HistoriquePérimètre], 
    relevés: DataFrame[RelevéIndex],
    date_limite: Optional[pd.Timestamp] = None
) -> ResultatFacturation:
    """
    Pipeline complet de facturation avec méta-périodes mensuelles.
    
    Orchestre toute la chaîne de traitement en appelant pipeline_commun
    une seule fois puis en composant tous les autres pipelines :
    1. Détection des points de rupture et événements de facturation
    2. Génération des périodes d'abonnement avec TURPE fixe
    3. Génération des périodes d'énergie avec TURPE variable  
    4. Agrégation mensuelle en méta-périodes
    
    Args:
        historique: DataFrame contenant l'historique des événements contractuels
        relevés: DataFrame contenant les relevés d'index R151
        date_limite: Si fourni, exclut tous les événements après cette date
                    (défaut: 1er du mois courant)
        
    Returns:
        ResultatFacturation avec tous les résultats (historique_enrichi, 
        abonnements, energie, facturation)
        
    Examples:
        # Usage complet
        result = facturation(historique, relevés)
        
        # Accès à la facturation mensuelle
        factures_mensuelles = result.facturation
        
        # Accès aux résultats intermédiaires
        abonnements = result.abonnements
        periodes_energie = result.energie
        
        # Unpacking
        hist, abo, ener, fact = result
    """
    # Une seule fois pipeline_commun - évite la duplication
    historique_enrichi = pipeline_commun(historique, date_limite=date_limite)
    
    # Calculs en parallèle possibles (même historique enrichi)
    abonnements = pipeline_abonnement(historique_enrichi)
    energie = pipeline_energie(historique_enrichi, relevés)
    
    # Agrégation finale
    facturation_mensuelle = pipeline_facturation(abonnements, energie)
    
    return ResultatFacturation(
        historique_enrichi=historique_enrichi,
        abonnements=abonnements,
        energie=energie,
        facturation=facturation_mensuelle
    )


# Export des fonctions principales
__all__ = [
    'ResultatFacturation',
    'calculer_historique_enrichi',
    'calculer_abonnements',
    'calculer_energie', 
    'facturation'
]