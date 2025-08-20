"""
Pipeline commun pour la préparation des historiques contractuels.

Ce module contient les fonctions communes à tous les pipelines de calcul 
(énergies, abonnements, etc.) pour préparer et enrichir l'historique 
du périmètre contractuel.

Fonctions principales:
- pipeline_commun() - Pipeline de préparation commun à tous les calculs
"""

import pandera.pandas as pa
from pandera.typing import DataFrame

from electricore.core.périmètre import HistoriquePérimètre
from electricore.core.périmètre.fonctions import enrichir_historique_périmètre


@pa.check_types
def pipeline_commun(historique: DataFrame[HistoriquePérimètre]) -> DataFrame[HistoriquePérimètre]:
    """
    Pipeline commun qui prépare l'historique pour tous les calculs.
    
    Étapes communes à tous les pipelines :
    1. Détection des points de rupture
    2. Insertion des événements de facturation
    
    Args:
        historique: DataFrame contenant l'historique des événements contractuels
        
    Returns:
        DataFrame[HistoriquePérimètre] enrichi avec événements de facturation
    """
    return enrichir_historique_périmètre(historique)