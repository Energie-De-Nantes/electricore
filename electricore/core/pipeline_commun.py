"""
Pipeline commun pour la préparation des historiques contractuels.

Ce module contient les fonctions communes à tous les pipelines de calcul 
(énergies, abonnements, etc.) pour préparer et enrichir l'historique 
du périmètre contractuel.

Fonctions principales:
- pipeline_commun() - Pipeline de préparation commun à tous les calculs
"""

from typing import Optional
import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame

from electricore.core.models import HistoriquePérimètre
from electricore.core.pipeline_perimetre import enrichir_historique_périmètre


@pa.check_types
def pipeline_commun(
    historique: DataFrame[HistoriquePérimètre],
    date_limite: Optional[pd.Timestamp] = None
) -> DataFrame[HistoriquePérimètre]:
    """
    Pipeline commun qui prépare l'historique pour tous les calculs.
    
    Étapes communes à tous les pipelines :
    1. Filtrage des événements par date limite (optionnel)
    2. Détection des points de rupture
    3. Insertion des événements de facturation
    
    Args:
        historique: DataFrame contenant l'historique des événements contractuels
        date_limite: Si fourni, exclut tous les événements après cette date
                    (défaut: 1er du mois courant)
        
    Returns:
        DataFrame[HistoriquePérimètre] enrichi avec événements de facturation
    """
    if date_limite is None:
        date_limite = pd.Timestamp.now(tz="Europe/Paris").to_period("M").start_time.tz_localize("Europe/Paris")
    
    historique = historique[historique["Date_Evenement"] <= date_limite]
    
    return enrichir_historique_périmètre(historique)