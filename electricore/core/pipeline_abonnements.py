"""
Pipeline de calcul des périodes d'abonnement avec TURPE.

Ce module contient le pipeline complet pour générer les périodes d'abonnement
enrichies avec les tarifs TURPE fixes, partant de l'historique contractuel.

Pipeline principal:
- pipeline_abonnement() - Pipeline complet de calcul des abonnements
  1. Préparation commune (pipeline_commun)
  2. Génération des périodes d'abonnement  
  3. Ajout du TURPE fixe
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame

from electricore.core.périmètre import HistoriquePérimètre
from electricore.core.pipeline_commun import pipeline_commun
from electricore.core.abonnements.fonctions import generer_periodes_abonnement
from electricore.core.taxes.turpe import ajouter_turpe_fixe, load_turpe_rules


@pa.check_types
def pipeline_abonnement(historique: DataFrame[HistoriquePérimètre]) -> pd.DataFrame:
    """
    Pipeline complète pour générer les périodes d'abonnement avec TURPE.
    
    Orchestre toute la chaîne de traitement :
    1. Détection des points de rupture
    2. Insertion des événements de facturation
    3. Génération des périodes d'abonnement
    4. Ajout du TURPE fixe
    
    Args:
        historique: DataFrame contenant l'historique des événements contractuels
        
    Returns:
        DataFrame avec les périodes d'abonnement enrichies du TURPE fixe
    """
    # Pipeline avec pandas pipe utilisant pipeline_commun
    return (
        historique
        .pipe(pipeline_commun)
        .pipe(generer_periodes_abonnement)
        .pipe(ajouter_turpe_fixe(load_turpe_rules()))
    )