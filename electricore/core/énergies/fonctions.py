import pandera as pa
import pandas as pd

from pandera.typing import DataFrame
from electricore.core.périmètre import (
    HistoriquePérimètre, SituationPérimètre, 
    extraire_situation, extraire_période,
    extraite_relevés_entrées, extraite_relevés_sorties
)
from electricore.core.relevés import RelevéIndex
from electricore.core.énergies.modèles import BaseCalculEnergies

from icecream import ic

def préparer_base_énergies(
    historique: DataFrame[HistoriquePérimètre], deb: pd.Timestamp, fin: pd.Timestamp
) -> DataFrame[BaseCalculEnergies]:
    """
    🏗️ Prépare la base des énergies en identifiant les entrées, sorties et MCT dans la période.

    Args:
        historique (DataFrame[HistoriquePérimètre]): Historique des situations contractuelles.
        deb (pd.Timestamp): Début de la période de calcul des énergies.
        fin (pd.Timestamp): Fin de la période de calcul des énergies.

    Returns:
        DataFrame[SituationPérimètre]: Situation contractuelle enrichie pour le calcul des énergies.
    """
    colonnes_meta_releve = ['Unité', 'Précision', 'Source']
    colonnes_releve = ['Id_Calendrier_Distributeur', 'Date_Releve', 'Nature_Index', 'HP', 'HC', 'HCH', 'HPH', 'HPB', 'HCB', 'BASE']
    
    
    
    # 1) On récupére la situation du périmètre telle qu'elle était à la date de fin
    situation = extraire_situation(fin, historique)
    
    # 2) On filtre pour n'avoir que les PDLs en service, ou dont le service c'est terminé dans la période. 
    # (car pour les autres, aka terminés avant la période, il n'y a rien a calculer pour la période)
    _masque = (situation['Etat_Contractuel'] == 'EN SERVICE') | (
        (situation['Etat_Contractuel'] == 'RESILIE') & (situation['Date_Evenement'] >= deb)
    )
    colonnes_évenement = ['Ref_Situation_Contractuelle', 'pdl']
    base = (
        situation[_masque]
        .drop(columns=[col for col in situation if col not in colonnes_évenement])
        .sort_values(by="Ref_Situation_Contractuelle")
        .copy()
    )

    # 3) On interroge le périmètre sur les éventuelles entrées et sorties, et on récupére les relevés d'index associés.
    période: DataFrame[HistoriquePérimètre] = extraire_période(deb, fin, historique)

    entrées: DataFrame[RelevéIndex] = (
        extraite_relevés_entrées(période)
        .set_index('Ref_Situation_Contractuelle')
        .drop(columns=['pdl'])
        .add_suffix('_deb')
        .assign(E=True)
    )
    sorties: DataFrame[RelevéIndex] = (
        extraite_relevés_sorties(période)
        .set_index('Ref_Situation_Contractuelle')
        .drop(columns=['pdl'])
        .add_suffix('_fin')
        .assign(S=True)
    )
    
    # On les fusionne dans la base
    base = (
        base
        .merge(entrées, how='left', left_on='Ref_Situation_Contractuelle', right_index=True)
        .merge(sorties, how='left', left_on='Ref_Situation_Contractuelle', right_index=True)
        .fillna({'E': False, 'S': False})
    )

    return base
