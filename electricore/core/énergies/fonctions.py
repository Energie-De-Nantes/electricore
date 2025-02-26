import pandera as pa
import pandas as pd

from pandera.typing import DataFrame
from electricore.core.périmètre import HistoriquePérimètre, SituationPérimètre, extraire_situation, variations_mct_dans_periode, extraire_colonnes_periode
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
    colonnes_releve = ['Unité', 'Précision', 'Source', 'Id_Calendrier_Distributeur', 'Date_Releve', 'Nature_Index', 'HP', 'HC', 'HCH', 'HPH', 'HPB', 'HCB', 'BASE']
    colonnes_evenement = ['Date_Derniere_Modification_FTA', 'Evenement_Declencheur', 'Type_Evenement', 'Date_Evenement', 'Ref_Demandeur', 'Id_Affaire']
    
    # 1) On récupére la situation du périmètre telle qu'elle était à la date de fin
    situation = extraire_situation(fin, historique)
    
    # 2) On filtre pour n'avoir que les PDLs en service, ou dont le service c'est terminé dans la période. 
    # (car pour les autres, aka terminés avant la période, il n'y a rien a calculer pour la période)
    _masque = (situation['Etat_Contractuel'] == 'EN SERVICE') | (
        (situation['Etat_Contractuel'] == 'RESILIE') & (situation['Date_Evenement'] >= deb)
    )

    base = (
        situation[_masque]
        .drop(columns=[col for col in colonnes_releve + colonnes_evenement if col in situation])
        .copy()
    )

    # 3) On va chercher les éventuelles entrées et sorties, avec les relevés d'index associés.
    entrees = RelevéIndex.validate(
        extraire_colonnes_periode(historique, deb, fin, ['MES', 'PMES', 'CFNE'], colonnes_releve)
        .set_index('Ref_Situation_Contractuelle')
    ).add_suffix('_deb')
    sorties = RelevéIndex.validate(
        extraire_colonnes_periode(historique, deb, fin, ['RES', 'CFNS'], colonnes_releve)
        .set_index('Ref_Situation_Contractuelle')
        
    ).add_suffix('_fin')

    # On les fusionne dans la base
    base = (
        base
        .merge(entrees, how='left', left_on='Ref_Situation_Contractuelle', right_index=True)
        .merge(sorties, how='left', left_on='Ref_Situation_Contractuelle', right_index=True)
    )

    return base


