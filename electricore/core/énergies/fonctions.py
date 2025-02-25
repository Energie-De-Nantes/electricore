import pandera as pa
import pandas as pd

from pandera.typing import DataFrame
from electricore.core.périmètre import HistoriquePérimètre, SituationPérimètre, extraire_situation, variations_mct_dans_periode


from icecream import ic

def préparer_base_énergies(
    historique: DataFrame[HistoriquePérimètre], deb: pd.Timestamp, fin: pd.Timestamp
) -> DataFrame[SituationPérimètre]:
    """
    🏗️ Prépare la base des énergies en identifiant les entrées, sorties et MCT dans la période.

    Args:
        historique (DataFrame[HistoriquePérimètre]): Historique des situations contractuelles.
        deb (pd.Timestamp): Début de la période de calcul des énergies.
        fin (pd.Timestamp): Fin de la période de calcul des énergies.

    Returns:
        DataFrame[SituationPérimètre]: Situation contractuelle enrichie pour le calcul des énergies.
    """

    
    situation = extraire_situation(fin, historique)
    _masque = (situation['Etat_Contractuel'] == 'EN SERVICE') | (
        (situation['Etat_Contractuel'] == 'RESILIE') & (situation['Date_Evenement'] >= deb)
    )
    colonnes_releve = ['Unité', 'Précision', 'Source', 'Date_Releve', 'Nature_Index', 'HP', 'HC', 'HCH', 'HPH', 'HPB', 'HCB', 'BASE']
    colonnes_evenement = ['Date_Derniere_Modification_FTA', 'Evenement_Declencheur', 'Type_Evenement', 'Date_Evenement', 'Ref_Demandeur', 'Id_Affaire']
    base = (
        situation[_masque]
        .assign(
            sortie_période=situation["Evenement_Declencheur"].isin(["RES", "CFNS"])
        )
        .drop(columns=[col for col in colonnes_releve + colonnes_evenement if col in situation])
        .copy()
    )
    # variations_mct = variations_mct_dans_periode(historique, deb, fin)

    # situation["entrée_période"] = situation["Date_Evenement"].between(deb, fin)
    # situation["sortie_période"] = (situation["Etat_Contractuel"] == "RESILIE") & situation["Date_Evenement"].between(deb, fin)
    # situation["MCT_période"] = situation["Ref_Situation_Contractuelle"].isin(variations_mct["Ref_Situation_Contractuelle"])

    return base