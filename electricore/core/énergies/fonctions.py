import pandera as pa
import pandas as pd

from pandera.typing import DataFrame
from electricore.core.périmètre import (
    HistoriquePérimètre, SituationPérimètre, ModificationContractuelleImpactante,
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

# @pa.check_types
def découper_périodes(
    base_énergies: DataFrame[BaseCalculEnergies],
    modifications: DataFrame[ModificationContractuelleImpactante]
) -> DataFrame[BaseCalculEnergies]:
    """
    📌 Découpe la base de calcul d'énergies en sous-périodes calculables.

    Cette fonction segmente les périodes impactées par des modifications contractuelles (MCT)
    en sous-périodes homogènes, prêtes pour les calculs d’énergies.

    - Ajoute des points de découpage à chaque MCT.
    - Génère des périodes couvrantes et calculables avec les valeurs mises à jour.
    
    🚀 Résultat : Des périodes propres et exploitables pour le calcul des énergies.
    """

    # 1️⃣ **Séparer les périodes impactées et non impactées**
    impactées = base_énergies[
        base_énergies["Ref_Situation_Contractuelle"].isin(modifications["Ref_Situation_Contractuelle"])
    ]
    non_impactées = base_énergies[
        ~base_énergies["Ref_Situation_Contractuelle"].isin(modifications["Ref_Situation_Contractuelle"])
    ]

    # 2️⃣ **Générer les sous-périodes pour les lignes impactées**
    all_periods = []

    for ref_situation, modifs in modifications.groupby("Ref_Situation_Contractuelle"):
        # Trier les modifications chronologiquement
        modifs = modifs.sort_values(by="Date_Evenement")

        # Récupérer la ligne initiale
        base_ligne = impactées[impactées["Ref_Situation_Contractuelle"] == ref_situation].iloc[0].copy()

        # Initialisation des dates de découpage
        dates_coupure = [base_ligne["Date_Releve_deb"]] + \
                        modifs["Date_Evenement"].tolist() + \
                        [base_ligne["Date_Releve_fin"]]
        dates_coupure = sorted(set(dates_coupure))

        # 3️⃣ **Créer une ligne par sous-période**
        for i in range(len(dates_coupure) - 1):
            periode = base_ligne.copy()
            periode["Date_Début"] = dates_coupure[i]
            periode["Date_Fin"] = dates_coupure[i + 1]

            # Appliquer la modification contractuelle si elle intervient à cette date
            modif_courante = modifs[modifs["Date_Evenement"] == dates_coupure[i]]
            if not modif_courante.empty:
                modif_courante = modif_courante.iloc[0]
                periode["Puissance_Souscrite"] = modif_courante["Avant_Puissance_Souscrite"]
                periode["Formule_Tarifaire_Acheminement"] = modif_courante["Avant_Formule_Tarifaire_Acheminement"]

            all_periods.append(periode)
    return all_periods

    # 4️⃣ **Concaténer les périodes impactées + les non impactées**
    base_decoupée = pd.concat([non_impactées] + all_periods, ignore_index=True)

    return base_decoupée