import pandas as pd

# def energies_et_taxes(deb: pd.Timestamp, fin: pd.Timestamp, base: pd.DataFrame) -> pd.DataFrame:
#     """
#     Calcule les énergies et les taxes pour une période donnée, sur l'ensemble du périmètre
#     """
    
#     complet = ajout_dates_par_defaut(deb, fin, base)
#     energies = calcul_energie(complet)
#     energies['Puissance_Souscrite'] = pd.to_numeric(energies['Puissance_Souscrite'])
#     rules = get_applicable_rules(deb, fin)
#     turpe = compute_turpe(entries=energies, rules=rules)
#     final = validation(
#         supprimer_colonnes(
#         fusion_des_sous_periode(turpe)))
#     return final.round(2)

import pandera as pa
from pandera.typing import DataFrame

from electricore.inputs.flux import (
    FluxC15, FluxR151, 
    lire_flux_c15, lire_flux_r151
)
from electricore.core.périmètre import (
    HistoriquePérimètre, 

)
from electricore.core.relevés import (
    RelevéIndex, 
)
from electricore.core.énergies.fonctions import préparer_base_énergies, ajouter_relevés, calculer_energies

# TODO rename facturation depuis flux ou un truc du genre. 
def facturation_flux(deb: pd.Timestamp, fin: pd.Timestamp, c15: pd.DataFrame, r151: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les énergies et les taxes pour une période donnée, sur l'ensemble du périmètre
    """
    historique = lire_flux_c15(c15)
    relevés = lire_flux_r151(r151)
    base = préparer_base_énergies(historique=historique, deb=deb, fin=fin)

    base["Date_Releve_deb"] = base["Date_Releve_deb"].fillna(deb)
    base["Date_Releve_fin"] = base["Date_Releve_fin"].fillna(fin)

    complet = ajouter_relevés(base, relevés)
    return complet

# TODO rename facturation depuis flux ou un truc du genre.
@pa.check_types
def facturation(
        deb: pd.Timestamp, 
        fin: pd.Timestamp, 
        historique: DataFrame[HistoriquePérimètre], 
        relevés: DataFrame[RelevéIndex]
        ) -> pd.DataFrame:
    """
    Calcule les énergies et les taxes pour une période donnée, sur l'ensemble du périmètre
    """
    # Base = pour tous les couples (ref, pdl), on a toutes les Entrées/Sorties du périmètre et le relevés associés
    base = préparer_base_énergies(historique=historique, deb=deb, fin=fin)

    # TODO: Gestion des cas spécifiques

    # Ajouter les dates de début et de fin pour le cas général (aka les na ici)
    base["Date_Releve_deb"] = base["Date_Releve_deb"].fillna(deb)
    base["Date_Releve_fin"] = base["Date_Releve_fin"].fillna(fin)

    # Ajouter les relevés manquants à la base.
    avec_relevés = base.copy()
    avec_relevés = ajouter_relevés(avec_relevés, relevés, '_deb')
    avec_relevés = ajouter_relevés(avec_relevés, relevés, '_fin')

    énergies = calculer_energies(avec_relevés)

    # colonnes_triees = sorted(énergies.columns)
    return énergies #.reindex(columns=colonnes_triees)