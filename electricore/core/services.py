import pandas as pd

from electricore.core.energies import ajout_dates_par_defaut, calcul_energie
from electricore.core.turpe import get_applicable_rules, compute_turpe
from electricore.core.format import supprimer_colonnes, fusion_des_sous_periode, validation

def energies_et_taxes(deb: pd.Timestamp, fin: pd.Timestamp, base: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les énergies et les taxes pour une période donnée, sur l'ensemble du périmètre
    """
    
    complet = ajout_dates_par_defaut(deb, fin, base)
    energies = calcul_energie(complet)
    energies['Puissance_Souscrite'] = pd.to_numeric(energies['Puissance_Souscrite'])
    rules = get_applicable_rules(deb, fin)
    turpe = compute_turpe(entries=energies, rules=rules)
    final = validation(
        supprimer_colonnes(
        fusion_des_sous_periode(turpe)))
    return final.round(2)