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
from icecream import ic
import pandera.pandas as pa
from pandera.typing import DataFrame

from electricore.inputs.flux import (
    FluxC15, FluxR151, 
    lire_flux_c15, lire_flux_r151
)
from electricore.core.périmètre import (
    HistoriquePérimètre,
    ModificationContractuelleImpactante,
    extraire_historique_à_date,
    extraire_période,
    extraire_modifications_impactantes
)
from electricore.core.relevés import (
    RelevéIndex, 
)
from electricore.core.énergies.fonctions import (
    BaseCalculEnergies,
    préparer_base_énergies, 
    ajouter_relevés, 
    calculer_energies
)
from electricore.core.models.periode_energie import PeriodeEnergie
from electricore.core.taxes.turpe import (
    get_applicable_rules,
    compute_turpe,
    load_turpe_rules,
    ajouter_turpe_fixe
)
from electricore.core.périmètre.fonctions import (
    detecter_points_de_rupture,
    inserer_evenements_facturation,
    enrichir_historique_périmètre
)

# Import de la nouvelle API d'orchestration
from electricore.core.orchestration import (
    facturation,
    calculer_abonnements,
    calculer_energie,
    calculer_historique_enrichi
)
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

# NOTE: Cette fonction utilise l'ancienne approche avec gestion de dates spécifiques
# Pour les nouveaux développements, considérer orchestration.facturation() qui offre une API plus moderne
@pa.check_types
def facturation(
        deb: pd.Timestamp, 
        fin: pd.Timestamp, 
        historique: DataFrame[HistoriquePérimètre], 
        relevés: DataFrame[RelevéIndex],
        inclure_jour_fin: bool=False
        ) -> pd.DataFrame:
    """
    Calcule les énergies et les taxes pour une période donnée, sur l'ensemble du périmètre.
    
    NOTE: Ancienne approche avec gestion manuelle des dates et cas spécifiques.
    Pour une approche plus moderne, voir orchestration.facturation().
    """
    historique: DataFrame[HistoriquePérimètre] = extraire_historique_à_date(historique=historique, fin=fin)
    # Base = pour tous les couples (ref, pdl), on a toutes les Entrées/Sorties du périmètre et le relevés associés
    base: DataFrame[BaseCalculEnergies] = préparer_base_énergies(historique=historique, deb=deb, fin=fin)

    # TODO: Gestion des cas spécifiques
    # Pour l'instant, on les met juste de coté.
    cas_spécifiques: DataFrame[ModificationContractuelleImpactante] = extraire_modifications_impactantes(deb, historique)
    # Filtrer la base pour enlever les Ref_Contractuelle présentes dans cas_spécifiques
    if not cas_spécifiques.empty:
        refs_à_exclure = cas_spécifiques["Ref_Situation_Contractuelle"].unique()
        base = base[~base["Ref_Situation_Contractuelle"].isin(refs_à_exclure)]
        print(f"Filtrage de {len(refs_à_exclure)} références contractuelles avec des modifications impactantes")

    # Ajouter les dates de début et de fin pour le cas général (aka les na ici)
    base["Date_Releve_deb"] = base["Date_Releve_deb"].fillna(deb)
    base["Date_Releve_fin"] = base["Date_Releve_fin"].fillna(fin)

    # Ajouter les relevés manquants à la base.
    avec_relevés = base.copy()
    avec_relevés = ajouter_relevés(avec_relevés, relevés, '_deb')
    avec_relevés = ajouter_relevés(avec_relevés, relevés, '_fin')

    énergies = calculer_energies(avec_relevés, inclure_jour_fin)

    régles_turpe = get_applicable_rules(deb, fin)

    turpe = compute_turpe(énergies, régles_turpe)

    # colonnes_triees = sorted(énergies.columns)
    return turpe #.reindex(columns=colonnes_triees)








# ======================================================================
# FONCTION SUPPRIMÉE - calculer_abonnements_et_energies()
# 
# Cette fonction legacy a été remplacée par l'orchestration moderne.
# Utiliser à la place :
#   - orchestration.facturation() pour le pipeline complet
#   - orchestration.calculer_abonnements() pour juste les abonnements  
#   - orchestration.calculer_energie() pour juste l'énergie
# ======================================================================