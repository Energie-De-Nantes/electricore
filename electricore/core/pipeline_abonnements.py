"""
Pipeline pur de calcul des périodes d'abonnement avec TURPE.

Ce module contient le pipeline pour générer les périodes d'abonnement
enrichies avec les tarifs TURPE fixes, à partir d'un historique déjà enrichi.

Pipeline principal:
- pipeline_abonnement() - Pipeline pur de calcul des abonnements
  1. Génération des périodes d'abonnement (à partir d'historique enrichi)
  2. Ajout du TURPE fixe
  
⚠️ Ce pipeline attend un historique déjà enrichi (après pipeline_commun).
   Pour l'orchestration complète, voir orchestration.py
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame

from electricore.core.périmètre import HistoriquePérimètre
from electricore.core.models import PeriodeAbonnement
from electricore.core.taxes.turpe import ajouter_turpe_fixe, load_turpe_rules
from electricore.core.utils.formatage import formater_date_francais


def calculer_bornes_periodes(abonnements: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les bornes de début et fin de période pour chaque contrat.
    
    Trie par contrat et date, puis utilise shift(-1) pour déterminer
    la fin de chaque période.
    """
    df = abonnements.sort_values(["Ref_Situation_Contractuelle", "Date_Evenement"])
    
    return df.assign(
        debut=df["Date_Evenement"],
        fin=df.groupby("Ref_Situation_Contractuelle")["Date_Evenement"].shift(-1)
    )


def selectionner_colonnes_abonnement(periodes: pd.DataFrame) -> pd.DataFrame:
    """
    Sélectionne et réordonne les colonnes finales pour les périodes d'abonnement.
    """
    colonnes_finales = [
        "Ref_Situation_Contractuelle",
        "pdl",
        "mois_annee",
        "debut_lisible",
        "fin_lisible",
        "Formule_Tarifaire_Acheminement",
        "Puissance_Souscrite",
        "nb_jours",
        "debut",
        "fin",
    ]
    return periodes[colonnes_finales].reset_index(drop=True)


@pa.check_types
def generer_periodes_abonnement(historique: DataFrame[HistoriquePérimètre]) -> DataFrame[PeriodeAbonnement]:
    """
    Génère les périodes homogènes d'abonnement à partir des événements impactant le TURPE fixe.
    
    Pipeline de transformation :
    1. Filtre les événements pertinents avec query
    2. Calcule les bornes de période (début/fin)
    3. Élimine les périodes incomplètes
    4. Enrichit avec les données calculées et formatées
    5. Sélectionne les colonnes finales
    """
    return (
        historique
        .query("impacte_abonnement == True and Ref_Situation_Contractuelle.notna()")
        .pipe(calculer_bornes_periodes)
        .dropna(subset=["fin"])
        .assign(
            nb_jours=lambda df: (df["fin"].dt.normalize() - df["debut"].dt.normalize()).dt.days,
            debut_lisible=lambda df: df["debut"].apply(formater_date_francais),
            fin_lisible=lambda df: df["fin"].apply(
                lambda d: formater_date_francais(d) if pd.notna(d) else "en cours"
            ),
            mois_annee=lambda df: df["debut"].apply(
                lambda d: formater_date_francais(d, "LLLL yyyy")
            )
        )
        .pipe(selectionner_colonnes_abonnement)
    )


@pa.check_types
def pipeline_abonnement(historique_enrichi: DataFrame[HistoriquePérimètre]) -> pd.DataFrame:
    """
    Pipeline pur pour générer les périodes d'abonnement avec TURPE.
    
    ⚠️  ATTEND un historique déjà enrichi (après pipeline_commun).
        Pour l'orchestration complète, utiliser orchestration.calculer_abonnements()
    
    Pipeline de transformation :
    1. Génération des périodes d'abonnement à partir d'historique enrichi
    2. Ajout du TURPE fixe
    
    Args:
        historique_enrichi: DataFrame enrichi avec événements de facturation
        
    Returns:
        DataFrame avec les périodes d'abonnement enrichies du TURPE fixe
    """
    # Pipeline pur - pas d'appel à pipeline_commun
    return (
        historique_enrichi
        .pipe(generer_periodes_abonnement)
        .pipe(ajouter_turpe_fixe(load_turpe_rules()))
    )