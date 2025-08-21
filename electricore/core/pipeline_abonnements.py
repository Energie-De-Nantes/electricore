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
from electricore.core.models import PeriodeAbonnement
from electricore.core.taxes.turpe import ajouter_turpe_fixe, load_turpe_rules

from babel.dates import format_date


@pa.check_types
def generer_periodes_abonnement(historique: DataFrame[HistoriquePérimètre]) -> DataFrame[PeriodeAbonnement]:
    """
    Génère les périodes homogènes d'abonnement à partir des événements impactant le TURPE fixe.
    """
    # 1. Filtrer les événements pertinents
    filtres = (
        (historique["impact_turpe_fixe"] == True) &
        (historique["Ref_Situation_Contractuelle"].notna())
    )
    abonnements = historique[filtres].copy()

    # 2. Trier par ref et date
    abonnements = abonnements.sort_values(["Ref_Situation_Contractuelle", "Date_Evenement"])

    # 3. Construire les débuts et fins de période
    abonnements["periode_debut"] = abonnements["Date_Evenement"]
    abonnements["periode_fin"] = abonnements.groupby("Ref_Situation_Contractuelle")["Date_Evenement"].shift(-1)

    # 4. Ne garder que les lignes valides
    periodes = abonnements.dropna(subset=["periode_fin"]).copy()

    # 5. Ajouter nb jours (arrondi à la journée, pas de time)
    periodes["nb_jours"] = (periodes["periode_fin"].dt.normalize() - periodes["periode_debut"].dt.normalize()).dt.days

    # 6. Ajout lisibles
    periodes['periode_debut_lisible'] = periodes['periode_debut'].apply(
        lambda d: format_date(d, "d MMMM yyyy", locale="fr_FR") if not pd.isna(d) else None
    )
    periodes['periode_fin_lisible'] = periodes['periode_fin'].apply(
        lambda d: format_date(d, "d MMMM yyyy", locale="fr_FR") if not pd.isna(d) else "en cours"
    )

    periodes['mois_annee'] = periodes['periode_debut'].apply(
        lambda d: format_date(d, "LLLL yyyy", locale="fr_FR")
    )
    return periodes[[
        "Ref_Situation_Contractuelle",
        "pdl",
        "mois_annee",
        "periode_debut_lisible",
        "periode_fin_lisible",
        "Formule_Tarifaire_Acheminement",
        "Puissance_Souscrite",
        "nb_jours",
        "periode_debut",
    ]].reset_index(drop=True)


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