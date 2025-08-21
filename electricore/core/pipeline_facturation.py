"""
Pipeline de facturation agrégée en méta-périodes mensuelles.

Ce module orchestre les pipelines d'abonnement et d'énergie pour produire
des méta-périodes mensuelles optimisées pour la facturation.

L'agrégation utilise une puissance moyenne pondérée par nombre de jours,
mathématiquement équivalente au calcul détaillé grâce à la linéarité
de la formule de tarification.

Pipeline principal:
- pipeline_facturation() - Pipeline complet produisant les méta-périodes
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame
from toolz import curry

from electricore.core.périmètre import HistoriquePérimètre
from electricore.core.relevés import RelevéIndex
from electricore.core.models.periode_abonnement import PeriodeAbonnement
from electricore.core.models.periode_energie import PeriodeEnergie
from electricore.core.models.periode_meta import PeriodeMeta
from electricore.core.pipeline_abonnements import pipeline_abonnement
from electricore.core.pipeline_energie import pipeline_energie
from electricore.core.utils.formatage import formater_date_francais


def agreger_abonnements_mensuel(abonnements: DataFrame[PeriodeAbonnement]) -> pd.DataFrame:
    """
    Agrège les périodes d'abonnement par mois avec puissance moyenne pondérée.
    
    Utilise la propriété de linéarité de la tarification pour calculer
    une puissance moyenne pondérée par nb_jours, mathématiquement équivalente
    au calcul détaillé par sous-périodes.
    
    Args:
        abonnements: DataFrame des périodes d'abonnement détaillées
        
    Returns:
        DataFrame agrégé par mois avec puissance moyenne pondérée
    """
    if abonnements.empty:
        return pd.DataFrame()
    
    return (
        abonnements
        .assign(
            # Calcul intermédiaire pour la moyenne pondérée
            puissance_ponderee=lambda x: x['Puissance_Souscrite'] * x['nb_jours']
        )
        .groupby(['Ref_Situation_Contractuelle', 'pdl', 'mois_annee'])
        .agg({
            'nb_jours': 'sum',
            'puissance_ponderee': 'sum',
            'turpe_fixe': 'sum',
            'Formule_Tarifaire_Acheminement': 'first',  # Identique dans le mois
            'debut': 'min',
            'fin': 'max',
            # Comptage des sous-périodes pour métadonnées
            'Ref_Situation_Contractuelle': 'size'  # Temporaire pour compter
        })
        .rename(columns={'Ref_Situation_Contractuelle': 'nb_sous_periodes_abo'})
        .assign(
            # Calcul final de la puissance moyenne
            puissance_moyenne=lambda x: x['puissance_ponderee'] / x['nb_jours'],
            debut_mois=lambda x: x['debut'],
            fin_mois=lambda x: x['fin'],
            # Flag de changement si plus d'une sous-période
            has_changement_abo=lambda x: x['nb_sous_periodes_abo'] > 1
        )
        .drop(columns=['puissance_ponderee', 'debut', 'fin'])
        .reset_index()
    )


def agreger_energies_mensuel(energies: DataFrame[PeriodeEnergie]) -> pd.DataFrame:
    """
    Agrège les périodes d'énergie par mois avec sommes simples.
    
    Les énergies sont additives, donc on peut simplement sommer
    toutes les valeurs par mois.
    
    Args:
        energies: DataFrame des périodes d'énergie détaillées
        
    Returns:
        DataFrame agrégé par mois avec énergies sommées
    """
    if energies.empty:
        return pd.DataFrame()
    
    # Colonnes d'énergie à agréger (certaines peuvent être absentes)
    colonnes_energie = ['BASE_energie', 'HP_energie', 'HC_energie']
    colonnes_energie_presentes = [col for col in colonnes_energie if col in energies.columns]
    
    # Préparer l'agrégation
    agg_dict = {
        col: 'sum' for col in colonnes_energie_presentes
    }
    agg_dict.update({
        'turpe_variable': 'sum',
        'Ref_Situation_Contractuelle': 'size'  # Temporaire pour compter
    })
    
    return (
        energies
        .groupby(['Ref_Situation_Contractuelle', 'pdl', 'mois_annee'])
        .agg(agg_dict)
        .rename(columns={'Ref_Situation_Contractuelle': 'nb_sous_periodes_energie'})
        .assign(
            # Flag de changement si plus d'une sous-période
            has_changement_energie=lambda x: x['nb_sous_periodes_energie'] > 1
        )
        .reset_index()
    )


@curry
def joindre_agregats(ener_mensuel: pd.DataFrame, abo_mensuel: pd.DataFrame) -> pd.DataFrame:
    """
    Joint les agrégats d'abonnement et d'énergie sur les clés communes.
    
    Args:
        ener_mensuel: DataFrame agrégé des énergies
        abo_mensuel: DataFrame agrégé des abonnements
        
    Returns:
        DataFrame joint avec toutes les données de facturation
    """
    # Clés de jointure
    cles_jointure = ['Ref_Situation_Contractuelle', 'pdl', 'mois_annee']
    
    # Jointure externe pour gérer les cas où une période existe d'un côté seulement
    meta_periodes = pd.merge(
        abo_mensuel, 
        ener_mensuel, 
        on=cles_jointure, 
        how='outer',
        suffixes=('_abo', '_energie')
    )
    
    # Gestion des valeurs manquantes
    return (
        meta_periodes
        .assign(
            # Si pas de sous-périodes d'énergie, mettre 0 au lieu de NaN
            nb_sous_periodes_energie=lambda x: x['nb_sous_periodes_energie'].fillna(0).astype(int),
            has_changement_energie=lambda x: x['has_changement_energie'].fillna(False),
            
            # Flag de changement global
            has_changement=lambda x: x['has_changement_abo'] | x['has_changement_energie']
        )
        .drop(columns=['has_changement_abo', 'has_changement_energie'], errors='ignore')
    )


@pa.check_types
def pipeline_facturation(
    historique: DataFrame[HistoriquePérimètre], 
    relevés: DataFrame[RelevéIndex]
) -> DataFrame[PeriodeMeta]:
    """
    Pipeline complet de facturation avec méta-périodes mensuelles.
    
    Orchestre les pipelines d'abonnement et d'énergie pour produire
    des méta-périodes optimisées pour la facturation, en utilisant
    l'agrégation mensuelle mathématiquement équivalente.
    
    Pipeline :
    1. Génération des périodes d'abonnement détaillées
    2. Génération des périodes d'énergie détaillées  
    3. Agrégation mensuelle des abonnements (puissance moyenne pondérée)
    4. Agrégation mensuelle des énergies (sommes simples)
    5. Jointure des agrégats
    6. Ajout des métadonnées et formatage
    
    Args:
        historique: DataFrame contenant l'historique des événements contractuels
        relevés: DataFrame contenant les relevés d'index R151
        
    Returns:
        DataFrame[PeriodeMeta] avec les méta-périodes mensuelles de facturation
    """
    # Génération des périodes détaillées
    periodes_abonnement = pipeline_abonnement(historique)
    periodes_energie = pipeline_energie(historique, relevés)
    
    # Pipeline d'agrégation avec pandas.pipe()
    meta_periodes = (
        agreger_abonnements_mensuel(periodes_abonnement)
        .pipe(joindre_agregats(agreger_energies_mensuel(periodes_energie)))
        .assign(
            debut_lisible=lambda x: x['debut_mois'].apply(formater_date_francais),
            fin_lisible=lambda x: x['fin_mois'].apply(formater_date_francais)
        )
    )
    
    return meta_periodes


# Export des fonctions principales
__all__ = [
    'pipeline_facturation',
    'agreger_abonnements_mensuel', 
    'agreger_energies_mensuel',
    'joindre_agregats'
]