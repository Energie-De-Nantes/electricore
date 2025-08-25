"""
Pipeline pur d'agrégation de facturation en méta-périodes mensuelles.

Ce module agrège les périodes d'abonnement et d'énergie déjà calculées 
pour produire des méta-périodes mensuelles optimisées pour la facturation.

L'agrégation utilise une puissance moyenne pondérée par nombre de jours,
mathématiquement équivalente au calcul détaillé grâce à la linéarité
de la formule de tarification.

Pipeline principal:
- pipeline_facturation() - Pipeline pur d'agrégation des méta-périodes

⚠️ Ce pipeline attend des périodes déjà calculées.
   Pour l'orchestration complète, voir orchestration.py
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame
from toolz import curry

from electricore.core.models.periode_abonnement import PeriodeAbonnement
from electricore.core.models.periode_energie import PeriodeEnergie
from electricore.core.models.periode_meta import PeriodeMeta
from electricore.core.utils.formatage import formater_date_francais


def _construire_memo_puissance(group_memo_values: pd.Series) -> str:
    """
    Construit un mémo lisible des périodes d'abonnement.
    
    Ne génère un mémo que s'il y a des changements de puissance réels.
    
    Args:
        group_memo_values: Série des valeurs memo_puissance pour un groupe
        
    Returns:
        Chaîne vide si pas de changement de puissance, historique détaillé sinon
    """
    if len(group_memo_values) <= 1:
        return ""
    
    # Extraire les puissances de chaque mémo pour vérifier s'il y a changement
    puissances = []
    for memo in group_memo_values:
        # Extraire la puissance du format "14j à 6kVA"
        if "à " in memo and "kVA" in memo:
            puissance_str = memo.split("à ")[1].replace("kVA", "")
            puissances.append(float(puissance_str))
    
    # Si toutes les puissances sont identiques, pas de mémo
    if len(set(puissances)) <= 1:
        return ""
    
    # Sinon, joindre tous les mémos
    return ", ".join(group_memo_values)


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
            puissance_ponderee=lambda x: x['Puissance_Souscrite'] * x['nb_jours'],
            # Colonne pour le mémo (sera agrégée après)
            memo_puissance=lambda x: x.apply(lambda row: f"{row['nb_jours']}j à {int(row['Puissance_Souscrite'])}kVA", axis=1)
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
            'Ref_Situation_Contractuelle': 'size',  # Temporaire pour compter
            # Mémo d'historique des puissances (si changements réels) 
            'memo_puissance': lambda x: _construire_memo_puissance(x)
        })
        .rename(columns={'Ref_Situation_Contractuelle': 'nb_sous_periodes_abo'})
        .assign(
            # Calcul final de la puissance moyenne
            puissance_moyenne=lambda x: x['puissance_ponderee'] / x['nb_jours'],
            # Flag de changement si plus d'une sous-période
            has_changement_abo=lambda x: x['nb_sous_periodes_abo'] > 1
        )
        .drop(columns=['puissance_ponderee'])
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
        'debut': 'min',
        'fin': 'max',
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
    
    # Gestion des valeurs manquantes et réconciliation des colonnes de dates
    return (
        meta_periodes
        .assign(
            # Réconcilier les colonnes de dates (priorité aux abonnements)
            debut=lambda x: x['debut_abo'].fillna(x['debut_energie']) if 'debut_energie' in x.columns else x['debut_abo'],
            fin=lambda x: x['fin_abo'].fillna(x['fin_energie']) if 'fin_energie' in x.columns else x['fin_abo'],
            
            # Réconcilier nb_jours (calculer si manquant)
            nb_jours=lambda x: x['nb_jours'].fillna(
                (x['fin'].dt.normalize() - x['debut'].dt.normalize()).dt.days
            ).astype(int),
            
            # Réconcilier les autres colonnes d'abonnement manquantes
            puissance_moyenne=lambda x: x['puissance_moyenne'].fillna(0),
            Formule_Tarifaire_Acheminement=lambda x: x['Formule_Tarifaire_Acheminement'].fillna('INCONNU'),
            turpe_fixe=lambda x: x['turpe_fixe'].fillna(0),
            
            # Si pas de sous-périodes d'énergie, mettre 0 au lieu de NaN
            nb_sous_periodes_energie=lambda x: x['nb_sous_periodes_energie'].fillna(0).astype(int),
            nb_sous_periodes_abo=lambda x: x['nb_sous_periodes_abo'].fillna(1).astype(int),
            has_changement_energie=lambda x: x['has_changement_energie'].fillna(False),
            has_changement_abo=lambda x: x['has_changement_abo'].fillna(False),
            
            # Flag de changement global
            has_changement=lambda x: x['has_changement_abo'] | x['has_changement_energie']
        )
        .drop(columns=['has_changement_abo', 'has_changement_energie', 'debut_abo', 'debut_energie', 'fin_abo', 'fin_energie'], errors='ignore')
    )


@pa.check_types
def pipeline_facturation(
    periodes_abonnement: DataFrame[PeriodeAbonnement],
    periodes_energie: DataFrame[PeriodeEnergie]
) -> DataFrame[PeriodeMeta]:
    """
    Pipeline pur d'agrégation de facturation avec méta-périodes mensuelles.
    
    ⚠️  ATTEND des périodes déjà calculées (abonnements + énergie).
        Pour l'orchestration complète, utiliser orchestration.facturation()
    
    Pipeline d'agrégation pur :
    1. Agrégation mensuelle des abonnements (puissance moyenne pondérée)
    2. Agrégation mensuelle des énergies (sommes simples)
    3. Jointure des agrégats
    4. Formatage et tri final
    
    Args:
        periodes_abonnement: DataFrame des périodes d'abonnement avec TURPE fixe
        periodes_energie: DataFrame des périodes d'énergie avec TURPE variable
        
    Returns:
        DataFrame[PeriodeMeta] avec les méta-périodes mensuelles de facturation
    """
    # Pipeline pur d'agrégation - pas d'appel aux autres pipelines
    meta_periodes = (
        agreger_abonnements_mensuel(periodes_abonnement)
        .pipe(joindre_agregats(agreger_energies_mensuel(periodes_energie)))
        .assign(
            debut_lisible=lambda x: x['debut'].apply(formater_date_francais),
            fin_lisible=lambda x: x['fin'].apply(formater_date_francais)
        )
        .sort_values(['Ref_Situation_Contractuelle','debut'])
    )
    
    return meta_periodes


# Export des fonctions principales
__all__ = [
    'pipeline_facturation',
    'agreger_abonnements_mensuel', 
    'agreger_energies_mensuel',
    'joindre_agregats'
]