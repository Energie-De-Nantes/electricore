"""
Pipeline de calcul des périodes d'énergie électrique.

Ce module contient toutes les fonctions de transformation des relevés d'index
en périodes d'énergie calculées avec validation de qualité et enrichissement
hiérarchique des cadrans.

Pipeline principal:
1. reconstituer_chronologie_relevés() - Reconstitution chronologique
2. calculer_periodes_energie() - Pipeline complet de calcul
   - preparer_releves() - Tri et normalisation
   - calculer_decalages_par_pdl() - Décalages par PDL
   - calculer_differences_cadrans() - Calcul vectorisé des énergies
   - calculer_flags_qualite() - Indicateurs de qualité
   - filtrer_periodes_valides() - Filtrage déclaratif
   - formater_colonnes_finales() - Formatage final
   - enrichir_cadrans_principaux() - Enrichissement hiérarchique
"""

import pandera.pandas as pa
import pandas as pd
import numpy as np
from toolz import curry
from pandera.typing import DataFrame

from electricore.core.périmètre import HistoriquePérimètre, extraire_releves_evenements
from electricore.core.relevés import RelevéIndex, interroger_relevés
from electricore.core.relevés.modèles import RequêteRelevé
from electricore.core.models.periode_energie import PeriodeEnergie
from electricore.core.taxes.turpe import ajouter_turpe_variable, load_turpe_rules


@curry
def reconstituer_chronologie_relevés(relevés: DataFrame[RelevéIndex],
                                    événements: DataFrame[HistoriquePérimètre]) -> pd.DataFrame:
    """
    Reconstitue la chronologie complète des relevés nécessaires pour la facturation.
    
    Assemble tous les relevés aux dates pertinentes en combinant :
    - Les relevés aux dates d'événements contractuels (flux C15 : MES, RES, MCT)
    - Les relevés aux dates de facturation (dates prises dans événements et mesures dans le flux R151 : dates spécifiques de facturation)
    
    Args:
        relevés: Relevés d'index quotidiens complets (flux R151)
        événements: Événements contractuels + événements FACTURATION
        
    Returns:
        DataFrame chronologique avec priorité: flux_C15 > flux_R151
    """
    # 1. Séparer les événements contractuels des événements FACTURATION
    evt_contractuels = événements[événements['Evenement_Declencheur'] != 'FACTURATION']
    evt_facturation = événements[événements['Evenement_Declencheur'] == 'FACTURATION']
    
    # 2. Extraire les relevés des événements contractuels
    rel_evenements = extraire_releves_evenements(evt_contractuels) if not evt_contractuels.empty else pd.DataFrame()
    
    # 3. Pour FACTURATION : construire requête et interroger les relevés existants
    if not evt_facturation.empty:
        requete = RequêteRelevé.validate(
            evt_facturation[['pdl', 'Date_Evenement']].rename(columns={'Date_Evenement': 'Date_Releve'})
        )
        rel_facturation = interroger_relevés(requete, relevés)
        
        # Si certains événements FACTURATION n'ont pas de relevé, créer des entrées factices
        facturation_avec_releves = rel_facturation['pdl'].astype(str) + '_' + rel_facturation['Date_Releve'].astype(str) if not rel_facturation.empty else set()
        requetes_manquantes = requete[~(requete['pdl'].astype(str) + '_' + requete['Date_Releve'].astype(str)).isin(facturation_avec_releves)]
        
        if not requetes_manquantes.empty:
            # Créer des relevés factices avec tous les index à NaN mais la structure complète
            index_cols = ['BASE', 'HP', 'HC', 'HCH', 'HPH', 'HPB', 'HCB']
            rel_factices = requetes_manquantes.copy()
            for col in index_cols + ['Id_Calendrier_Distributeur', 'Nature_Index']:
                rel_factices[col] = np.nan
            rel_factices['Source'] = 'FACTURATION'
            rel_factices['Unité'] = 'kWh'
            rel_factices['Précision'] = 'kWh'
            rel_factices['ordre_index'] = 0
            
            # Combiner relevés trouvés + relevés factices
            rel_facturation = pd.concat([rel_facturation, rel_factices], ignore_index=True) if not rel_facturation.empty else rel_factices
    else:
        rel_facturation = pd.DataFrame()

    # 4. Combiner, propager les références contractuelles, puis appliquer priorité alphabétique
    return (
        pd.concat([rel_evenements, rel_facturation], ignore_index=True)
        .sort_values(['pdl', 'Date_Releve', 'ordre_index'])  # Tri chronologique pour ffill
        .pipe(lambda df: df.assign(
            Ref_Situation_Contractuelle=df.groupby('pdl')['Ref_Situation_Contractuelle'].ffill(),
            Formule_Tarifaire_Acheminement=df.groupby('pdl')['Formule_Tarifaire_Acheminement'].ffill()
        ))
        .sort_values(['pdl', 'Date_Releve', 'Source']) # Flux_C15 < Flux_Rxx Alphabétiquement
        .drop_duplicates(subset=['Ref_Situation_Contractuelle', 'Date_Releve', 'ordre_index'], keep='first') # Déduplication par contrat
        .sort_values(['pdl', 'Date_Releve', 'ordre_index'])
        .reset_index(drop=True)
    )


@pa.check_types
def preparer_releves(relevés: DataFrame[RelevéIndex]) -> DataFrame[RelevéIndex]:
    """Prépare les relevés pour le calcul : tri et reset de l'index."""
    # Colonnes de tri : ordre_index est optionnel
    colonnes_tri = ['pdl', 'Date_Releve']
    if 'ordre_index' in relevés.columns:
        colonnes_tri.append('ordre_index')
    
    return (
        relevés
        .copy()
        .sort_values(colonnes_tri)
        .reset_index(drop=True)
    )


@pa.check_types
def calculer_decalages_par_pdl(relevés: DataFrame[RelevéIndex]) -> pd.DataFrame:
    """Calcule les décalages des relevés précédents par contrat (ou PDL) et enrichit le DataFrame."""
    # Déterminer la clé de groupement selon la présence de Ref_Situation_Contractuelle
    cle_groupement = 'Ref_Situation_Contractuelle' if 'Ref_Situation_Contractuelle' in relevés.columns else 'pdl'
    
    # Calculer les décalages pour les relevés précédents
    relevés_décalés = relevés.groupby(cle_groupement).shift(1)
    
    # Enrichir avec les données décalées et renommer
    return (
        relevés
        .assign(
            Date_Debut=relevés_décalés['Date_Releve'],
            source_avant=relevés_décalés['Source']
        )
        .rename(columns={
            'Date_Releve': 'Date_Fin',
            'Source': 'source_apres'
        })
    )


@pa.check_types
def calculer_differences_cadrans(data: pd.DataFrame, cadrans: list) -> pd.DataFrame:
    """Vectorise le calcul des énergies pour tous les cadrans présents."""
    résultat = data.copy()
    
    # Déterminer la clé de groupement selon la présence de Ref_Situation_Contractuelle
    cle_groupement = 'Ref_Situation_Contractuelle' if 'Ref_Situation_Contractuelle' in data.columns else 'pdl'
    
    # Récupérer les relevés décalés pour le calcul vectorisé
    relevés_décalés = data.groupby(cle_groupement).shift(1)
    
    # Calculer les différences pour tous les cadrans en une seule opération
    cadrans_présents = [c for c in cadrans if c in data.columns]
    
    if cadrans_présents:
        # Calcul vectorisé des différences
        différences = data[cadrans_présents].subtract(relevés_décalés[cadrans_présents], fill_value=np.nan)
        # Ajouter le suffixe _energie
        différences.columns = [f'{col}_energie' for col in différences.columns]
        résultat = pd.concat([résultat, différences], axis=1)
    
    # Ajouter les colonnes manquantes avec NaN
    cadrans_manquants = [c for c in cadrans if c not in data.columns]
    for cadran in cadrans_manquants:
        résultat[f'{cadran}_energie'] = np.nan
    
    return résultat


@pa.check_types
def calculer_flags_qualite(data: pd.DataFrame, cadrans: list) -> pd.DataFrame:
    """Calcule les flags de qualité des données de manière vectorisée."""
    colonnes_energie = [f'{cadran}_energie' for cadran in cadrans]
    colonnes_energie_présentes = [col for col in colonnes_energie if col in data.columns]
    
    return (
        data
        .assign(
            data_complete=data[colonnes_energie_présentes].notna().any(axis=1) if colonnes_energie_présentes else False,
            duree_jours=(data['Date_Fin'] - data['Date_Debut']).dt.days.astype('Int64')
        )
        .assign(periode_irreguliere=lambda df: (df['duree_jours'] > 35).fillna(False).astype(bool))
    )


@pa.check_types
def formater_colonnes_finales(data: pd.DataFrame, cadrans: list) -> DataFrame[PeriodeEnergie]:
    """Sélectionne et formate les colonnes finales du résultat."""
    colonnes_base = [
        'pdl', 'Date_Debut', 'Date_Fin', 'duree_jours',
        'source_avant', 'source_apres', 
        'data_complete', 'periode_irreguliere'
    ]
    
    # Ajouter les colonnes contractuelles si présentes
    colonnes_contractuelles = ['Ref_Situation_Contractuelle', 'Formule_Tarifaire_Acheminement']
    for col in colonnes_contractuelles:
        if col in data.columns:
            colonnes_base.append(col)
    
    colonnes_energie = [f'{cadran}_energie' for cadran in cadrans if f'{cadran}_energie' in data.columns]
    colonnes_finales = colonnes_base + colonnes_energie
    
    return data[colonnes_finales].copy()


@pa.check_types
def filtrer_periodes_valides(data: pd.DataFrame) -> pd.DataFrame:
    """Filtre les périodes invalides de manière déclarative."""
    return (
        data
        .dropna(subset=['Date_Debut'])  # Éliminer les premiers relevés sans début
        .query('Date_Debut != Date_Fin')  # Éliminer les périodes de durée zéro
        .reset_index(drop=True)
    )



@pa.check_types
def enrichir_cadrans_principaux(data: DataFrame[PeriodeEnergie]) -> DataFrame[PeriodeEnergie]:
    """
    Enrichit les cadrans principaux avec synthèse hiérarchique des énergies.
    
    Effectue une synthèse en cascade pour créer une hiérarchie complète des cadrans :
    1. HC_energie = somme(HC_energie, HCH_energie, HCB_energie) si au moins une valeur
    2. HP_energie = somme(HP_energie, HPH_energie, HPB_energie) si au moins une valeur  
    3. BASE_energie = somme(BASE_energie, HP_energie, HC_energie) si au moins une valeur
    
    Cette fonction gère les différents niveaux de précision des compteurs :
    - Compteurs 4 cadrans : HPH/HPB + HCH/HCB → HP + HC → BASE
    - Compteurs HP/HC : HP + HC → BASE
    - Compteurs simples : BASE inchangé
    
    Args:
        data: DataFrame[PeriodeEnergie] avec les énergies calculées
        
    Returns:
        DataFrame[PeriodeEnergie] avec les cadrans principaux enrichis
    """
    résultat = data.copy()
    
    # Étape 1 : Synthèse HC depuis les sous-cadrans HCH et HCB
    colonnes_hc = ['HC_energie', 'HCH_energie', 'HCB_energie']
    colonnes_hc_présentes = [col for col in colonnes_hc if col in résultat.columns]
    if colonnes_hc_présentes:
        résultat['HC_energie'] = résultat[colonnes_hc_présentes].sum(axis=1, min_count=1)
    
    # Étape 2 : Synthèse HP depuis les sous-cadrans HPH et HPB  
    colonnes_hp = ['HP_energie', 'HPH_energie', 'HPB_energie']
    colonnes_hp_présentes = [col for col in colonnes_hp if col in résultat.columns]
    if colonnes_hp_présentes:
        résultat['HP_energie'] = résultat[colonnes_hp_présentes].sum(axis=1, min_count=1)
    
    # Étape 3 : Synthèse BASE depuis HP et HC (utilise les valeurs enrichies des étapes précédentes)
    colonnes_base = ['BASE_energie', 'HP_energie', 'HC_energie']
    colonnes_base_présentes = [col for col in colonnes_base if col in résultat.columns]
    if colonnes_base_présentes:
        résultat['BASE_energie'] = résultat[colonnes_base_présentes].sum(axis=1, min_count=1)
    
    return résultat


@pa.check_types
def calculer_periodes_energie(relevés: DataFrame[RelevéIndex]) -> DataFrame[PeriodeEnergie]:
    """
    Calcule les périodes d'énergie avec flags de qualité des données.
    
    🔄 **Version refactorisée** - Approche fonctionnelle optimisée :
    - **Pipeline déclaratif** avec pandas.pipe() pour une meilleure lisibilité
    - **Vectorisation maximale** des calculs d'énergies (élimination des boucles explicites)
    - **Typage Pandera strict** avec validation automatique des données
    - **Fonctions pures** facilement testables et maintenables
    - **Performance améliorée** grâce aux optimisations vectorielles
    
    Pipeline de transformation :
    1. `preparer_releves()` - Tri et normalisation des relevés
    2. `calculer_decalages_par_pdl()` - Calcul des décalages par PDL avec groupby
    3. `calculer_differences_cadrans()` - Calcul vectorisé des énergies tous cadrans
    4. `calculer_flags_qualite()` - Indicateurs de qualité vectorisés
    5. `filtrer_periodes_valides()` - Filtrage déclaratif avec query()
    6. `formater_colonnes_finales()` - Sélection et formatage final
    7. `enrichir_cadrans_principaux()` - Enrichissement hiérarchique HC, HP, BASE
    
    Args:
        relevés: DataFrame[RelevéIndex] avec relevés d'index chronologiques
        
    Returns:
        DataFrame[PeriodeEnergie] avec périodes d'énergie calculées et validées
        
    Raises:
        SchemaError: Si les données d'entrée ne respectent pas le modèle RelevéIndex
    """
    # Cadrans d'index électriques standard
    cadrans = ["BASE", "HP", "HC", "HPH", "HPB", "HCH", "HCB"]
    
    return (
        relevés
        .pipe(preparer_releves)
        .pipe(calculer_decalages_par_pdl)
        .pipe(calculer_differences_cadrans, cadrans=cadrans)
        .pipe(calculer_flags_qualite, cadrans=cadrans)
        .pipe(filtrer_periodes_valides)  # Filtrer avant le formatage
        .pipe(formater_colonnes_finales, cadrans=cadrans)
        .pipe(enrichir_cadrans_principaux)  # Enrichissement hiérarchique des cadrans
    )


def pipeline_energie(
    historique: DataFrame[HistoriquePérimètre], 
    relevés: DataFrame[RelevéIndex]
) -> DataFrame[PeriodeEnergie]:
    """
    Pipeline complète pour générer les périodes d'énergie avec calcul TURPE optionnel.
    
    Orchestre toute la chaîne de traitement :
    1. Détection des points de rupture
    2. Insertion des événements de facturation  
    3. Combinaison des relevés événements + mensuels
    4. Génération de la grille complète de facturation
    5. Calcul des périodes d'énergie avec flags qualité
    6. Enrichissement avec FTA et calcul TURPE variable (optionnel)
    
    Args:
        historique: DataFrame contenant l'historique des événements contractuels
        relevés: DataFrame contenant les relevés d'index R151
        avec_turpe: Si True, enrichit avec le calcul du TURPE variable
        
    Returns:
        DataFrame[PeriodeEnergie] avec les périodes d'énergie calculées et optionnellement le TURPE
    """
    from electricore.core.pipeline_commun import pipeline_commun
    
    # Préparer l'historique filtré
    periodes_energie = (
        historique
        .pipe(pipeline_commun)
        .query("impact_energie or impact_turpe_variable or Evenement_Declencheur == 'FACTURATION'")
        .pipe(reconstituer_chronologie_relevés(relevés))
        .pipe(calculer_periodes_energie)
        .pipe(ajouter_turpe_variable(load_turpe_rules()))
    )
    
    return periodes_energie