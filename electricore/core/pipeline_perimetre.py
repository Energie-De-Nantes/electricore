"""
Pipeline des fonctions de traitement du périmètre contractuel.

Ce module contient les fonctions déplacées depuis périmètre/fonctions.py
dans le cadre de la migration vers l'architecture unifiée models/ + pipelines.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame
from babel.dates import format_date

from electricore.core.models.historique_perimetre import HistoriquePérimètre
from electricore.core.models.releve_index import RelevéIndex


@pa.check_types
def detecter_points_de_rupture(historique: DataFrame[HistoriquePérimètre]) -> DataFrame[HistoriquePérimètre]:
    """
    Enrichit l'historique avec les colonnes d'impact (turpe, énergie, turpe_variable) et un résumé des modifications.
    Toutes les lignes sont conservées.

    Args:
        historique (pd.DataFrame): Historique complet des événements contractuels.

    Returns:
        pd.DataFrame: Historique enrichi avec détection des ruptures et résumé humain.
    """
    index_cols = ['BASE', 'HP', 'HC', 'HPH', 'HCH', 'HPB', 'HCB']

    historique = historique.sort_values(by=["Ref_Situation_Contractuelle", "Date_Evenement"]).copy()
    historique["Avant_Puissance_Souscrite"] = historique.groupby("Ref_Situation_Contractuelle")["Puissance_Souscrite"].shift(1)
    historique["Avant_Formule_Tarifaire_Acheminement"] = historique.groupby("Ref_Situation_Contractuelle")["Formule_Tarifaire_Acheminement"].shift(1)

    changement_fta = (
        historique["Avant_Formule_Tarifaire_Acheminement"].notna() &
        (historique["Avant_Formule_Tarifaire_Acheminement"] != historique["Formule_Tarifaire_Acheminement"])
    )
    
    impacte_abonnement = (
        (historique["Avant_Puissance_Souscrite"].notna() &
         (historique["Avant_Puissance_Souscrite"] != historique["Puissance_Souscrite"])) |
        changement_fta
    )
    
    changement_calendrier = (
        historique["Avant_Id_Calendrier_Distributeur"].notna() &
        historique["Après_Id_Calendrier_Distributeur"].notna() &
        (historique["Avant_Id_Calendrier_Distributeur"] != historique["Après_Id_Calendrier_Distributeur"])
    )
    
    changement_index = pd.concat([
        (historique[f"Avant_{col}"].notna() &
         historique[f"Après_{col}"].notna() &
         (historique[f"Avant_{col}"] != historique[f"Après_{col}"]))
        for col in index_cols
    ], axis=1).any(axis=1)

    impacte_energie = changement_calendrier | changement_index | changement_fta


    historique["impacte_abonnement"] = impacte_abonnement
    historique["impacte_energie"] = impacte_energie

    # Forcer les impacts à True pour les événements d’entrée et de sortie
    evenements_entree_sortie = ["CFNE", "MES", "PMES", "CFNS", "RES"]
    mask_entree_sortie = historique["Evenement_Declencheur"].isin(evenements_entree_sortie)

    historique.loc[mask_entree_sortie, ["impacte_abonnement", "impacte_energie"]] = True

    def generer_resume(row):
        modifs = []
        if row["impacte_abonnement"]:
            if pd.notna(row.get("Avant_Puissance_Souscrite")) and row["Avant_Puissance_Souscrite"] != row["Puissance_Souscrite"]:
                modifs.append(f"P: {row['Avant_Puissance_Souscrite']} → {row['Puissance_Souscrite']}")
            if pd.notna(row.get("Avant_Formule_Tarifaire_Acheminement")) and row["Avant_Formule_Tarifaire_Acheminement"] != row["Formule_Tarifaire_Acheminement"]:
                modifs.append(f"FTA: {row['Avant_Formule_Tarifaire_Acheminement']} → {row['Formule_Tarifaire_Acheminement']}")
        if row["impacte_energie"]:
            modifs.append("rupture index")
        if changement_calendrier.loc[row.name]:
            modifs.append(f"Cal: {row['Avant_Id_Calendrier_Distributeur']} → {row['Après_Id_Calendrier_Distributeur']}")
        return ", ".join(modifs) if modifs else ""

    historique["resume_modification"] = historique.apply(generer_resume, axis=1)

    return historique.reset_index(drop=True)




@pa.check_types
def inserer_evenements_facturation(historique: DataFrame[HistoriquePérimètre]) -> DataFrame[HistoriquePérimètre]:
    """
    Insère des événements de facturation artificielle au 1er de chaque mois.
    
    Cette fonction génère des événements "FACTURATION" pour permettre un calcul mensuel
    des abonnements. Elle traite chaque PDL individuellement selon sa période d'activité.
    
    LOGIQUE GLOBALE :
    1. Détecter les périodes d'activité de chaque PDL (entrée → sortie)
    2. Générer tous les 1ers du mois dans la plage globale
    3. Associer chaque PDL aux mois où il est actif
    4. Créer les événements artificiels et propager les données contractuelles
    
    Args:
        historique: DataFrame contenant l'historique des événements contractuels
        
    Returns:
        DataFrame étendu avec les événements de facturation artificiels
    """
    tz = "Europe/Paris"

    # =============================================================================
    # ÉTAPE 1 : DÉTECTION DES PÉRIODES D'ACTIVITÉ (INDIVIDUALISÉ PAR PDL)
    # =============================================================================
    
    # 1A. Définir la date limite pour les PDL non résiliés
    # LOGIQUE : génère des événements jusqu'au début du mois courant inclus
    fin_par_defaut = pd.Timestamp.now(tz=tz).to_period("M").start_time.tz_localize(tz)
    
    # 1B. Construire les périodes d'activité valides avec filtrage intégré
    periodes = (pd.DataFrame({
        "start": (historique
            .query("Evenement_Declencheur in ['CFNE', 'MES', 'PMES']")
            .groupby('Ref_Situation_Contractuelle')['Date_Evenement']
            .min()),
        "end": (historique
            .query("Evenement_Declencheur in ['RES', 'CFNS']")
            .groupby('Ref_Situation_Contractuelle')['Date_Evenement']
            .max())
    })
    .fillna(fin_par_defaut)
    # Filtrer les PDL entrés après la date limite (pas d'événements pour ces PDL)
    # LOGIQUE : Un PDL entré après le 1er du mois courant ne génère pas d'événements pour ce mois
    .query("start <= end"))
    
    if len(periodes) == 0:
        return historique  # Retourner l'historique original sans modification

    # =============================================================================
    # ÉTAPE 2 : GÉNÉRATION DES DATES MENSUELLES (GLOBAL)
    # =============================================================================
    
    # 2A. Générer tous les 1ers du mois dans la plage globale (inclus)
    # ASTUCE : ajouter 1 jour à max_date pour être sûr d'inclure le mois de fin
    all_months = pd.date_range(
        start=periodes["start"].min(),
        end=periodes["end"].max() + pd.DateOffset(days=1),
        freq="MS", 
        tz=tz
    )
    
    # =============================================================================
    # ÉTAPE 3-4 : CRÉATION DES ÉVÉNEMENTS ARTIFICIELS (PIPE UNIFIÉ)
    # =============================================================================
    
    # Créer les événements de facturation en pipe unique
    evenements = (
        periodes.reset_index()
        # Produit cartésien : chaque PDL valide × chaque mois
        .merge(pd.DataFrame({"Date_Evenement": all_months}), how="cross")
        # Ajouter le mapping Ref_Situation_Contractuelle → pdl
        .merge(
            historique[['Ref_Situation_Contractuelle', 'pdl']].drop_duplicates(), 
            on='Ref_Situation_Contractuelle', 
            how='left'
        )
        # Filtrer pour ne garder que les mois où chaque PDL est actif
        # CORRECTION : comparer les dates, pas les timestamps avec heures
        # Car les événements FACTURATION sont générés avec freq="MS" (début de mois avec heure)
        # alors que fin_par_defaut est à 00:00:00
        # IMPORTANT : on exclue le mois d'entrée, c'est le relevé CFNE/MES qui sera début de période
        .query("Date_Evenement.dt.date > start.dt.date and Date_Evenement.dt.date <= end.dt.date")
        # Ajouter les colonnes d'événements artificiels
        .assign(
            Evenement_Declencheur="FACTURATION",
            Type_Evenement="artificiel",
            Source="synthese_mensuelle",
            resume_modification="Facturation mensuelle",
            impacte_abonnement=True,
            impacte_energie=True
        )
        # Sélectionner les colonnes nécessaires
        [[
            "Ref_Situation_Contractuelle", "pdl", "Date_Evenement",
            "Evenement_Declencheur", "Type_Evenement", "Source", "resume_modification",
            "impacte_abonnement", "impacte_energie"
        ]]
    )

    # =============================================================================
    # ÉTAPE 5 : PROPAGATION DES DONNÉES CONTRACTUELLES (PIPE UNIFIÉ)
    # =============================================================================
    
    # Fusionner et propager les données contractuelles en pipe unique
    fusion = (
        pd.concat([historique, evenements], ignore_index=True)
        .sort_values(["Ref_Situation_Contractuelle", "Date_Evenement"])
        .reset_index(drop=True)
        .assign(**{
            # Propager les données par PDL avec forward fill
            # LOGIQUE : chaque événement artificiel hérite des caractéristiques du dernier événement réel
            col: lambda df, c=col: df.groupby("Ref_Situation_Contractuelle")[c].ffill()
            for col in [
                name for name, col_info in HistoriquePérimètre.to_schema().columns.items()
                if not col_info.nullable
            ]
            # Filtrer les colonnes présentes dans le DataFrame
            if col in pd.concat([historique, evenements], ignore_index=True).columns
        })
    )
    return fusion

@pa.check_types
def extraire_releves_evenements(historique: DataFrame[HistoriquePérimètre]) -> DataFrame[RelevéIndex]:
    """
    Génère des relevés d'index (avant/après) à partir d'un historique enrichi des événements contractuels.

    - Un relevé "avant" (ordre_index=0) est créé à partir des index Avant_*
    - Un relevé "après" (ordre_index=1) est créé à partir des index Après_*
    - La colonne 'ordre_index' permet de trier correctement les relevés successifs.

    Args:
        historique (pd.DataFrame): Historique enrichi (HistoriquePérimètreÉtendu).

    Returns:
        pd.DataFrame: Relevés d'index conformes au modèle RelevéIndex.
    """
    index_cols = ["BASE", "HP", "HC", "HCH", "HPH", "HPB", "HCB", "Id_Calendrier_Distributeur"]
    identifiants = ["pdl", "Ref_Situation_Contractuelle", "Formule_Tarifaire_Acheminement"]

    # Générer les relevés avant/après en pipe unique
    return (
        pd.concat([
            # Relevés "avant" (ordre_index=0)
            (historique[identifiants + ["Date_Evenement"] + [f"Avant_{col}" for col in index_cols]]
             .rename(columns={f"Avant_{col}": col for col in index_cols})
             .assign(ordre_index=0)),
            # Relevés "après" (ordre_index=1) 
            (historique[identifiants + ["Date_Evenement"] + [f"Après_{col}" for col in index_cols]]
             .rename(columns={f"Après_{col}": col for col in index_cols})
             .assign(ordre_index=1))
        ], ignore_index=True)
        # Filtrer les lignes avec des index valides
        .dropna(subset=index_cols, how="all")
        # Ajouter les métadonnées
        .assign(
            Source="flux_C15",
            Unité="kWh",
            Précision="kWh"
        )
        # Renommer la colonne de date
        .rename(columns={"Date_Evenement": "Date_Releve"})
        # Sélectionner les colonnes finales présentes dans le DataFrame
        .pipe(lambda df: df[[col for col in RelevéIndex.to_schema().columns.keys() if col in df.columns]])
    )


@pa.check_types
def enrichir_historique_périmètre(historique: DataFrame[HistoriquePérimètre]) -> DataFrame[HistoriquePérimètre]:
    """
    Enrichit l'historique du périmètre avec les points de rupture et les événements de facturation.
    
    Cette fonction combine deux traitements essentiels sur l'historique du périmètre :
    1. Détection des points de rupture (changements de périodes)
    2. Insertion des événements de facturation synthétiques (1er du mois)
    
    Utilisée comme étape préparatoire dans les pipelines de calcul d'abonnements et d'énergies.
    
    Args:
        historique: Historique des événements contractuels du périmètre
        
    Returns:
        DataFrame enrichi avec points de rupture détectés et événements de facturation
    """
    return (
        historique
        .pipe(detecter_points_de_rupture)
        .pipe(inserer_evenements_facturation)
    )


# Export des fonctions principales
__all__ = [
    'detecter_points_de_rupture',
    'inserer_evenements_facturation', 
    'extraire_releves_evenements',
    'enrichir_historique_périmètre'
]