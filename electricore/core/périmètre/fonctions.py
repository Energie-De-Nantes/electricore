import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame
from babel.dates import format_date

from electricore.core.périmètre.modèles import HistoriquePérimètre, SituationPérimètre, ModificationContractuelleImpactante
from electricore.core.relevés.modèles import RelevéIndex

@pa.check_types
def extraire_situation(date: pd.Timestamp, historique: DataFrame[HistoriquePérimètre]) -> DataFrame[SituationPérimètre]:
    """
    Extrait la situation du périmètre à une date donnée.
    
    Args:
        date (pd.Timestamp): La date de référence.
        historique (pd.DataFrame): L'historique des événements contractuels.

    Returns:
        pd.DataFrame: Une vue du périmètre à `date`, conforme à `SituationPérimètre`.
    """
    return (
        historique[historique["Date_Evenement"] <= date]
        .sort_values(by="Date_Evenement", ascending=False)
        .drop_duplicates(subset=["Ref_Situation_Contractuelle"], keep="first")
    )
@pa.check_types
def extraire_historique_à_date(
    historique: DataFrame[HistoriquePérimètre],
    fin: pd.Timestamp
) -> DataFrame[HistoriquePérimètre]:
    """
    Extrait uniquement les variations (changements contractuels) qui ont eu lieu dans une période donnée.

    Args:
        deb (pd.Timestamp): Début de la période.
        fin (pd.Timestamp): Fin de la période.
        historique (pd.DataFrame): Historique des événements contractuels.

    Returns:
        pd.DataFrame: Un sous-ensemble de l'historique contenant uniquement les variations dans la période.
    """
    return historique[
        (historique["Date_Evenement"] <= fin)
    ].sort_values(by="Date_Evenement", ascending=True)  # Trie par ordre chronologique

@pa.check_types
def extraire_période(
    deb: pd.Timestamp, fin: pd.Timestamp, 
    historique: DataFrame[HistoriquePérimètre]
) -> DataFrame[HistoriquePérimètre]:
    """
    Extrait uniquement les variations (changements contractuels) qui ont eu lieu dans une période donnée.

    Args:
        deb (pd.Timestamp): Début de la période.
        fin (pd.Timestamp): Fin de la période.
        historique (pd.DataFrame): Historique des événements contractuels.

    Returns:
        pd.DataFrame: Un sous-ensemble de l'historique contenant uniquement les variations dans la période.
    """
    return historique[
        (historique["Date_Evenement"] >= deb) & (historique["Date_Evenement"] <= fin)
    ].sort_values(by="Date_Evenement", ascending=True)  # Trie par ordre chronologique

@pa.check_types
def extraite_relevés_entrées(
    historique: DataFrame[HistoriquePérimètre]
) -> DataFrame[RelevéIndex]:
        _événements = ['MES', 'PMES', 'CFNE']
        _colonnes_meta_releve = ['Ref_Situation_Contractuelle', 'pdl', 'Unité', 'Précision', 'Source']
        _colonnes_relevé = ['Id_Calendrier_Distributeur', 'Date_Releve', 'Nature_Index', 'HP', 'HC', 'HCH', 'HPH', 'HPB', 'HCB', 'BASE']
        _colonnes_relevé_après = ['Après_'+c for c in _colonnes_relevé]
        return RelevéIndex.validate(
            historique[historique['Evenement_Declencheur'].isin(_événements)][_colonnes_meta_releve + _colonnes_relevé_après]
            .rename(columns={k: v for k,v in zip(_colonnes_relevé_après, _colonnes_relevé)})
            .dropna(subset=['Date_Releve'])
            )

@pa.check_types
def extraite_relevés_sorties(
    historique: DataFrame[HistoriquePérimètre]
) -> DataFrame[RelevéIndex]:
        _événements = ['RES', 'CFNS']
        _colonnes_meta_releve = ['Ref_Situation_Contractuelle', 'pdl', 'Unité', 'Précision', 'Source']
        _colonnes_relevé = ['Id_Calendrier_Distributeur', 'Date_Releve', 'Nature_Index', 'HP', 'HC', 'HCH', 'HPH', 'HPB', 'HCB', 'BASE']
        _colonnes_relevé_avant = ['Avant_'+c for c in _colonnes_relevé]
        return RelevéIndex.validate(
            historique[historique['Evenement_Declencheur'].isin(_événements)][_colonnes_meta_releve + _colonnes_relevé_avant]
            .rename(columns={k: v for k,v in zip(_colonnes_relevé_avant, _colonnes_relevé)})
            .dropna(subset=['Date_Releve'])
            )

@pa.check_types
def extraire_modifications_impactantes(
    deb: pd.Timestamp,
    historique: DataFrame[HistoriquePérimètre]
) -> DataFrame[ModificationContractuelleImpactante]:
    """
    Détecte les MCT dans une période donnée et renvoie les variations de Puissance_Souscrite
    et Formule_Tarifaire_Acheminement avant et après chaque MCT.

    Args:
        deb (pd.Timestamp): Début de la période.
        historique (pd.DataFrame): Historique des événements contractuels.

    Returns:
        DataFrame[ModificationContractuelleImpactante]: DataFrame contenant les MCT avec les valeurs avant/après.
    """

    # 🔍 Décaler les valeurs pour obtenir les données "avant" AVANT de filtrer
    historique = historique.sort_values(by=["Ref_Situation_Contractuelle", "Date_Evenement"])
    historique["Avant_Puissance_Souscrite"] = historique.groupby("Ref_Situation_Contractuelle")["Puissance_Souscrite"].shift(1)
    historique["Avant_Formule_Tarifaire_Acheminement"] = historique.groupby("Ref_Situation_Contractuelle")["Formule_Tarifaire_Acheminement"].shift(1)


    # 📌 Filtrer uniquement les MCT dans la période donnée
    impacts = (
          historique[
            (historique["Date_Evenement"] >= deb) &
            (historique["Evenement_Declencheur"] == "MCT")]
          .copy()
          .rename(columns={'Puissance_Souscrite': 'Après_Puissance_Souscrite', 'Formule_Tarifaire_Acheminement':'Après_Formule_Tarifaire_Acheminement'})
          .drop(columns=['Segment_Clientele', 'Num_Depannage', 'Categorie', 'Etat_Contractuel', 'Type_Compteur', 'Date_Derniere_Modification_FTA', 'Type_Evenement', 'Ref_Demandeur', 'Id_Affaire'])
    )
    
    # TODO: Prendre en compte plus de cas
    impacts['Impacte_energies'] = (
        impacts["Avant_Id_Calendrier_Distributeur"].notna() & 
        impacts["Après_Id_Calendrier_Distributeur"].notna() & 
        (impacts["Avant_Id_Calendrier_Distributeur"] != impacts["Après_Id_Calendrier_Distributeur"])
    )

    # ➕ Ajout de la colonne de lisibilité du changement
    def generer_resumé(row):
        modifications = []
        if row["Avant_Puissance_Souscrite"] != row["Après_Puissance_Souscrite"]:
            modifications.append(f"P: {row['Avant_Puissance_Souscrite']} → {row['Après_Puissance_Souscrite']}")
        if row["Avant_Formule_Tarifaire_Acheminement"] != row["Après_Formule_Tarifaire_Acheminement"]:
            modifications.append(f"FTA: {row['Avant_Formule_Tarifaire_Acheminement']} → {row['Après_Formule_Tarifaire_Acheminement']}")
        return ", ".join(modifications) if modifications else "Aucun changement"
    
    impacts["Résumé_Modification"] = impacts.apply(generer_resumé, axis=1)

    ordre_colonnes = ModificationContractuelleImpactante.to_schema().columns.keys()
    impacts = impacts[ordre_colonnes]
    
    return impacts

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

    impact_turpe_fixe = (
        (historique["Avant_Puissance_Souscrite"].notna() &
         (historique["Avant_Puissance_Souscrite"] != historique["Puissance_Souscrite"])) |
        (historique["Avant_Formule_Tarifaire_Acheminement"].notna() &
         (historique["Avant_Formule_Tarifaire_Acheminement"] != historique["Formule_Tarifaire_Acheminement"]))
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

    impact_energie = changement_calendrier | changement_index

    impact_turpe_variable = (
      (impact_energie) |
      (historique["Avant_Formule_Tarifaire_Acheminement"].notna() &
         (historique["Avant_Formule_Tarifaire_Acheminement"] != historique["Formule_Tarifaire_Acheminement"]))
    )

    historique["impact_turpe_fixe"] = impact_turpe_fixe
    historique["impact_energie"] = impact_energie
    historique["impact_turpe_variable"] = impact_turpe_variable

    # Forcer les impacts à True pour les événements d’entrée et de sortie
    evenements_entree_sortie = ["CFNE", "MES", "PMES", "CFNS", "RES"]
    mask_entree_sortie = historique["Evenement_Declencheur"].isin(evenements_entree_sortie)

    historique.loc[mask_entree_sortie, ["impact_turpe_fixe", "impact_energie", "impact_turpe_variable"]] = True

    def generer_resume(row):
        modifs = []
        if row["impact_turpe_fixe"]:
            if pd.notna(row.get("Avant_Puissance_Souscrite")) and row["Avant_Puissance_Souscrite"] != row["Puissance_Souscrite"]:
                modifs.append(f"P: {row['Avant_Puissance_Souscrite']} → {row['Puissance_Souscrite']}")
            if pd.notna(row.get("Avant_Formule_Tarifaire_Acheminement")) and row["Avant_Formule_Tarifaire_Acheminement"] != row["Formule_Tarifaire_Acheminement"]:
                modifs.append(f"FTA: {row['Avant_Formule_Tarifaire_Acheminement']} → {row['Formule_Tarifaire_Acheminement']}")
        if row["impact_energie"]:
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
    
    # 1A. Identifier les dates d'entrée de chaque PDL
    print("🔍 Détection des entrées...")
    entrees = historique[historique['Evenement_Declencheur'].isin(['CFNE', 'MES', 'PMES'])]
    debuts = entrees.groupby('Ref_Situation_Contractuelle')['Date_Evenement'].min()
    print(f"   - {len(entrees)} événements d'entrée pour {len(debuts)} PDL")
    
    # 1B. Identifier les dates de sortie de chaque PDL
    print("🔍 Détection des sorties...")
    sorties = historique[historique['Evenement_Declencheur'].isin(['RES', 'CFNS'])]
    fins = sorties.groupby('Ref_Situation_Contractuelle')['Date_Evenement'].max()
    print(f"   - {len(sorties)} événements de sortie pour {len(fins)} PDL")
    
    # 1C. Définir la date limite pour les PDL non résiliés
    # LOGIQUE : génère des événements jusqu'au début du mois courant inclus
    fin_par_defaut = pd.Timestamp.now(tz=tz).to_period("M").start_time.tz_localize(tz)
    print(f"   - Date limite pour PDL actifs : {fin_par_defaut}")
    
    # 1D. Construire le DataFrame des périodes individuelles
    periodes = pd.DataFrame({
        "start": debuts,
        "end": fins
    }).fillna(fin_par_defaut)
    
    print(f"📊 Périodes d'activité :")
    print(f"   - Total PDL : {len(periodes)}")
    print(f"   - PDL résiliés : {periodes['end'].notna().sum() - (periodes['end'] == fin_par_defaut).sum()}")
    print(f"   - PDL actifs : {(periodes['end'] == fin_par_defaut).sum()}")

    # 1E. Filtrer les PDL entrés après la date limite (pas d'événements pour ces PDL)
    # LOGIQUE : Un PDL entré après le 1er du mois courant ne génère pas d'événements pour ce mois
    periodes_valides = periodes[periodes["start"] <= periodes["end"]]
    periodes_invalides = periodes[periodes["start"] > periodes["end"]]
    
    if len(periodes_invalides) > 0:
        print(f"⚠️  PDL entrés trop tard (après {fin_par_defaut.strftime('%Y-%m-%d')}) : {len(periodes_invalides)}")
        for ref, row in periodes_invalides.iterrows():
            print(f"   - {ref}: entrée le {row['start'].strftime('%Y-%m-%d')} (exclu)")
    
    if len(periodes_valides) == 0:
        print("❌ Aucun PDL n'a de période valide pour générer des événements")
        return historique  # Retourner l'historique original sans modification
    
    print(f"   - PDL avec périodes valides : {len(periodes_valides)}")

    # =============================================================================
    # ÉTAPE 2 : GÉNÉRATION DES DATES MENSUELLES (GLOBAL)
    # =============================================================================
    
    # 2A. Calculer la plage globale de dates (uniquement pour les PDL valides)
    min_date = periodes_valides["start"].min()
    max_date = periodes_valides["end"].max()
    print(f"🗓️  Plage globale : {min_date} → {max_date}")
    
    # 2B. Générer tous les 1ers du mois dans cette plage (inclus)
    # ASTUCE : ajouter 1 jour à max_date pour être sûr d'inclure le mois de fin
    max_date_inclusive = max_date + pd.DateOffset(days=1)
    all_months = pd.date_range(start=min_date, end=max_date_inclusive, freq="MS", tz=tz)
    print(f"   - {len(all_months)} mois générés de {all_months[0].strftime('%Y-%m')} à {all_months[-1].strftime('%Y-%m')}")
    
    # =============================================================================
    # ÉTAPE 3 : ASSOCIATION PDL ↔ MOIS (INDIVIDUALISÉ PAR PDL)
    # =============================================================================
    
    # 3A. Faire un produit cartésien : chaque PDL valide × chaque mois
    print("🔗 Association PDL ↔ mois...")
    ref_mois = (
        periodes_valides.reset_index()
        .merge(pd.DataFrame({"Date_Evenement": all_months}), how="cross")
    )
    
    # 3A bis. Ajouter le mapping Ref_Situation_Contractuelle → pdl depuis l'historique
    mapping_pdl = historique[['Ref_Situation_Contractuelle', 'pdl']].drop_duplicates()
    ref_mois = ref_mois.merge(mapping_pdl, on='Ref_Situation_Contractuelle', how='left')
    
    print(f"   - {len(ref_mois)} combinaisons PDL×mois avant filtrage")
    
    # 3B. Filtrer pour ne garder que les mois où chaque PDL est actif
    # CORRECTION : comparer les dates, pas les timestamps avec heures
    # Car les événements FACTURATION sont générés avec freq="MS" (début de mois avec heure)
    # alors que fin_par_defaut est à 00:00:00
    # IMPORTANT : pour éviter les conflits avec ffill, exclure le mois d'entrée
    ref_mois = ref_mois[
        (ref_mois["Date_Evenement"].dt.date > ref_mois["start"].dt.date) & 
        (ref_mois["Date_Evenement"].dt.date <= ref_mois["end"].dt.date)
    ]
    print(f"   - {len(ref_mois)} combinaisons PDL×mois après filtrage")
    
    # 3C. Statistiques de répartition
    events_par_pdl = ref_mois.groupby('Ref_Situation_Contractuelle').size()
    print(f"   - Moyenne : {events_par_pdl.mean():.1f} événements/PDL")
    print(f"   - Min/Max : {events_par_pdl.min()}/{events_par_pdl.max()} événements/PDL")

    # =============================================================================
    # ÉTAPE 4 : CRÉATION DES ÉVÉNEMENTS ARTIFICIELS (GLOBAL)
    # =============================================================================
    
    # 4A. Créer les événements de facturation
    print("📅 Création des événements artificiels...")
    evenements = ref_mois.copy()
    evenements["Evenement_Declencheur"] = "FACTURATION"
    evenements["Type_Evenement"] = "artificiel"
    evenements["Source"] = "synthese_mensuelle"
    evenements["resume_modification"] = "Facturation mensuelle"
    evenements["impact_turpe_fixe"] = True
    evenements["impact_energie"] = True
    evenements["impact_turpe_variable"] = True

    # 4B. Sélectionner les colonnes nécessaires (inclure pdl pour éviter les NaN)
    evenements = evenements[[
        "Ref_Situation_Contractuelle", "pdl", "Date_Evenement",
        "Evenement_Declencheur", "Type_Evenement", "Source", "resume_modification",
        "impact_turpe_fixe", "impact_energie", "impact_turpe_variable"
    ]]
    print(f"   - {len(evenements)} événements de facturation créés")

    # =============================================================================
    # ÉTAPE 5 : PROPAGATION DES DONNÉES CONTRACTUELLES (INDIVIDUALISÉ PAR PDL)
    # =============================================================================
    
    # 5A. Fusionner historique original + événements artificiels
    print("🔄 Propagation des données contractuelles...")
    fusion = pd.concat([historique, evenements], ignore_index=True).sort_values(
        ["Ref_Situation_Contractuelle", "Date_Evenement"]
    ).reset_index(drop=True)
    
    # 5B. Identifier les colonnes à propager (non-nullables du modèle Pandera)
    # Ensure the schema is populated to access field information
    schema = HistoriquePérimètre.to_schema()
    colonnes_ffill = [
        name for name, col_info in schema.columns.items()
        if name in fusion.columns and not col_info.nullable
    ]
    print(f"   - {len(colonnes_ffill)} colonnes à propager : {colonnes_ffill}")

    # 5C. Propager les données par PDL avec forward fill
    # LOGIQUE : chaque événement artificiel hérite des caractéristiques du dernier événement réel
    fusion[colonnes_ffill] = (
        fusion.groupby("Ref_Situation_Contractuelle")[colonnes_ffill]
        .ffill()
    )

    # =============================================================================
    # ÉTAPE 6 : EXTRACTION ET ASSEMBLAGE FINAL (GLOBAL)
    # =============================================================================
    
    # 6A. Extraire uniquement les événements FACTURATION avec données propagées
    ajout = fusion[fusion["Evenement_Declencheur"] == "FACTURATION"]
    print(f"   - {len(ajout)} événements FACTURATION avec données propagées")

    # 6B. Assemblage final : historique original + événements artificiels
    historique_etendu = pd.concat([historique, ajout], ignore_index=True).sort_values(
        ["Ref_Situation_Contractuelle", "Date_Evenement"]
    ).reset_index(drop=True)
    
    print(f"✅ Historique étendu : {len(historique_etendu)} événements total")
    print(f"   - Événements originaux : {len(historique)}")
    print(f"   - Événements artificiels : {len(ajout)}")
    
    return historique_etendu

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
        pd.DataFrame: Relevés d’index conformes au modèle RelevéIndex.
    """
    index_cols = ["BASE", "HP", "HC", "HCH", "HPH", "HPB", "HCB", "Id_Calendrier_Distributeur"]
    identifiants = ["pdl", "Ref_Situation_Contractuelle", "Formule_Tarifaire_Acheminement"]

    # Créer relevés "avant"
    avant = historique[identifiants + ["Date_Evenement"] + [f"Avant_{col}" for col in index_cols]].copy()
    avant = avant.rename(columns={f"Avant_{col}": col for col in index_cols})
    avant["ordre_index"] = 0

    # Créer relevés "après"
    apres = historique[identifiants + ["Date_Evenement"] + [f"Après_{col}" for col in index_cols]].copy()
    apres = apres.rename(columns={f"Après_{col}": col for col in index_cols})
    apres["ordre_index"] = 1

    # Concaténer
    resultats = pd.concat([avant, apres], ignore_index=True)
    resultats = resultats.dropna(subset=index_cols, how="all")
    resultats["Source"] = "flux_C15"
    resultats["Unité"] = "kWh" 
    resultats["Précision"] = "kWh"

    resultats = resultats.rename(columns={"Date_Evenement": "Date_Releve"})

    # Réordonner selon modèle RelevéIndex (on ne garde que les colonnes présentes)
    colonnes_finales = [col for col in RelevéIndex.to_schema().columns.keys() if col in resultats.columns]
    resultats = resultats[colonnes_finales]

    return resultats


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