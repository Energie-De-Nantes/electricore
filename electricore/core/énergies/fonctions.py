import pandera.pandas as pa
import pandas as pd
import numpy as np

from pandera.typing import DataFrame
from electricore.core.périmètre import (
    HistoriquePérimètre, SituationPérimètre, ModificationContractuelleImpactante,
    extraire_situation, extraire_période,
    extraite_relevés_entrées, extraite_relevés_sorties
)
from electricore.core.relevés import RelevéIndex, interroger_relevés
from electricore.core.énergies.modèles import BaseCalculEnergies

from icecream import ic

def préparer_base_énergies(
    historique: DataFrame[HistoriquePérimètre], deb: pd.Timestamp, fin: pd.Timestamp
) -> DataFrame[BaseCalculEnergies]:
    """
    🏗️ Prépare la base des énergies en identifiant les entrées, sorties et MCT dans la période.

    Args:
        historique (DataFrame[HistoriquePérimètre]): Historique des situations contractuelles.
        deb (pd.Timestamp): Début de la période de calcul des énergies.
        fin (pd.Timestamp): Fin de la période de calcul des énergies.

    Returns:
        DataFrame[SituationPérimètre]: Situation contractuelle enrichie pour le calcul des énergies.
    """
    colonnes_meta_releve = ['Unité', 'Précision', 'Source']
    colonnes_releve = ['Id_Calendrier_Distributeur', 'Date_Releve', 'Nature_Index', 'HP', 'HC', 'HCH', 'HPH', 'HPB', 'HCB', 'BASE']



    # 1) On récupére la situation du périmètre telle qu'elle était à la date de fin
    situation = extraire_situation(fin, historique)

    # 2) On filtre pour n'avoir que les PDLs en service, ou dont le service c'est terminé dans la période.
    # (car pour les autres, aka terminés avant la période, il n'y a rien a calculer pour la période)
    _masque = (situation['Etat_Contractuel'] == 'EN SERVICE') | (
        (situation['Etat_Contractuel'] == 'RESILIE') & (situation['Date_Evenement'] >= deb)
    )
    # Ajouter ici des colonnes supp si besoin de l'info plus loin
    colonnes_évenement = ['Ref_Situation_Contractuelle', 
                          'pdl', 
                          'Formule_Tarifaire_Acheminement', 
                          'Puissance_Souscrite',
                          'Type_Compteur', 'Num_Compteur', 'Num_Depannage']
    base = (
        situation[_masque]
        .drop(columns=[col for col in situation if col not in colonnes_évenement])
        .sort_values(by="Ref_Situation_Contractuelle")
        .copy()
    )

    # 3) On interroge le périmètre sur les éventuelles entrées et sorties, et on récupére les relevés d'index associés.
    période: DataFrame[HistoriquePérimètre] = extraire_période(deb, fin, historique)

    entrées: DataFrame[RelevéIndex] = (
        extraite_relevés_entrées(période)
        .set_index('Ref_Situation_Contractuelle')
        .drop(columns=['pdl'])
        .add_suffix('_deb')
        .assign(Entree=True)
    )
    sorties: DataFrame[RelevéIndex] = (
        extraite_relevés_sorties(période)
        .set_index('Ref_Situation_Contractuelle')
        .drop(columns=['pdl'])
        .add_suffix('_fin')
        .assign(Sortie=True)
    )

    # On les fusionne dans la base
    base = (
        base
        .merge(entrées, how='left', left_on='Ref_Situation_Contractuelle', right_index=True)
        .merge(sorties, how='left', left_on='Ref_Situation_Contractuelle', right_index=True)
        .fillna({'Entree': False, 'Sortie': False})
        .infer_objects(copy=False)  # Explicitly infer proper dtypes after fillna
    )

    return base

# @pa.check_types
def découper_périodes(
    base_énergies: DataFrame[BaseCalculEnergies],
    modifications: DataFrame[ModificationContractuelleImpactante]
) -> DataFrame[BaseCalculEnergies]:
    """
    📌 Découpe la base de calcul d'énergies en sous-périodes calculables.

    Cette fonction segmente les périodes impactées par des modifications contractuelles (MCT)
    en sous-périodes homogènes, prêtes pour les calculs d’énergies.

    - Ajoute des points de découpage à chaque MCT.
    - Génère des périodes couvrantes et calculables avec les valeurs mises à jour.

    🚀 Résultat : Des périodes propres et exploitables pour le calcul des énergies.
    """

    # 1️⃣ **Séparer les périodes impactées et non impactées**
    impactées = base_énergies[
        base_énergies["Ref_Situation_Contractuelle"].isin(modifications["Ref_Situation_Contractuelle"])
    ]
    non_impactées = base_énergies[
        ~base_énergies["Ref_Situation_Contractuelle"].isin(modifications["Ref_Situation_Contractuelle"])
    ]

    # 2️⃣ **Générer les sous-périodes pour les lignes impactées**
    all_periods = []

    for ref_situation, modifs in modifications.groupby("Ref_Situation_Contractuelle"):
        # Trier les modifications chronologiquement
        modifs = modifs.sort_values(by="Date_Evenement")

        # Récupérer la ligne initiale
        base_ligne = impactées[impactées["Ref_Situation_Contractuelle"] == ref_situation].iloc[0].copy()

        # Initialisation des dates de découpage
        dates_coupure = [base_ligne["Date_Releve_deb"]] + \
                        modifs["Date_Evenement"].tolist() + \
                        [base_ligne["Date_Releve_fin"]]
        dates_coupure = sorted(set(dates_coupure))

        # 3️⃣ **Créer une ligne par sous-période**
        for i in range(len(dates_coupure) - 1):
            periode = base_ligne.copy()
            periode["Date_Début"] = dates_coupure[i]
            periode["Date_Fin"] = dates_coupure[i + 1]

            # Appliquer la modification contractuelle si elle intervient à cette date
            modif_courante = modifs[modifs["Date_Evenement"] == dates_coupure[i]]
            if not modif_courante.empty:
                modif_courante = modif_courante.iloc[0]
                periode["Puissance_Souscrite"] = modif_courante["Avant_Puissance_Souscrite"]
                periode["Formule_Tarifaire_Acheminement"] = modif_courante["Avant_Formule_Tarifaire_Acheminement"]

            all_periods.append(periode)
    return all_periods

    # 4️⃣ **Concaténer les périodes impactées + les non impactées**
    base_decoupée = pd.concat([non_impactées] + all_periods, ignore_index=True)

    return base_decoupée

def ajouter_relevés(
    base: DataFrame[BaseCalculEnergies], 
    relevés: DataFrame[RelevéIndex],
    suffixe: str = "_deb"  # Valeur par défaut "_deb", peut être "_fin"
) -> DataFrame[BaseCalculEnergies]:
    """
    🔄 Ajoute les relevés manquants dans la base de calcul des énergies.

    Args:
        base (DataFrame[BaseCalculEnergies]): Base existante des calculs d'énergie.
        relevés (DataFrame[RelevéIndex]): Relevés d'index disponibles.
        suffixe (str, optional): Suffixe qui identifie s'il s'agit de relevés de début ("_deb") 
                                ou de fin ("_fin"). Par défaut "_deb".

    Returns:
        DataFrame[BaseCalculEnergies]: Base mise à jour avec les relevés ajoutés.
    """
    # Dynamiquement construire les noms de colonnes basés sur le suffixe
    col_date_releve = f"Date_Releve{suffixe}"
    col_source = f"Source{suffixe}"
    
    # 🏷️ Extraire les paires (Date_Releve, pdl) manquantes dans la base
    requêtes_manquantes = (
        base
        .loc[base[col_source].isna(), [col_date_releve, "pdl"]]
        .rename(columns={col_date_releve: 'Date_Releve'})
        .drop_duplicates()
    )
    if requêtes_manquantes.empty:
        return base  # ✅ Rien à ajouter, on retourne la base inchangée.
    
    # 🔍 Récupération des relevés manquants
    relevés_trouvés = (
        interroger_relevés(requêtes_manquantes, relevés)
        .add_suffix(suffixe)
        .rename(columns={f'pdl{suffixe}': 'pdl'})
    )
    
    # Préparation pour la mise à jour
    base_mise_a_jour = base.copy()
    
    # Mise à jour
    base_mise_a_jour.update(relevés_trouvés)

    return base_mise_a_jour

def calculer_energies(
    base: DataFrame[BaseCalculEnergies],
    inclure_jour_fin: bool=False
) -> DataFrame[BaseCalculEnergies]:
    """
    ⚡ Calcule les énergies consommées en faisant la différence entre les index de fin et de début
    pour les lignes où les calendriers de distribution sont identiques.

    Args:
        base (DataFrame[BaseCalculEnergies]): Base contenant les relevés de début et de fin.

    Returns:
        DataFrame[BaseCalculEnergies]: Base avec les énergies calculées.
    """
    # Liste des cadrans d'index à traiter
    cadrans = ['HPH', 'HPB', 'HCH', 'HCB', 'HP', 'HC', 'BASE']
    
    # Copie de la base pour ne pas modifier l'original
    resultat = base.copy()
    
    # Vérification de l'égalité des calendriers distributeur
    calendriers_identiques = (
        resultat["Id_Calendrier_Distributeur_deb"] == 
        resultat["Id_Calendrier_Distributeur_fin"]
    )
    
    # On ne calcule les énergies que pour les lignes où les calendriers sont identiques
    lignes_valides = resultat[calendriers_identiques].index
    
    if len(lignes_valides) == 0:
        print("⚠️ Aucune ligne avec des calendriers identiques trouvée.")
        return resultat
    
    # Pour chaque cadran, calculer l'énergie consommée
    for cadran in cadrans:
        col_deb = f"{cadran}_deb"
        col_fin = f"{cadran}_fin"
        col_energie = cadran
        
        # Calculer l'énergie comme la différence entre l'index de fin et de début
        # On arrondit à l'entier inférieur pour éviter les problèmes de précision différentes entre les relevés,
        resultat.loc[lignes_valides, col_energie] = (
            np.floor(resultat.loc[lignes_valides, col_fin]) - 
            np.floor(resultat.loc[lignes_valides, col_deb])
        )
        
        # Vérifier les valeurs négatives (anomalies potentielles)
        nb_negatifs = (resultat.loc[lignes_valides, col_energie] < 0).sum()
        if nb_negatifs > 0:
            print(f"⚠️ {nb_negatifs} valeurs négatives détectées pour {col_energie}")
    
    # Ajouter une colonne pour indiquer si l'énergie a été calculée
    resultat["Energie_Calculee"] = False
    resultat.loc[lignes_valides, "Energie_Calculee"] = True
    
    # Calculer la somme totale des énergies (tous cadrans confondus)
        # Calcul du nombre de jours entre les deux relevés
    resultat['j'] = (
        resultat["Date_Releve_fin"].dt.date - resultat["Date_Releve_deb"].dt.date
    ).apply(lambda x: x.days + (1 if inclure_jour_fin else 0))

    # Calculer HP et HC en prenant la somme des colonnes correspondantes
    resultat['HP'] = resultat[['HPH', 'HPB', 'HP']].sum(axis=1, min_count=1)
    resultat['HC'] = resultat[['HCH', 'HCB', 'HC']].sum(axis=1, min_count=1)

    # Calculer BASE uniquement là où BASE est NaN
    resultat.loc[resultat['BASE'].isna(), 'BASE'] = resultat[['HP', 'HC']].sum(axis=1, min_count=1)
    
    return resultat