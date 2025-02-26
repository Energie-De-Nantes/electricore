import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

from electricore.core.périmètre.modèles import HistoriquePérimètre, SituationPérimètre, VariationsMCT

@pa.check_types
def filtrer_evenements_periode(
    historique: DataFrame[HistoriquePérimètre], deb: pd.Timestamp, fin: pd.Timestamp, evenements: list
) -> DataFrame[HistoriquePérimètre]:
    """
    Filtre l'historique pour ne conserver que les événements spécifiques dans la période donnée.
    """
    return historique[
        (historique['Date_Evenement'] >= deb) &
        (historique['Date_Evenement'] <= fin) &
        (historique['Evenement_Declencheur'].isin(evenements))
    ].copy()

@pa.check_types
def extraire_colonnes_periode(
    historique: DataFrame[HistoriquePérimètre], 
    deb: pd.Timestamp, 
    fin: pd.Timestamp, 
    evenements: list, 
    colonnes: list, 
    # suffixe: str
) -> DataFrame:
    """
    Extrait des colonnes spécifiques pour les événements donnés dans une période donnée.

    Args:
        historique (DataFrame[HistoriquePérimètre]): Historique des événements contractuels.
        deb (pd.Timestamp): Début de la période.
        fin (pd.Timestamp): Fin de la période.
        evenements (list): Liste des événements à extraire.
        colonnes (list): Colonnes à extraire.
        suffixe (str): Suffixe à ajouter aux colonnes ("_deb" ou "_fin").

    Returns:
        pd.DataFrame: DataFrame filtré, indexé et avec colonnes suffixées.
    """
    # Filtrer les événements dans la période donnée
    filtrés = filtrer_evenements_periode(historique=historique, deb=deb, fin=fin, evenements=evenements)
    
    if filtrés.empty:
        return pd.DataFrame(columns=["Ref_Situation_Contractuelle", "pdl"] + colonnes)

    return filtrés[["pdl"] +colonnes].copy()
    # Extraction des colonnes et ajout d'un suffixe
    extrait = (
        filtrés
        .copy()
        # .assign(source=lambda df: "Périmètre_" + df['Evenement_Declencheur'].astype(str))
        .set_index("Ref_Situation_Contractuelle")[["pdl"] +colonnes]
        # .add_suffix(suffixe)
        
    )
    
    return extrait

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
def variations_dans_periode(
    deb: pd.Timestamp, fin: pd.Timestamp, historique: DataFrame[HistoriquePérimètre]
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
def variations_mct_dans_periode(
    deb: pd.Timestamp, fin: pd.Timestamp, historique: DataFrame[HistoriquePérimètre]
) -> DataFrame[VariationsMCT]:
    """
    Détecte les MCT dans une période donnée et renvoie les variations de Puissance_Souscrite
    et Formule_Tarifaire_Acheminement avant et après chaque MCT.

    Args:
        deb (pd.Timestamp): Début de la période.
        fin (pd.Timestamp): Fin de la période.
        historique (pd.DataFrame): Historique des événements contractuels.

    Returns:
        pd.DataFrame: Une DataFrame contenant les MCT et leurs variations.
    """

    # Filtrer uniquement les MCT dans la période donnée
    mct_events = historique[
        (historique["Date_Evenement"] >= deb) & (historique["Date_Evenement"] <= fin) &
        (historique["Evenement_Declencheur"] == "MCT")
    ].sort_values(by=["Ref_Situation_Contractuelle", "Date_Evenement"])

    # Liste des résultats
    results = []

    for _, mct_row in mct_events.iterrows():
        ref_situation = mct_row["Ref_Situation_Contractuelle"]
        date_mct = mct_row["Date_Evenement"]

        # Trouver la ligne juste avant avec le même Ref_Situation_Contractuelle
        previous_event = historique[
            (historique["Ref_Situation_Contractuelle"] == ref_situation) &
            (historique["Date_Evenement"] < date_mct)
        ].sort_values(by="Date_Evenement", ascending=False).head(1)

        if not previous_event.empty:
            results.append({
                "Date_MCT": date_mct,
                "Puissance_Souscrite_Avant": previous_event.iloc[0]["Puissance_Souscrite"],
                "Puissance_Souscrite_Après": mct_row["Puissance_Souscrite"],
                "Formule_Tarifaire_Acheminement_Avant": previous_event.iloc[0]["Formule_Tarifaire_Acheminement"],
                "Formule_Tarifaire_Acheminement_Après": mct_row["Formule_Tarifaire_Acheminement"],
            })

    # Convertir en DataFrame
    return pd.DataFrame(results)