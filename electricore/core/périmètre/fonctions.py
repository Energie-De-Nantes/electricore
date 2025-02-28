import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

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
    impacts = historique[
        (historique["Date_Evenement"] <= deb) &
        (historique["Evenement_Declencheur"] == "MCT")
    ].copy().rename(columns={'Puissance_Souscrite': 'Après_Puissance_Souscrite', 'Formule_Tarifaire_Acheminement':'Après_Formule_Tarifaire_Acheminement'})

    return impacts