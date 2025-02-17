import pandas as pd
import pandera as pa
from pandera.typing import Series, DataFrame
from typing import Annotated
"""
| Formulaire SGE / Fiche catalogue / Intervention                                                  | Type_Evenement | Nature_Evenement | Description formulaire / fiche                                                                                           |
| ------------------------------------------------------------------------------------------------ | --------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------ |
| F100B                                                                                            | CONTRAT         | PMES              | Première mise en service                                                                                                 |
| F120B                                                                                            | CONTRAT         | MES               | Mise en service sur installation existante                                                                               |
| F130                                                                                             | CONTRAT         | CFNS              | Changement de Fournisseur                                                                                                |
| F130                                                                                             | CONTRAT         | CFNE              | Changement de Fournisseur                                                                                                |
| F140B                                                                                            | CONTRAT         | RES               | Résiliation                                                                                                              |
| F140 (RIF)                                                                                       | CONTRAT         | RES               | Résiliation initiative fournisseur                                                                                       |
| F180                                                                                             | CONTRAT         | MCT               | Modification de formule tarifaire ou de puissance d’acheminement                                                         |
| F1020                                                                                            | CONTRAT         | MES               | Mise en service ou rétablissement dans la journée (BT≤36 kVA)                                                            |
| M007                                                                                             | CONTRAT         | AUTRE             | Modification d’information Client                                                                                        |
| F180                                                                                             | TECHNIQUE       | MCF               | Modification de la programmation du calendrier fournisseur                                                               |
| F185                                                                                             | TECHNIQUE       | CMAT              | Modification du dispositif de comptage sans impact sur la formule tarifaire d’acheminement ou sur la puissance souscrite |
| F200B                                                                                            | TECHNIQUE       | COU               | Interventions pour impayé et rétablissement                                                                              |
| F200B                                                                                            | TECHNIQUE       | RET               | Interventions pour impayé et rétablissement                                                                              |
| F1020                                                                                            | TECHNIQUE       | RET               | Mise en service ou rétablissement dans la journée (BT≤36 kVA)                                                            |
| Opération de maintenance sur Compteur ou Disjoncteur                                             | TECHNIQUE       | CMAT              | N/A                                                                                                                      |
| F800B                                                                                            | CONTRAT         | MES               | Mise en service d’un raccordement provisoire pour une durée > 28j                                                        |
| F800B                                                                                            | CONTRAT         | RES               | Résiliation d’un raccordement provisoire pour une durée > 28j                                                            |
| F820                                                                                             | CONTRAT         | MES               | Mise en service d’un raccordement provisoire pour une durée <= 28j                                                       |
| F820                                                                                             | CONTRAT         | RES               | Résiliation d’un raccordement provisoire pour une durée <= 28j                                                           |
| F825                                                                                             | CONTRAT         | MES               | Mise en service d’un raccordement provisoire pour une durée <= 28j (saisie par le Distributeur)                          |
| F825                                                                                             | CONTRAT         | RES               | Résiliation d’un raccordement provisoire pour une durée <= 28j (saisie par le Distributeur)                              |
| F840B                                                                                            | TECHNIQUE       | MDBRA             | Raccordement BT < 36kVA                                                                                                  |
| F860                                                                                             | TECHNIQUE       | CMAT              | Déplacement de comptage / Déplacement ou modification de raccordement                                                    |
| F880                                                                                             | TECHNIQUE       | MDBRA             | Suppression de raccordement                                                                                              |
| F940B                                                                                            | TECHNIQUE       | AUTRE             | Intervention de courte durée                                                                                             |
| M007                                                                                             | CONTRAT         | MDACT             | Modification d’information Client                                                                                        |
| Prestation de visite de contrôle de branchement provisoire (Prolongation Branchement Provisoire) | TECHNIQUE       | AUTRE             | NA                                                                                                                       |
"""

class HistoriquePérimètre(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour l'historique des événements contractuels.
    
    Contient toutes les modifications de périmètre au fil du temps.
    """

    # Timestamp
    Date_Evenement: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)

    # Couple d'identifiants
    pdl: Series[str] = pa.Field(nullable=False)
    Ref_Situation_Contractuelle: Series[str] = pa.Field(nullable=False)
    
    # Infos Contractuelles
    Segment_Clientele: Series[str] = pa.Field(nullable=False)
    Etat_Contractuel: Series[str] = pa.Field(nullable=False) # "EN SERVICE", "RESILIE", etc.
    Evenement_Declencheur: Series[str] = pa.Field(nullable=False)  # Ex: "MCT", "MES", "RES"
    Type_Evenement: Series[str] = pa.Field(nullable=False)
    Categorie: Series[str] = pa.Field(nullable=False)

    # Infos calculs tarifs
    Puissance_Souscrite: Series[int] = pa.Field(nullable=False, coerce=True)
    Formule_Tarifaire_Acheminement: Series[str] = pa.Field(nullable=False,)

    # Infos Compteur
    Type_Compteur: Series[str] = pa.Field(nullable=False)
    Num_Compteur: Series[str] = pa.Field(nullable=False)

    # Infos Demande (Optionnel)
    Ref_Demandeur: Series[str] = pa.Field(nullable=True)
    Id_Affaire: Series[str] = pa.Field(nullable=True)

    # Relève ? On ajoute là on on fait un modèle de relève à part ?

class SituationPérimètre(HistoriquePérimètre):
    """
    📌 Modèle Pandera pour la situation à une date donnée.
    
    Générée à partir de l'historique pour donner un état du périmètre à un instant `t`.
    Chaque `Ref_Situation_Contractuelle` doit être unique.
    """

    @pa.check("Ref_Situation_Contractuelle")
    def unique_ref(cls, series: Series[str]) -> bool:
        """Vérifie que chaque Ref_Situation_Contractuelle est unique dans la situation."""
        return series.is_unique

class VariationsMCT(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour la sortie de `variations_mct_dans_periode`.
    
    Contient les variations de puissance et de tarif après un MCT.
    """

    Date_MCT: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)

    Puissance_Souscrite_Avant: Series[int] = pa.Field(nullable=False, coerce=True)
    Puissance_Souscrite_Après: Series[int] = pa.Field(nullable=False, coerce=True)

    Formule_Tarifaire_Acheminement_Avant: Series[str] = pa.Field(nullable=False)
    Formule_Tarifaire_Acheminement_Après: Series[str] = pa.Field(nullable=False)

# 📌 Fonction pour extraire la situation à une date donnée
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

# 📌 Main pour tester en dev
if __name__ == "__main__":
    print("🚀 Test de génération de la situation du périmètre")

    # Historique réaliste avec des MCT bien placés
    historique = pd.DataFrame({
        "pdl": [
            "12345", "12345", "12345",  # PDL 12345
            "67890", "67890", "67890",  # PDL 67890
            "11111", "11111"  # PDL 11111
        ],
        "Ref_Situation_Contractuelle": [
            "A1", "A1", "A1",  # Même Ref pour PDL 12345
            "B1", "B1", "B1",  # Même Ref pour PDL 67890
            "C1", "C1"  # Même Ref pour PDL 11111
        ],
        "Date_Evenement": pd.to_datetime([
            "2024-01-01", "2024-02-10", "2024-03-05",  # PDL 12345
            "2024-01-15", "2024-02-20", "2024-03-10",  # PDL 67890
            "2024-02-01", "2024-03-12"  # PDL 11111
        ]).tz_localize("Europe/Paris"),
        "Etat_Contractuel": [
            "EN SERVICE", "EN SERVICE", "RESILIE",  # PDL 12345
            "EN SERVICE", "EN SERVICE", "RESILIE",  # PDL 67890
            "EN SERVICE", "EN SERVICE"  # PDL 11111
        ],
        "Evenement_Declencheur": [
            "MES", "MCT", "RES",  # PDL 12345 (MCT entre MES et RES)
            "MES", "MCT", "RES",  # PDL 67890 (MCT entre MES et RES)
            "MES", "MCT"  # PDL 11111 (MCT en période active)
        ],
        "Type_Evenement": [
            "CONTRAT", "CONTRAT", "CONTRAT",  # PDL 12345
            "CONTRAT", "CONTRAT", "CONTRAT",  # PDL 67890
            "CONTRAT", "CONTRAT"  # PDL 11111
        ],
        "Segment_Clientele": [
            "RES", "RES", "RES",  # PDL 12345
            "RES", "RES", "RES",  # PDL 67890
            "PRO", "PRO"  # PDL 11111
        ],
        "Categorie": [
            "C2", "C2", "C2",  # PDL 12345
            "C2", "C2", "C2",  # PDL 67890
            "C5", "C5"  # PDL 11111
        ],
        "Type_Compteur": [
            "CCB", "CCB", "CCB",  # PDL 12345
            "CCB", "CCB", "CCB",  # PDL 67890
            "CCB", "CCB"  # PDL 11111
        ],
        "Num_Compteur": [
            "C1", "C1", "C1",  # PDL 12345
            "C2", "C2", "C2",  # PDL 67890
            "C3", "C3"  # PDL 11111
        ],
        "Puissance_Souscrite": [
            6, 9, 9,  # Changement à 9 kVA après MCT pour PDL 12345
            12, 15, 15,  # Changement à 15 kVA après MCT pour PDL 67890
            3, 6  # Changement à 6 kVA après MCT pour PDL 11111
        ],
        "Formule_Tarifaire_Acheminement": [
            "BTINFCU4", "BTINFMU4", "BTINFMU4",  # PDL 12345
            "BTINFCUST", "BTINFCU4", "BTINFCU4",  # PDL 67890
            "BTINFCU4", "BTINFMU4"  # PDL 11111
        ],
        "Ref_Demandeur": [
            None, "Dem1", None,  # PDL 12345
            None, "Dem2", None,  # PDL 67890
            None, "Dem3"  # PDL 11111
        ],
        "Id_Affaire": [
            None, "Aff1", None,  # PDL 12345
            None, "Aff2", None,  # PDL 67890
            None, "Aff3"  # PDL 11111
        ]
    })


    # Définir la date de référence
    date_reference = pd.Timestamp("2024-02-15", tz="Europe/Paris")

    # Extraire la situation
    situation = extraire_situation(date_reference, historique)

    # Afficher le résultat
    print("\n📊 Situation du périmètre au", date_reference)
    print(situation)

    # Définir la période d'analyse
    deb = pd.Timestamp("2024-01-01", tz="Europe/Paris")
    fin = pd.Timestamp("2024-03-15", tz="Europe/Paris")

    # Extraire les variations MCT
    variations_mct = variations_mct_dans_periode(deb, fin, historique)

    # Afficher les résultats
    print("\n🔄 Variations MCT dans la période du", deb, "au", fin)
    print(variations_mct)