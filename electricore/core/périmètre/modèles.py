import pandas as pd
import pandera.pandas as pa
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
    Categorie: Series[str] = pa.Field(nullable=True)

    # Infos calculs tarifs
    Puissance_Souscrite: Series[float] = pa.Field(nullable=False, coerce=True)
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

class ModificationContractuelleImpactante(pa.DataFrameModel):
    """
    📌 Modèle Pandera pour la sortie de `variations_mct_dans_periode`.
    
    Contient les variations de puissance et de tarif après un MCT. (et ptet d'autres, à voir)
    """
    Ref_Situation_Contractuelle: Series[str] = pa.Field(nullable=False)
    pdl: Series[str] = pa.Field(nullable=False)
    Impacte_energies : Series[bool] = pa.Field(nullable=False)
    Résumé_Modification: Series[str] = pa.Field(nullable=False)

    Date_Evenement: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)

    Avant_Puissance_Souscrite: Series[float] = pa.Field(nullable=False, coerce=True)
    Après_Puissance_Souscrite: Series[float] = pa.Field(nullable=False, coerce=True)

    Avant_Formule_Tarifaire_Acheminement: Series[str] = pa.Field(nullable=False)
    Après_Formule_Tarifaire_Acheminement: Series[str] = pa.Field(nullable=False)

    # +relevés avant et Après
