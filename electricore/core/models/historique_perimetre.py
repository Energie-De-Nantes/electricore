import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series, DataFrame
from typing import Annotated

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