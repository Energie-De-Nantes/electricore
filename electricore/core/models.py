import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Series
from typing import Annotated

class BaseFacturationModèle(pa.DataFrameModel):
    """
    Modèle Pandera pour la base de facturation.

    Ce modèle définit la structure des données nécessaires pour le calcul des énergies 
    et la facturation des points de livraison (PDL). Il regroupe les informations 
    contractuelles, les caractéristiques du compteur, ainsi que les relevés de début 
    et de fin de période.

    ## 📌 **Colonnes**
    
    ### 🔹 **Données contractuelles**
    - `pdl` (str) : Identifiant unique du Point De Livraison (PDL).
    - `Ref_Situation_Contractuelle` (str) : Référence unique attribuée par enedis à un rattachement de pdl à un fournisseur. 
    - `Segment_Clientele` (str) : Type de clientèle (ex. "Résidentiel", "Professionnel").
    - `Categorie` (str) : Catégorie C2 à C5.
    - `Puissance_Souscrite` (int) : Puissance souscrite en kVA. Nécessaire pour calculer le TURPE.
    - `Formule_Tarifaire_Acheminement` (str) : Formule tarifaire appliquée à l'acheminement. Nécessaire pour calculer le TURPE.

    ### 🔹 **Caractéristiques du compteur**

    Ces données doivent légalement apparaitre dans la facture. 
    - `Num_Depannage` (str) : Numéro de dépannage lié au PDL. 
    - `Type_Compteur` (str) : Type de compteur installé (ex. "CCB": linky, "CEB": bleu, "CFB": Electromécanique, "PSC": Point sans comptage).
    - `Num_Compteur` (str) : Numéro de série du compteur.

    ### 🔹 **Relevés de début de période**
    - `Date_Releve_deb` (`datetime64[ns, Europe/Paris]`, nullable) : Date du relevé de début.
    - `Nature_Index_deb` (str, nullable) : Nature de l'index relevé (ex. "Réel", "Estimé").
    - `HP_deb` (float, nullable) : Index de consommation en Heures Pleines (début de période).
    - `HC_deb` (float, nullable) : Index de consommation en Heures Creuses (début de période).
    - `HCH_deb` (float, nullable) : Index en Heures Creuses Hautes.
    - `HPH_deb` (float, nullable) : Index en Heures Pleines Hautes.
    - `HPB_deb` (float, nullable) : Index en Heures Pleines Basses.
    - `HCB_deb` (float, nullable) : Index en Heures Creuses Basses.
    - `BASE_deb` (float, nullable) : Index de consommation en option BASE.
    - `source_releve_deb` (str) : Source du relevé (ex. "C15", "R151").

    ### 🔹 **Relevés de fin de période**
    - `Date_Releve_fin` (`datetime64[ns, Europe/Paris]`, nullable) : Date du relevé de fin.
    - `HP_fin` (float, nullable) : Index de consommation en Heures Pleines (fin de période).
    - `HC_fin` (float, nullable) : Index de consommation en Heures Creuses (fin de période).
    - `HCH_fin` (float, nullable) : Index en Heures Creuses Hautes.
    - `HPH_fin` (float, nullable) : Index en Heures Pleines Hautes.
    - `HPB_fin` (float, nullable) : Index en Heures Pleines Basses.
    - `HCB_fin` (float, nullable) : Index en Heures Creuses Basses.
    - `BASE_fin` (float, nullable) : Index de consommation en option BASE.
    - `source_releve_fin` (str) : Source du relevé (ex. "C15", "R151").

    ## 🔹 **Contraintes et Validation**
    - **`Date_Releve_deb` et `Date_Releve_fin`** sont en **Europe/Paris** (`datetime64[ns, Europe/Paris]`).
    - Les valeurs des index (`HP`, `HC`, `BASE`, etc.) peuvent être `NaN` si non disponibles. 
    Les inndex doivent êtres exprimés dans les cadrans les plus précis disponibles uniquement. 
    Exemple si on a un compteur de type CCB (linky), les cadrans HPH et HPB doivent êtres remplis, et pas HP.
    - `source_releve_deb` et `source_releve_fin` doivent toujours être renseignés.

    ## 🚀 **Utilisation**
    ```python
    from core.schemas import BaseFacturationModèle
    
    # Valider un DataFrame de facturation
    df = BaseFacturationModèle.validate(df)
    ```
    """
    # Données contractuelles
    pdl: Series[str] = pa.Field(nullable=False,)
    Ref_Situation_Contractuelle: Series[str] = pa.Field(nullable=False,)
    Segment_Clientele: Series[str] = pa.Field(nullable=False,)
    Categorie: Series[str] = pa.Field(nullable=False,)

    Puissance_Souscrite: Series[int] = pa.Field(nullable=False, coerce=True)
    Formule_Tarifaire_Acheminement: Series[str] = pa.Field(nullable=False,)
    
    Num_Depannage: Series[str] = pa.Field(nullable=False,)
    Type_Compteur: Series[str] = pa.Field(nullable=False,)
    Num_Compteur: Series[str] = pa.Field(nullable=False,)

    # Début de période de facturation
    Date_Releve_deb: Series[Annotated[pd.DatetimeTZDtype, "s", "Europe/Paris"]] = pa.Field(nullable=True, coerce=True)
    Nature_Index_deb: Series[str] = pa.Field(nullable=True)
    HP_deb: Series[float] = pa.Field(nullable=True, coerce=True)
    HC_deb: Series[float] = pa.Field(nullable=True, coerce=True)
    HCH_deb: Series[float] = pa.Field(nullable=True, coerce=True)
    HPH_deb: Series[float] = pa.Field(nullable=True, coerce=True)
    HPB_deb: Series[float] = pa.Field(nullable=True, coerce=True)
    HCB_deb: Series[float] = pa.Field(nullable=True, coerce=True)
    BASE_deb: Series[float] = pa.Field(nullable=True, coerce=True)
    source_releve_deb: Series[str] = pa.Field(nullable=True)
    # Fin de période de facturation
    Date_Releve_fin: Series[Annotated[pd.DatetimeTZDtype, "s", "Europe/Paris"]] = pa.Field(nullable=True, coerce=True)
    HP_fin: Series[float] = pa.Field(nullable=True, coerce=True)
    HC_fin: Series[float] = pa.Field(nullable=True, coerce=True)
    HCH_fin: Series[float] = pa.Field(nullable=True, coerce=True)
    HPH_fin: Series[float] = pa.Field(nullable=True, coerce=True)
    HPB_fin: Series[float] = pa.Field(nullable=True, coerce=True)
    HCB_fin: Series[float] = pa.Field(nullable=True, coerce=True)
    BASE_fin: Series[float] = pa.Field(nullable=True, coerce=True)
    source_releve_fin: Series[str] = pa.Field(nullable=True)


    # @pa.check("Type_Compteur", "HP_deb", "HP_fin", "HC_deb", "HC_fin", "BASE_deb", "BASE_fin")
    # def validate_linky(cls, df: pd.DataFrame) -> pd.Series:
    #     """
    #     Si Type_Compteur == "CCB", alors HP, HC et BASE doivent être NaN.
    #     """
    #     mask_ccb = df["Type_Compteur"] == "CCB"
    #     return df.loc[mask_ccb, ["HP_deb", "HP_fin", "HC_deb", "HC_fin", "BASE_deb", "BASE_fin"]].isna().all(axis=1)