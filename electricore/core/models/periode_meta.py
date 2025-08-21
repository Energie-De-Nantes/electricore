import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from typing import Annotated, Optional


class PeriodeMeta(pa.DataFrameModel):
    """
    Représente une méta-période mensuelle de facturation agrégée.
    
    Cette classe combine les données des périodes d'abonnement et d'énergie
    en une seule ligne par mois et par référence contractuelle, permettant
    une facturation simplifiée tout en conservant l'exactitude mathématique.
    
    L'agrégation utilise :
    - Puissance moyenne pondérée par nb_jours (mathématiquement équivalente)
    - Somme simple pour les énergies et montants TURPE
    - Métadonnées pour traçabilité des changements
    """
    # Identifiants
    Ref_Situation_Contractuelle: Series[str] = pa.Field(nullable=False)
    pdl: Series[str] = pa.Field(nullable=False)
    mois_annee: Series[str] = pa.Field(nullable=False)  # ex: "mars 2025"
    
    # Période globale du mois
    debut: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)
    fin: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)
    nb_jours: Series[int] = pa.Field(nullable=False, ge=0)
    
    # Données agrégées depuis abonnements
    puissance_moyenne: Series[float] = pa.Field(nullable=False, ge=0)
    Formule_Tarifaire_Acheminement: Series[str] = pa.Field(nullable=False)
    turpe_fixe: Series[float] = pa.Field(nullable=True)
    
    # Données agrégées depuis énergies
    BASE_energie: Series[float] = pa.Field(nullable=True, coerce=True)
    HP_energie: Series[float] = pa.Field(nullable=True, coerce=True)
    HC_energie: Series[float] = pa.Field(nullable=True, coerce=True)
    turpe_variable: Series[float] = pa.Field(nullable=True)

    #turpe: Series[float] = pa.Field(nullable=True)
    
    # Métadonnées de traçabilité
    nb_sous_periodes_abo: Series[int] = pa.Field(nullable=False, ge=1)
    nb_sous_periodes_energie: Series[int] = pa.Field(nullable=False, ge=0)
    has_changement: Series[bool] = pa.Field(nullable=False)
    
    # Colonnes optionnelles pour compatibilité
    debut_lisible: Optional[Series[str]] = pa.Field(nullable=True)
    fin_lisible: Optional[Series[str]] = pa.Field(nullable=True)
    
    class Config:
        """Configuration pour permettre des colonnes supplémentaires."""
        strict = False