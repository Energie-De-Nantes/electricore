"""
Modèle Pandera pour les règles tarifaires TURPE.

Ce module définit la structure de validation des données TURPE utilisées
dans les calculs de taxes et redevances pour l'énergie électrique.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from typing import Annotated, Optional


class RegleTurpe(pa.DataFrameModel):
    """
    Modèle de validation pour les règles tarifaires TURPE.
    
    Structure les données des grilles tarifaires d'acheminement de l'électricité
    définies par la CRE (Commission de Régulation de l'Énergie).
    """
    
    # Identifiant de la formule tarifaire
    Formule_Tarifaire_Acheminement: Series[str] = pa.Field(nullable=False)
    
    # Période de validité des règles
    start: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False)
    end: Optional[Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]]] = pa.Field(nullable=True)
    
    # Composantes fixes annuelles (€/an)
    cg: Series[float] = pa.Field(nullable=False, ge=0)  # Composante de gestion
    cc: Series[float] = pa.Field(nullable=False, ge=0)  # Composante de comptage
    b: Series[float] = pa.Field(nullable=False, ge=0)   # Terme de puissance souscrite (€/kW/an)
    
    # Tarifs par cadran horaire (c€/kWh)
    HPH: Series[float] = pa.Field(nullable=False, ge=0)  # Heures Pleines Hiver
    HCH: Series[float] = pa.Field(nullable=False, ge=0)  # Heures Creuses Hiver
    HPB: Series[float] = pa.Field(nullable=False, ge=0)  # Heures Pleines Été
    HCB: Series[float] = pa.Field(nullable=False, ge=0)  # Heures Creuses Été
    HP: Series[float] = pa.Field(nullable=False, ge=0)   # Heures Pleines (bi-horaire)
    HC: Series[float] = pa.Field(nullable=False, ge=0)   # Heures Creuses (bi-horaire)
    BASE: Series[float] = pa.Field(nullable=False, ge=0) # Tarif Base (monophasé)
    
    class Config:
        """Configuration pour permettre des colonnes supplémentaires."""
        strict = False