import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
from typing import Annotated, Optional

class PeriodeAbonnement(pa.DataFrameModel):
    """
    Représente une période homogène de facturation de la part fixe (TURPE)
    pour une situation contractuelle donnée.
    """
    Ref_Situation_Contractuelle: Series[str]
    mois_annee: Series[str]  # ex: "mars 2025"
    periode_debut_lisible: Series[str]  # ex: "1 mars 2025"
    periode_fin_lisible: Series[str]    # ex: "31 mars 2025" ou "en cours"
    Formule_Tarifaire_Acheminement: Series[str]
    Puissance_Souscrite: Series[float]
    nb_jours: Series[int]
    periode_debut: Series[Annotated[pd.DatetimeTZDtype, "ns", "Europe/Paris"]] = pa.Field(nullable=False, coerce=True)

    # Champs de sortie (optionnels)
    turpe_fixe_journalier: Optional[Series[float]] = pa.Field(nullable=True)
    turpe_fixe: Optional[Series[float]] = pa.Field(nullable=True)