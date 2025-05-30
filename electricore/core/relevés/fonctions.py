import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame

from electricore.core.relevés.modèles import RelevéIndex, RequêteRelevé

@pa.check_types
def interroger_relevés(
    requêtes: DataFrame[RequêteRelevé], 
    relevés: DataFrame[RelevéIndex], 
    tolérance: pd.Timedelta = pd.Timedelta(hours=4)
) -> DataFrame[RelevéIndex]:
    """
    🔍 Interroge les relevés d'index pour récupérer les index correspondant à une liste de dates et PDL,
    avec une tolérance d'une heure sur la Date_Releve.

    Args:
        requêtes (DataFrame[RequêteRelevé]): DataFrame contenant les colonnes "Date_Releve" et "pdl" pour la requête.
        relevés (DataFrame[RelevéIndex]): DataFrame contenant les relevés d'index.

    Returns:
        DataFrame: DataFrame contenant les relevés correspondant aux requêtes.
    """

    # Sauvegarde de l'index d'origine
    requêtes_avec_index_col = requêtes.copy().reset_index()

    # 🔄 Jointure avec tolérance
    relevés_proches = pd.merge_asof(
        requêtes_avec_index_col.sort_values(by=["Date_Releve"]), 
        relevés.copy().sort_values(by=["Date_Releve"]), 
        on="Date_Releve", 
        by="pdl", 
        direction="nearest",
        tolerance=tolérance 
    )

    return relevés_proches.dropna(subset=['Source']).set_index('index')
