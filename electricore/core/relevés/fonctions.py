import pandera as pa
from pandera.typing import DataFrame

from electricore.core.relevés.modèles import RelevéIndex, RequêteRelevé

@pa.check_types
def interroger_relevés(
    requêtes: DataFrame[RequêteRelevé], 
    relevés: DataFrame[RelevéIndex]
) -> DataFrame:
    """
    🔍 Interroge les relevés d'index pour récupérer les index correspondant à une liste de dates et PDL.

    Args:
        requêtes (DataFrame[RequêteRelevé]): DataFrame contenant les colonnes "Date_Releve" et "pdl" pour la requête.
        relevés (DataFrame[RelevéIndex]): DataFrame contenant les relevés d'index.

    Returns:
        DataFrame: DataFrame contenant les relevés correspondant aux requêtes.
    """
    # 📌 Jointure entre les requêtes et les relevés
    return requêtes.merge(
        relevés,
        on=["Date_Releve", "pdl"],  
        how="left"  # On garde toutes les requêtes même si aucun relevé n'est trouvé
    )
