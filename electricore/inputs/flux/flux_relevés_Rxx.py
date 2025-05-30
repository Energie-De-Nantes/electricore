import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame
from electricore.core.relevés.modèles import RelevéIndex
from electricore.inputs.flux.modèles import FluxR151

# Flux R151 énergies quotidiennes
@pa.check_types()
def lire_flux_r151(source: DataFrame[FluxR151]) -> DataFrame[RelevéIndex]:

    df: DataFrame[FluxR151] = source.copy()
    df["Source"] = "flux_R151"

    # Supprimer des lignes ou 'Id_Calendrier_Distributeur' == 'INCONNU'
    # Dans la doc enedis, c'est quand il n'y a pas de mesure. Pourquoi en faire une entrée alors ? Yo no sé
    # Supprimer les lignes où 'Id_Calendrier_Distributeur' == 'INCONNU' et == 'DN999999'
    # Note : Je ne sais pas ce que DN999999 vient faire là. C'est dans ces lignes qu'on retrouve des valeurs dans la colonnes INCONNU. Pour l'instant on supprime.   
    df = df[~df['Id_Calendrier_Distributeur'].isin(['INCONNU', 'DN999999'])]
    
    # Réordonner les colonnes pour correspondre au modèle RelevéIndex attendu
    # ordre_colonnes = RelevéIndex.to_schema().columns.keys()
    # df = df[ordre_colonnes]

    # Supprimer des colonnes si présentes
    _to_drop: list[str] = [c for c in ['INCONNU'] if c in df.columns]
    df = df.drop(columns=_to_drop)

    return df