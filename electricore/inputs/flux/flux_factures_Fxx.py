import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame
from electricore.core.périmètre.modèles import HistoriquePérimètre
from electricore.inputs.flux.modèles import FluxF1X

from icecream import ic

@pa.check_types()
def lire_flux_f1x(f1x: DataFrame[FluxF1X]): # -> DataFrame[HistoriquePérimètre]:

    df: DataFrame[FluxF1X] = f1x.copy()
    # FluxF15.validate(source)
    df.sort_values(by=["pdl", "Date_Facture"], inplace=True)
    ordre_colonnes = FluxF1X.to_schema().columns.keys()
    df = df[ordre_colonnes]
    return df