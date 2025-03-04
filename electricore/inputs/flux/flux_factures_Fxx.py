import pandas as pd
import pandera as pa
from pandera.typing import DataFrame
from electricore.core.périmètre.modèles import HistoriquePérimètre
from electricore.inputs.flux.modèles import FluxF15

from icecream import ic

@pa.check_types()
def lire_flux_f15(f15: DataFrame[FluxF15]): # -> DataFrame[HistoriquePérimètre]:

    df: DataFrame[FluxF15] = f15.copy()
    # FluxF15.validate(source)
    df["Source"] = "flux_F15"
    df.sort_values(by=["pdl", "Date_Facture"], inplace=True)
    return df