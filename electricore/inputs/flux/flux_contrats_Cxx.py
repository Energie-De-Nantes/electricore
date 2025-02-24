import pandas as pd
import pandera as pa
from pandera.typing import DataFrame
from electricore.core.périmètre.modèles import HistoriquePérimètre
from electricore.inputs.flux.modèles import FluxC15


@pa.check_types()
def lire_flux_c15(source: pd.DataFrame) -> DataFrame[HistoriquePérimètre]:

    df: DataFrame[FluxC15] = FluxC15.validate(source)
    df["Source"] = "flux_C15"

    return HistoriquePérimètre.validate(df)