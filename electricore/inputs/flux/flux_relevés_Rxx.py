import pandas as pd
import pandera as pa
from pandera.typing import DataFrame
from electricore.core.relevés.modèles import RelevéIndex
from electricore.inputs.flux.modèles import FluxR151


# Flux R151 énergies quotidiennes
@pa.check_types()
def charger_flux_r151(source: pd.DataFrame) -> DataFrame[RelevéIndex]:

    df = FluxR151.validate(source)
    df["Source"] = "flux_R151"
    return RelevéIndex.validate(df)