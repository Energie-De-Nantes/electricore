import pandas as pd
import pandera as pa
from pandera.typing import DataFrame
from electricore.core.relevés.modèles import RelevéIndex
from electricore.inputs.flux.modèles import FluxR151


# Flux R151 énergies quotidiennes
@pa.check_types()
def charger_flux_r151(source: pd.DataFrame) -> DataFrame[RelevéIndex]:

    df: DataFrame[FluxR151] = FluxR151.validate(source)
    df["Source"] = "flux_R151"

    # Réordonner les colonnes pour correspondre au modèle Pandera
    ordre_colonnes = FluxR151.to_schema().columns.keys()
    df = df[ordre_colonnes]

    # Supprimer des colonnes si présentes
    _to_drop: list[str] = [c for c in ['INCONNU'] if c in df.columns]
    df = df.drop(columns=_to_drop)
    return RelevéIndex.validate(df)