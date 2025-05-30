import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame
from electricore.core.périmètre.modèles import HistoriquePérimètre
from electricore.inputs.flux.modèles import FluxC15

# Point sur les cas relevés Avant et/ou relevé Après. 
# Corrélations calculées sur les données dispo le 2025/02/26 :
# | Evenement_Declencheur | Avant | Après | ~Avant | ~Après |
# |-----------------------|-------|-------|--------|--------|
# | AUTRE                 | 0.0   | 0.0   | 1.0    | 1.0    |
# | CFNE                  | 0.0   | 0.95  | 1.0    | 0.05   |
# | CFNS                  | 1.0   | 0.0   | 0.0    | 1.0    |
# | CMAT                  | 0.11  | 0.78  | 0.89   | 0.22   |
# | MCF                   | 1.0   | 1.0   | 0.0    | 0.0    |
# | MCT                   | 0.67  | 1.0   | 0.33   | 0.0    |
# | MDACT                 | 0.0   | 0.0   | 1.0    | 1.0    |
# | MDPRM                 | 0.0   | 0.0   | 1.0    | 1.0    |
# | MES                   | 0.0   | 0.98  | 1.0    | 0.02   |
# | PMES                  | 0.04  | 0.88  | 0.96   | 0.12   |
# | RES                   | 1.0   | 0.0   | 0.0    | 1.0    |

# AUTRE, MDACT, MDPRM = jamais de relevés, ni Avant, ni Après
# MES, PMES, CFNE, jamais Avant, PRESQUE toujours Après (On a rien quand c'est nature = ESTIMÉ...)
# RES, CFNS = toujours Avant, jamais Après (makes sense)
# MCF toujours Avant ET Après (Modification de la programmation du calendrier fournisseur)
# MCT souvent Avant, toujours Après (Modification de la formule tarifaire d’acheminement ou de la puissance souscrite ou du statut d’Autoconsommation Collective)
# CMAT rarement Avant, souvent Après (Changement de compteur ou de disjoncteur ou Activation du calendrier Distributeur)
# Pas encore rencontré : MDBRA (Modification de données de branchement)

@pa.check_types()
def lire_flux_c15(source: pd.DataFrame) -> DataFrame[HistoriquePérimètre]:

    df: DataFrame[FluxC15] = FluxC15.validate(source)
    df["Source"] = "flux_C15"

    return HistoriquePérimètre.validate(df)