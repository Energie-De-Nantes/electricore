"""electricore-client — client léger vers l'API facturiste electricore.

Distribué séparément du moteur, dépendances de base **httpx + pydantic
uniquement** : ce paquet n'importe jamais polars/duckdb/fastapi au top-level
(invariant prouvé par le test de pureté). Le client Arrow historique
(DataFrames polars) vit dans le sous-module `electricore_client.arrow`, derrière
l'extra `[arrow]`, et n'est *pas* importé ici.

Lecture seule sur electricore (ADR-0012). Voir ADR-0043 pour la conception.
"""

from __future__ import annotations

from .client import ElectricoreClient
from .exceptions import (
    ContractVersionError,
    ElectricoreClientError,
    IngestionEnCours,
    PreconditionNonRemplie,
)
from .headers import EnTetesMeta
from .models import (
    EstimationProvision,
    LigneChronologie,
    LigneEvenement,
    LignePeriodeEnergie,
    LigneReleve,
    LigneSortie,
    LigneTurpeVariable,
    ObjetReleve,
    PeriodeMeta,
    RapportProvision,
    ResolutionRscRequest,
    ResultatResolutionRsc,
    ResultatTurpeVariable,
    SortiesRequest,
    TurpeVariableRequest,
)
from .streaming import JsonlStream

__version__ = "0.5.0"

__all__ = [
    "ElectricoreClient",
    "ElectricoreClientError",
    "IngestionEnCours",
    "PreconditionNonRemplie",
    "ContractVersionError",
    "EnTetesMeta",
    "JsonlStream",
    "PeriodeMeta",
    "ObjetReleve",
    "LigneChronologie",
    "LigneEvenement",
    "LigneReleve",
    "LignePeriodeEnergie",
    "LigneTurpeVariable",
    "TurpeVariableRequest",
    "ResultatTurpeVariable",
    "ResolutionRscRequest",
    "ResultatResolutionRsc",
    "EstimationProvision",
    "RapportProvision",
    "SortiesRequest",
    "LigneSortie",
    "__version__",
]
