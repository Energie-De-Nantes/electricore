"""Modèles de contrat des endpoints facturiste (source unique, ADR-0043).

Ces modèles pydantic décrivent les lignes émises par l'API en JSONL. Ils sont
**single-sourcés** : le moteur (routers FastAPI) les importe d'ici plutôt que
de redéfinir des modèles inline.
"""

from __future__ import annotations

from .chronologie import (
    CONTRAT_VERSION_CHRONOLOGIE,
    LigneChronologie,
    LigneEvenement,
    LignePeriodeEnergie,
    LigneReleve,
    valider_ligne_chronologie,
)
from .meta_periodes import CONTRAT_VERSION_META_PERIODES, ObjetReleve, PeriodeMeta
from .prestations import CONTRAT_VERSION_PRESTATIONS, PrestationF15
from .rsc import (
    CONTRAT_VERSION_RSC,
    ResolutionRscRequest,
    ResultatResolutionRsc,
)
from .turpe_variable import (
    CONTRAT_VERSION_TURPE_VARIABLE,
    LigneTurpeVariable,
    ResultatTurpeVariable,
    TurpeVariableRequest,
)

__all__ = [
    "PeriodeMeta",
    "ObjetReleve",
    "CONTRAT_VERSION_META_PERIODES",
    "LigneChronologie",
    "LigneEvenement",
    "LigneReleve",
    "LignePeriodeEnergie",
    "CONTRAT_VERSION_CHRONOLOGIE",
    "valider_ligne_chronologie",
    "LigneTurpeVariable",
    "TurpeVariableRequest",
    "ResultatTurpeVariable",
    "CONTRAT_VERSION_TURPE_VARIABLE",
    "ResolutionRscRequest",
    "ResultatResolutionRsc",
    "CONTRAT_VERSION_RSC",
    "PrestationF15",
    "CONTRAT_VERSION_PRESTATIONS",
]
