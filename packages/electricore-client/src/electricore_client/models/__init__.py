"""Modèles de contrat des endpoints facturiste (source unique, ADR-0043).

Ces modèles pydantic décrivent les lignes émises par l'API en JSONL. Ils sont
**single-sourcés** : le moteur (routers FastAPI) les importe d'ici plutôt que
de redéfinir des modèles inline.
"""

from __future__ import annotations

from .meta_periodes import CONTRAT_VERSION_META_PERIODES, ObjetReleve, PeriodeMeta

__all__ = [
    "PeriodeMeta",
    "ObjetReleve",
    "CONTRAT_VERSION_META_PERIODES",
]
