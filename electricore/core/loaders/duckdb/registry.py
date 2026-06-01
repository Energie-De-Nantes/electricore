"""
Registre des configurations de flux DuckDB.

Ce module centralise toutes les configurations de flux Enedis
avec leur schéma SQL, transformation Polars et validation Pandera.
"""

from electricore.core.models.historique_perimetre import HistoriquePérimètre

# Imports de validation
from electricore.core.models.releve_index import RelevéIndex

from .query import QueryConfig
from .sql import FLUX_SCHEMAS
from .transforms import (
    transform_factures,
    transform_historique_perimetre,
    transform_r64,
    transform_releves,
)

# =============================================================================
# REGISTRE FONCTIONNEL DES FLUX
# =============================================================================

FLUX_CONFIGS: dict[str, QueryConfig] = {
    "c15": QueryConfig(
        schema=FLUX_SCHEMAS["c15"], transform=transform_historique_perimetre, validator=HistoriquePérimètre
    ),
    "r151": QueryConfig(schema=FLUX_SCHEMAS["r151"], transform=transform_releves, validator=RelevéIndex),
    "r15": QueryConfig(schema=FLUX_SCHEMAS["r15"], transform=transform_releves, validator=RelevéIndex),
    "f15": QueryConfig(
        schema=FLUX_SCHEMAS["f15"],
        transform=transform_factures,
        validator=None,  # Pas encore de modèle Pandera pour les factures
    ),
    "r64": QueryConfig(
        schema=FLUX_SCHEMAS["r64"],
        transform=transform_r64,
        validator=None,  # Pas encore de modèle Pandera pour R64
    ),
}
