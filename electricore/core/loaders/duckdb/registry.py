"""
Registre des configurations de flux DuckDB.

Ce module centralise toutes les configurations de flux Enedis
avec leur schéma SQL, transformation Polars et validation Pandera.
"""

# Imports de validation
from electricore.core.models.releve_index import RelevéIndex

from .query import QueryConfig
from .sql import FLUX_SCHEMAS
from .transforms import (
    transform_dates,
    transform_factures,
    transform_historique,
    transform_r64,
    transform_releves,
)

# =============================================================================
# REGISTRE FONCTIONNEL DES FLUX
# =============================================================================

# Codes événementiels C15 — cf. electricore/core/CONTEXT.md
ENTREES_C15: tuple[str, ...] = ("PMES", "MES", "CFNE")
SORTIES_C15: tuple[str, ...] = ("RES", "CFNS")


class FluxInconnu(ValueError):
    """Flux demandé absent du registre `FLUX_CONFIGS`.

    Levée au seam du loader (`flux(nom)`) — le loader reste agnostique HTTP
    (ADR-0016/0019) ; un caller transport mappe sur un 404. Pendant inverse de
    `DuckDBLockError`. Porte `nom` et `disponibles` pour un consommateur qui ne
    veut pas parser le message.
    """

    def __init__(self, nom: str, disponibles: list[str]):
        self.nom = nom
        self.disponibles = disponibles
        super().__init__(f"Flux '{nom}' inconnu. Flux disponibles : {disponibles}")


FLUX_CONFIGS: dict[str, QueryConfig] = {
    "c15": QueryConfig(schema=FLUX_SCHEMAS["c15"], transform=transform_historique, validator=None),
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
    "affaires": QueryConfig(
        # jalon_date_heure est TIMESTAMPTZ : on le convertit en Europe/Paris (instant
        # préservé) pour un dtype stable quel que soit le fuseau de session (poste local
        # vs CI/VPS en UTC) — même contrat que les autres loaders (cf. transform_dates).
        schema=FLUX_SCHEMAS["affaires"],
        transform=transform_dates(("jalon_date_heure",)),
        validator=None,  # opérationnel (suivi des affaires SGE), pas de schéma Pandera
    ),
}
