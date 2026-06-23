"""Métadonnées portées par les en-têtes de réponse.

Les endpoints de lecture streament leurs lignes en JSONL : ils n'ont plus
d'enveloppe pour loger la version de contrat et le contexte résolu (mois,
grain). Ces métadonnées remontent donc dans des en-têtes HTTP, parsés ici en
un petit modèle typé.
"""

from __future__ import annotations

from collections.abc import Mapping

from pydantic import BaseModel, ConfigDict

# En-têtes de métadonnées (conventionnés serveur + client).
HEADER_CONTRACT_VERSION = "X-Contract-Version"
HEADER_MOIS = "X-Mois"
HEADER_GRAIN = "X-Grain"


class EnTetesMeta(BaseModel):
    """Métadonnées d'un flux JSONL, extraites des en-têtes de réponse.

    `contract_version` est toujours présent ; `mois` (méta-périodes) et `grain`
    (chronologie) le sont selon l'endpoint.
    """

    model_config = ConfigDict(extra="ignore")

    contract_version: int
    mois: str | None = None
    grain: str | None = None

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> EnTetesMeta:
        """Parse les en-têtes de réponse (insensibles à la casse via httpx)."""
        version = headers.get(HEADER_CONTRACT_VERSION)
        if version is None:
            raise ValueError(f"En-tête manquant : {HEADER_CONTRACT_VERSION}")
        return cls(
            contract_version=int(version),
            mois=headers.get(HEADER_MOIS),
            grain=headers.get(HEADER_GRAIN),
        )
