"""Modèles de contrat de la résolution RSC (POST RPC, #282/#5).

`POST /facturation/rsc` recoupe X12 (`flux_affaires`) ⨝ C15 (`flux_c15`) : l'appelant
prête un lot d'`id_Affaire`, electricore renvoie le `ref_situation_contractuelle`
correspondant — ou un motif d'erreur, par `id_affaire`, jamais de silent-drop. L'`id_affaire`
est **opaque** : ré-émis tel quel, jamais interprété.

Single-sourcés ici (ADR-0043) : le router FastAPI les importe, ne les redéfinit pas.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

# Version du contrat (cf. rsc_service.CONTRAT_VERSION). Source unique.
CONTRAT_VERSION_RSC = 1


class ResolutionRscRequest(BaseModel):
    """Lot d'`id_Affaire` à résoudre en une passe (le cas mono est `n = 1`)."""

    model_config = ConfigDict(extra="ignore")

    ids: list[str] = Field(..., description="Identifiants d'affaire (Id_Affaire Enedis) à résoudre")


class ResultatResolutionRsc(BaseModel):
    """Résultat pour un `id_affaire` : **soit** `ref_situation_contractuelle`, **soit**
    `error` (xor).

    Jamais de silent-drop (#282) : chaque `id_affaire` envoyé revient. `error` porte le
    motif (affaire inconnue, connue sans situation contractuelle, résolution ambiguë).
    """

    model_config = ConfigDict(extra="ignore")

    id_affaire: str
    ref_situation_contractuelle: str | None = None
    error: str | None = None
