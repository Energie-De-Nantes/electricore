"""Modèles de contrat du calculateur TURPE variable (POST RPC, ADR-0030, #247/#409).

Calculateur **sans état** : l'appelant prête l'assiette (énergies par cadran +
FTA + `debut`), electricore renvoie le **montant** € — ou un motif d'erreur, par
`id`, jamais de silent-drop. L'`id` est **opaque** : ré-émis tel quel, jamais
interprété. Les 7 cadrans sont nullables (null → 0).

Single-sourcés ici (ADR-0043) : le router FastAPI les importe, ne les redéfinit pas.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

# Version du contrat (cf. turpe_variable_service.CONTRAT_VERSION). Source unique.
CONTRAT_VERSION_TURPE_VARIABLE = 1


class LigneTurpeVariable(BaseModel):
    """Une ligne d'assiette à valoriser. Les 7 cadrans sont nullables (null → 0)."""

    model_config = ConfigDict(extra="ignore")

    id: str = Field(..., description="Identifiant opaque ré-émis tel quel (electricore ne l'interprète jamais)")
    formule_tarifaire_acheminement: str = Field(..., description="FTA, ex. BTINFCUST")
    debut: datetime = Field(..., description="Début de période (tz Europe/Paris) — sélection temporelle de la règle")
    energie_base_kwh: float | None = None
    energie_hp_kwh: float | None = None
    energie_hc_kwh: float | None = None
    energie_hph_kwh: float | None = None
    energie_hpb_kwh: float | None = None
    energie_hch_kwh: float | None = None
    energie_hcb_kwh: float | None = None


class TurpeVariableRequest(BaseModel):
    """Lot de lignes à valoriser en une passe."""

    model_config = ConfigDict(extra="ignore")

    lignes: list[LigneTurpeVariable] = Field(..., description="Lot de lignes (le cas mono-période est n=1)")


class ResultatTurpeVariable(BaseModel):
    """Résultat pour un `id` : **soit** `turpe_variable_eur`, **soit** `error` (xor).

    Jamais de silent-drop (ADR-0030) : chaque `id` envoyé revient. `error` porte le
    motif (FTA inconnue, aucune règle pour la date) ; `turpe_variable_eur` le montant.
    """

    model_config = ConfigDict(extra="ignore")

    id: str
    turpe_variable_eur: float | None = None
    error: str | None = None
