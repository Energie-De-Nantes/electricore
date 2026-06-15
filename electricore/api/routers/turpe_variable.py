"""Router du calculateur TURPE variable : Odoo POST l'assiette (ADR-0030, #247).

Endpoint **ERP-agnostique** (zéro `integrations.odoo`, ADR-0016) : il expose un
calculateur electricore-natif (assiette par cadran → montant €), pas un miroir des
modèles `souscription.*`. Réponse en JSON enveloppé avec `contract_version`, auth
`X-API-Key` — mêmes conventions que `GET /facturation/meta-periodes`.
"""

from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from electricore.api.security import get_current_api_key
from electricore.api.services.turpe_variable_service import CONTRAT_VERSION, calculer_turpe_variable

router = APIRouter(tags=["facturation"])


class LigneTurpeVariable(BaseModel):
    """Une ligne d'assiette à valoriser. Les 7 cadrans sont nullables (null → 0)."""

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

    lignes: list[LigneTurpeVariable] = Field(..., description="Lot de lignes (le cas mono-période est n=1)")


@router.post("/facturation/turpe-variable")
async def post_turpe_variable(
    requete: TurpeVariableRequest,
    api_key: str = Depends(get_current_api_key),
):
    """Valorise un lot d'assiettes → un `turpe_variable_eur` par `id` (JSON enveloppé).

    **Authentification requise** (`X-API-Key`).

    `turpe_variable_eur = Σ energie_cadran × c_cadran(FTA, debut) / 100`. Passer les 7
    cadrans : les coefficients à zéro de la règle FTA arbitrent la granularité (ADR-0030).
    """
    results = calculer_turpe_variable([ligne.model_dump() for ligne in requete.lignes])
    return {"contract_version": CONTRAT_VERSION, "results": results}
