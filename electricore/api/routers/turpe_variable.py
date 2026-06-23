"""Router du calculateur TURPE variable : Odoo POST l'assiette (ADR-0030, #247/#409).

Endpoint **ERP-agnostique** (zéro `integrations.odoo`, ADR-0016) : il expose un
calculateur electricore-natif (assiette par cadran → montant €), pas un miroir des
modèles `souscription.*`. Réponse en JSON enveloppé avec `contract_version`, auth
`X-API-Key` — mêmes conventions que `GET /facturation/meta-periodes`.

Les modèles (`LigneTurpeVariable`, `TurpeVariableRequest`) sont **single-sourcés** dans
`electricore_client` (ADR-0043) : importés ici, plus aucune définition inline.
"""

# Modèles single-sourcés dans le paquet client (ADR-0043).
from electricore_client.models import TurpeVariableRequest
from fastapi import APIRouter, Depends, Response

from electricore.api.security import get_current_api_key
from electricore.api.services.turpe_variable_service import CONTRAT_VERSION, calculer_turpe_variable

router = APIRouter(tags=["facturation"])


@router.post("/facturation/turpe-variable")
async def post_turpe_variable(
    requete: TurpeVariableRequest,
    response: Response,
    api_key: str = Depends(get_current_api_key),
):
    """Valorise un lot d'assiettes → un `turpe_variable_eur` par `id` (JSON enveloppé).

    **Authentification requise** (`X-API-Key`).

    `turpe_variable_eur = Σ energie_cadran × c_cadran(FTA, debut) / 100`. Passer les 7
    cadrans : les coefficients à zéro de la règle FTA arbitrent la granularité (ADR-0030).
    Le `contract_version` est porté à la fois dans l'enveloppe et dans l'en-tête
    `X-Contract-Version` (cohérent avec les autres endpoints facturiste).
    """
    results = calculer_turpe_variable([ligne.model_dump() for ligne in requete.lignes])
    response.headers["X-Contract-Version"] = str(CONTRAT_VERSION)
    return {"contract_version": CONTRAT_VERSION, "results": results}
