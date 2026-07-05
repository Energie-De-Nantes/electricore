"""Router de lecture des prestations F15 : la file « à refacturer » d'Odoo tire d'ici.

Endpoint **ERP-agnostique** (ADR-0016) : il expose les lignes F15 `unite='UNITE'`
electricore-natives, pas un miroir de `souscription.refacturation`. Réponse en
**JSONL streamé** (ADR-0043), modèle de ligne single-sourcé dans
`electricore_client` (`PrestationF15`). Pull-tout sans pagination ni fenêtre
temporelle (volume faible, ~une ligne par PDL) — la dédup vit chez le
consommateur, par `reference` (souscriptions_odoo#37).
"""

from typing import Annotated

# Modèle single-sourcé dans le paquet client (ADR-0043).
from electricore_client.models import PrestationF15
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from electricore.api.security import get_current_api_key
from electricore.api.serializers.jsonl import jsonl_response, reponses_openapi_jsonl
from electricore.api.services.prestations_service import CONTRAT_VERSION, prestations

router = APIRouter(tags=["facturation"])


@router.get(
    "/facturation/prestations",
    response_class=StreamingResponse,
    responses=reponses_openapi_jsonl(PrestationF15, "Prestations F15 (`PrestationF15`, contrat v1)."),
)
async def get_prestations(
    rsc: Annotated[
        list[str] | None,
        Query(description="Filtrer par référence(s) de situation contractuelle (répétable)"),
    ] = None,
    api_key: str = Depends(get_current_api_key),
):
    """Prestations et indemnités ponctuelles du flux F15 en **JSONL streamé** (pull-tout).

    **Authentification requise** (`X-API-Key`).

    Une ligne JSON = une `PrestationF15` (contrat v1) ; `reference` est la clé de
    dédup calculée serveur (le F15 n'a pas d'identifiant de ligne). Flux non
    paginé, sans fenêtre temporelle : le consommateur upsert par `reference`.

    **Réponse JSONL streamé** (`application/x-ndjson`, NDJSON) : à consommer
    **ligne par ligne** — pour inspecter à la main : `curl … | jq -c .`.
    """
    df = prestations(rsc=rsc)
    headers = {"X-Contract-Version": str(CONTRAT_VERSION)}
    # Validate-then-stream (#426) : toute ligne hors-contrat fait un 500 atomique.
    return jsonl_response(df, valider=PrestationF15.model_validate, headers=headers)
