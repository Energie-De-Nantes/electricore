"""Router de la résolution RSC : Odoo POST des `id_Affaire` (#282/#5, ADR-0010).

Endpoint **ERP-agnostique** (zéro `integrations.odoo`, ADR-0016) : il recoupe deux flux
Enedis — X12 (`flux_affaires`, existence de l'affaire) et C15 (`flux_c15`, qui porte
l'`id_affaire` natif **et** le `ref_situation_contractuelle`) — pour résoudre chaque
`id_Affaire` en sa RSC. Réponse en JSON enveloppé avec `contract_version`, auth `X-API-Key`
— mêmes conventions que `POST /facturation/turpe-variable`.

Le modèle (`ResolutionRscRequest`) est **single-sourcé** dans `electricore_client`
(ADR-0043) : importé ici, plus aucune définition inline.
"""

import polars as pl

# Modèle single-sourcé dans le paquet client (ADR-0043).
from electricore_client.models import ResolutionRscRequest
from fastapi import APIRouter, Depends, Response

from electricore.api.security import get_current_api_key
from electricore.api.services.rsc_service import CONTRAT_VERSION, resoudre_rsc

router = APIRouter(tags=["facturation"])


def _charger_c15(ids: list[str]) -> pl.DataFrame:
    """Événements C15 dont `id_affaire ∈ ids` (seam IO, monkeypatché en test).

    Le filtre est poussé dans la requête DuckDB (`id_affaire IN (...)`) : on ne ramène
    que les événements des affaires demandées, jamais tout le C15. On lit le `flux_c15`
    natif (pas le mart `spine_contrat` forward-fillé) pour que `id_affaire` ne matche que
    son événement déclencheur réel.
    """
    from electricore.core.loaders.duckdb import c15

    return c15().filter({"id_affaire": ids}).collect()


def _charger_affaires(ids: list[str]) -> pl.DataFrame:
    """Affaires X12 dont `affaire_id ∈ ids` (seam IO, monkeypatché en test) — recoupement."""
    from electricore.core.loaders.duckdb import affaires

    return affaires().filter({"affaire_id": ids}).collect()


@router.post("/facturation/rsc")
async def post_resolution_rsc(
    requete: ResolutionRscRequest,
    response: Response,
    api_key: str = Depends(get_current_api_key),
) -> dict:
    """Résout un lot d'`id_Affaire` → un `ref_situation_contractuelle` par id (JSON enveloppé).

    **Authentification requise** (`X-API-Key`).

    Chaque `id_affaire` envoyé revient, portant **soit** `ref_situation_contractuelle`
    **soit** un `error` (xor) — jamais de silent-drop. Le `contract_version` est porté à la
    fois dans l'enveloppe et dans l'en-tête `X-Contract-Version` (cohérent avec les autres
    endpoints facturiste).
    """
    response.headers["X-Contract-Version"] = str(CONTRAT_VERSION)
    ids = requete.ids
    # Court-circuit : un lot vide ne touche pas DuckDB (un `IN ()` casserait le SQL).
    if not ids:
        return {"contract_version": CONTRAT_VERSION, "results": []}

    results = resoudre_rsc(ids, c15=_charger_c15(ids), affaires=_charger_affaires(ids))
    return {"contract_version": CONTRAT_VERSION, "results": results}
