"""Router des sorties du périmètre : Odoo POST un lot de RSC (#632, ADR-0052).

`POST /perimetre/sorties` sert la fin de souscription gouvernée par le fait C15
(souscriptions_odoo#21, ADR 0031 côté addon) : à chaque campagne, Odoo interroge quelles
de ses souscriptions sont sorties du périmètre — événement C15 `{RES, CFNS}`, symétrique
pour le fournisseur. Le filtre RSC est **à la requête**, la liste vient du consommateur :
les périmètres Enedis peuvent être partagés entre entités, indistinguables dans les
flux — l'autorité du « à nous » est la souscription Odoo. Réponse en JSON enveloppé avec
`contract_version`, auth `X-API-Key` — mêmes conventions que `POST /facturation/rsc`.

Le modèle (`SortiesRequest`) est **single-sourcé** dans `electricore_client`
(ADR-0043) : importé ici, plus aucune définition inline.
"""

import polars as pl

# Modèle single-sourcé dans le paquet client (ADR-0043).
from electricore_client.models import SortiesRequest
from fastapi import APIRouter, Depends, Response

from electricore.api.security import get_current_api_key
from electricore.api.services.sorties_service import CONTRAT_VERSION, lister_sorties

router = APIRouter(tags=["perimetre"])


def _charger_c15_sorties(rsc: list[str]) -> pl.DataFrame:
    """Événements C15 de sortie (RES/CFNS) dont `ref_situation_contractuelle ∈ rsc`
    (seam IO, monkeypatché en test).

    Le filtre RSC **et** le filtre code de sortie sont poussés dans la requête DuckDB :
    on ne ramène ni tout le C15, ni les RSC hors du lot demandé.
    """
    from electricore.core.loaders.duckdb import c15

    return c15().filter({"ref_situation_contractuelle": rsc}).sorties().collect()


@router.post("/perimetre/sorties")
async def post_perimetre_sorties(
    requete: SortiesRequest,
    response: Response,
    api_key: str = Depends(get_current_api_key),
) -> dict:
    """Sorties du périmètre pour un lot de RSC (JSON enveloppé).

    **Authentification requise** (`X-API-Key`).

    Une ligne par RSC **sortie** (code C15 `RES`/`CFNS`) ; une RSC encore présente ou
    inconnue n'apparaît simplement pas dans la réponse — pas d'erreur, c'est le cas
    nominal « pas sortie ». Le `contract_version` est porté à la fois dans l'enveloppe
    et dans l'en-tête `X-Contract-Version` (cohérent avec les autres endpoints
    facturiste).
    """
    response.headers["X-Contract-Version"] = str(CONTRAT_VERSION)
    rsc = requete.rsc
    # Court-circuit : un lot vide ne touche pas DuckDB (un `IN ()` casserait le SQL).
    if not rsc:
        return {"contract_version": CONTRAT_VERSION, "sorties": []}

    sorties = lister_sorties(_charger_c15_sorties(rsc))
    return {"contract_version": CONTRAT_VERSION, "sorties": sorties}
