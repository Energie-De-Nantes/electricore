"""Router de la *vue facturiste* : chronologie du point / du contrat + verdicts (#367/#408).

Endpoint de **lecture** (« Odoo tire » [ADR-0027], read-only [ADR-0012], `X-API-Key`) qui rend
la **frise complète** d'un point (`pdl`) ou d'un contrat (`rsc`) : faits (événements C15 *y
compris hors-comptage* + relevés) tissés avec les verdicts dérivés (qualité/communication/
énergie). Drill-down/explication du **pourquoi**, là où `/meta-periodes` est l'extrait mensuel
valorisé — **pas de montants tarifaires** ici (différenciateur explicite).

Réponse en **JSONL streamé** (`application/jsonl`, ADR-0043) : une ligne = un
`LigneChronologie` (union discriminée sur `type_ligne`), validé par construction. Les
métadonnées (`contract_version`, `grain`) remontent dans les en-têtes. Le modèle d'union est
**single-sourcé** dans `electricore_client`.
"""

import datetime as dt
from collections.abc import Iterator

from electricore_client.models import valider_ligne_chronologie
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from electricore.api.security import get_current_api_key
from electricore.api.services.chronologie_service import CONTRAT_VERSION, chronologie_point_ou_contrat

router = APIRouter(tags=["facturation"])

MEDIA_TYPE_JSONL = "application/jsonl"


def _stringifier_dates(ligne: dict) -> dict:
    """Rend les `datetime`/`date` de la ligne en ISO8601 (le contrat porte des `str`)."""
    return {
        clef: (valeur.isoformat() if isinstance(valeur, (dt.datetime, dt.date)) else valeur)
        for clef, valeur in ligne.items()
    }


def _lignes_jsonl(rows: list[dict]) -> Iterator[bytes]:
    """Valide chaque ligne via l'union discriminée et l'émet comme une ligne JSONL.

    Les clés à valeur nulle sont retirées (un registre/une énergie nul n'est jamais
    émis, conformément au service) avant la résolution de l'union.
    """
    for row in rows:
        propres = {clef: valeur for clef, valeur in _stringifier_dates(row).items() if valeur is not None}
        ligne = valider_ligne_chronologie(propres)
        yield (ligne.model_dump_json(exclude_none=True) + "\n").encode()


@router.get("/facturation/chronologie")
async def get_chronologie(
    pdl: str | None = Query(
        None,
        examples=["12345678901234"],
        description="Point de livraison : toute l'histoire du PDL (RSC successives + charnières)",
    ),
    rsc: str | None = Query(
        None,
        description="Référence de situation contractuelle : une tenure bornée entrée→sortie",
    ),
    page_size: int | None = Query(
        None,
        ge=1,
        description="Indication optionnelle de taille de lot (hint) — le flux n'est pas paginé",
    ),
    api_key: str = Depends(get_current_api_key),
):
    """Frise complète d'un point (`pdl`) **ou** d'un contrat (`rsc`) en **JSONL streamé**.

    **Authentification requise** (`X-API-Key`). Lecture seule.

    Une ligne JSON = un `LigneChronologie` (union discriminée sur `type_ligne` :
    `evenement | releve | periode_energie`). Chaque ligne porte le **fait** (événement *ou*
    relevé) et, pour les périodes dérivées, les **verdicts** qualité/communication/énergie —
    sans montant tarifaire (turpe/cta/accise). Métadonnées en en-têtes (`X-Contract-Version`,
    `X-Grain`). Fournir exactement un grain (`pdl` XOR `rsc`).
    """
    if pdl is None and rsc is None:
        raise HTTPException(422, "Fournir un grain : `pdl` (point) ou `rsc` (contrat).")

    frise = chronologie_point_ou_contrat(pdl=pdl, rsc=rsc)
    rows = frise.to_dicts()
    grain = "point" if pdl is not None else "contrat"
    headers = {"X-Contract-Version": str(CONTRAT_VERSION), "X-Grain": grain}
    return StreamingResponse(_lignes_jsonl(rows), media_type=MEDIA_TYPE_JSONL, headers=headers)
