"""Router de la *vue facturiste* : chronologie du point / du contrat + verdicts (#367).

Endpoint de **lecture** (« Odoo tire » [ADR-0027], read-only [ADR-0012], `X-API-Key`) qui rend
la **frise complète** d'un point (`pdl`) ou d'un contrat (`rsc`) : faits (événements C15 *y
compris hors-comptage* + relevés) tissés avec les verdicts dérivés (qualité/communication/
énergie). Drill-down/explication du **pourquoi**, là où `/meta-periodes` est l'extrait mensuel
valorisé — **pas de montants tarifaires** ici (différenciateur explicite).

Réponse en JSON enveloppé, même convention que `/facturation/meta-periodes` et `/releves`
(`grain` / `filters` / `pagination` / `data`).
"""

from fastapi import APIRouter, Depends, HTTPException, Query

from electricore.api.security import get_current_api_key
from electricore.api.services.chronologie_service import CONTRAT_VERSION, chronologie_point_ou_contrat

router = APIRouter(tags=["facturation"])


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
    limit: int = Query(2000, le=10000, ge=1, description="Nombre maximum de lignes à retourner"),
    offset: int = Query(0, ge=0, description="Nombre de lignes à ignorer (pagination)"),
    api_key: str = Depends(get_current_api_key),
):
    """Frise complète d'un point (`pdl`) **ou** d'un contrat (`rsc`) en JSON enveloppé.

    **Authentification requise** (`X-API-Key`). Lecture seule.

    Chaque ligne porte le **fait** (événement *ou* relevé, son origine/sa nature) et, pour les
    périodes dérivées, les **verdicts** qualité/communication/énergie associés — sans montant
    tarifaire (turpe/cta/accise). Fournir exactement un grain (`pdl` ou `rsc`).
    """
    if pdl is None and rsc is None:
        raise HTTPException(422, "Fournir un grain : `pdl` (point) ou `rsc` (contrat).")

    frise = chronologie_point_ou_contrat(pdl=pdl, rsc=rsc)
    total = frise.height
    rows = frise.slice(offset, limit).to_dicts()
    grain = "point" if pdl is not None else "contrat"
    filtres = {k: v for k, v in (("pdl", pdl), ("rsc", rsc)) if v}
    return {
        "grain": grain,
        "contract_version": CONTRAT_VERSION,
        "filters": filtres or None,
        "pagination": {"limit": limit, "offset": offset, "returned": len(rows), "total": total},
        "data": rows,
    }
