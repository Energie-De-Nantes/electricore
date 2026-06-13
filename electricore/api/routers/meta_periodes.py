"""Router de lecture des méta-périodes mensuelles : Odoo tire d'electricore (ADR-0027).

Endpoint **ERP-agnostique** (zéro `integrations/odoo`, ADR-0016) : il expose un modèle
electricore-natif (la *méta-période mensuelle*), pas un miroir des modèles `souscription.*`.
Réponse en JSON enveloppé, même convention que `/flux/{table}` (`mois` / `filters` /
`pagination` / `data`).
"""

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from electricore.api.security import get_current_api_key
from electricore.api.services.meta_periodes_service import CONTRAT_VERSION, meta_periodes

router = APIRouter(tags=["facturation"])


@router.get("/facturation/meta-periodes")
async def get_meta_periodes(
    mois: str | None = Query(
        None,
        examples=["2026-05-01"],
        description="Mois au format YYYY-MM-DD (défaut : dernier mois disponible)",
    ),
    rsc: Annotated[
        list[str] | None,
        Query(description="Filtrer par référence(s) de situation contractuelle (répétable)"),
    ] = None,
    limit: int = Query(500, le=2000, ge=1, description="Nombre maximum de lignes à retourner"),
    offset: int = Query(0, ge=0, description="Nombre de lignes à ignorer (pagination)"),
    api_key: str = Depends(get_current_api_key),
):
    """Méta-périodes mensuelles `(ref_situation_contractuelle, debut, fin)` en JSON enveloppé.

    **Authentification requise** (`X-API-Key`).

    Charge utile non valorisée aux prix fournisseur : quantités physiques + montants
    réseau. Odoo construit/upsert ses `souscription.periode` à partir de ce flux.
    """
    mois_resolu, df = meta_periodes(mois=mois, rsc=rsc)
    total = df.height
    rows = df.slice(offset, limit).to_dicts()
    return {
        "mois": mois_resolu,
        "contract_version": CONTRAT_VERSION,
        "filters": {"rsc": rsc} if rsc else None,
        "pagination": {"limit": limit, "offset": offset, "returned": len(rows), "total": total},
        "data": rows,
    }
