"""Router de la déclaration CTA parc entier, sans Odoo (#651, ADR-0058).

Deux représentations d'**un seul service** (`declaration_cta_service`) : Arrow (le cœur,
grain RSC × mois avec empreintes) et xlsx (Résumé / Par taux / Détail). ERP-agnostique
(zéro `integrations`, ADR-0016), donc tag `taxes` et non `legacy` — successeur des
`/taxes/cta/*` legacy, qui restent inchangés à côté.
"""

from fastapi import APIRouter, Query

from electricore.api.decorators import ARROW_MEDIA_TYPE, XLSX_MEDIA_TYPE, binary_endpoint
from electricore.api.serializers import arrow_stream, xlsx_multi_sheet
from electricore.api.services.declaration_cta_service import declaration_cta, feuilles_declaration_cta

router = APIRouter()

_TRIMESTRE = Query(
    default=None,
    pattern=r"^\d{4}-T[1-4]$",
    examples=["2026-T3"],
    description="Trimestre YYYY-TN, qui doit être terminé (sinon 422 `precondition`). "
    "Sans filtre : tous les trimestres terminés.",
)


@binary_endpoint(router, "/taxes/cta/declaration.arrow", media_type=ARROW_MEDIA_TYPE, tags=["taxes"])
def export_declaration_cta_arrow(trimestre: str | None = _TRIMESTRE) -> bytes:
    """Déclaration CTA, cœur RSC × mois en Arrow IPC (`source_hash` + `empreinte_trimestre`)."""
    return arrow_stream(declaration_cta(trimestre))


@binary_endpoint(
    router,
    "/taxes/cta/declaration.xlsx",
    media_type=XLSX_MEDIA_TYPE,
    filename="declaration_cta{trimestre}.xlsx",
    tags=["taxes"],
)
def export_declaration_cta_xlsx(trimestre: str | None = _TRIMESTRE) -> bytes:
    """Déclaration CTA en XLSX : Résumé (avec empreinte du trimestre) / Par taux / Détail."""
    return xlsx_multi_sheet(feuilles_declaration_cta(declaration_cta(trimestre)))
