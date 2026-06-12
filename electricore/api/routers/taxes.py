"""Router des endpoints taxes énergétiques : Accise TICFE et CTA (issue #82)."""

from dataclasses import asdict
from datetime import date

from fastapi import APIRouter, Depends, Query

from electricore.api.decorators import arrow_endpoint, xlsx_endpoint
from electricore.api.models import MillesimeResponse, PeremptionResponse
from electricore.api.security import get_current_api_key
from electricore.api.serializers import arrow_stream, xlsx_multi_sheet
from electricore.api.services.taxes_service import (
    accise_par_contrat_service,
    cta_par_contrat_service,
    rapport_accise_service,
    rapport_cta_service,
)
from electricore.core.builds.rapport_taxe import feuilles_rapport_taxe
from electricore.core.millesimes import millesimes
from electricore.core.peremption import avertissements_peremption
from electricore.integrations.odoo.decorators import with_odoo

router = APIRouter(tags=["taxes"])


# =============================================================================
# Millésimes des taux régulés (#185, ADR-0024)
# =============================================================================


@router.get("/taxes/millesimes", response_model=list[MillesimeResponse])
def get_millesimes(api_key: str = Depends(get_current_api_key)) -> list[MillesimeResponse]:
    """Millésimes des trois registres de taux régulés (TURPE, Accise, CTA).

    Dérivés des CSV versionnés — dit ce que cette instance « sait » de la
    réglementation. Sans dépendance Odoo : disponible sur une instance no-ERP.
    """
    return [MillesimeResponse(**asdict(m)) for m in millesimes()]


@router.get("/taxes/peremption", response_model=list[PeremptionResponse])
def get_peremption(api_key: str = Depends(get_current_api_key)) -> list[PeremptionResponse]:
    """Avertissements de péremption des taux régulés à la date du jour (#186, ADR-0024).

    Rythmes attendus encodés comme heuristiques (TURPE au 1ᵉʳ août, Accise au
    1ᵉʳ janvier, CTA sans rythme connu). Liste vide = registres présumés à jour.
    """
    return [PeremptionResponse(**asdict(a), message=a.message) for a in avertissements_peremption(date.today())]


# =============================================================================
# Accise TICFE
# =============================================================================


@xlsx_endpoint(router, "/taxes/accise/rapport.xlsx", filename="accise_rapport{trimestre}.xlsx", requires_odoo=True)
@with_odoo
def export_accise_rapport_xlsx(
    odoo,
    trimestre: str | None = Query(
        default=None,
        examples=["2025-T1"],
        description="Filtre par trimestre au format YYYY-TX. Sans filtre : toutes les données.",
    ),
) -> bytes:
    """Livrable facturiste : Accise TICFE en XLSX multi-onglets (Résumé / Par taux / Détail)."""
    return xlsx_multi_sheet(feuilles_rapport_taxe(rapport_accise_service(odoo, trimestre)))


@xlsx_endpoint(router, "/taxes/accise/detail.xlsx", filename="accise_detail{trimestre}.xlsx", requires_odoo=True)
@with_odoo
def export_accise_detail_xlsx(
    odoo,
    trimestre: str | None = Query(
        default=None,
        examples=["2025-T1"],
        description="Filtre par trimestre au format YYYY-TX. Sans filtre : toutes les données.",
    ),
) -> bytes:
    """Détail Accise TICFE en XLSX mono-onglet (table PDL × mois, cas technique)."""
    return xlsx_multi_sheet({"Détail": accise_par_contrat_service(odoo, trimestre)})


@arrow_endpoint(router, "/taxes/accise/detail.arrow", requires_odoo=True)
@with_odoo
def export_accise_detail_arrow(
    odoo,
    trimestre: str | None = Query(
        default=None,
        examples=["2025-T1"],
        description="Filtre par trimestre au format YYYY-TX. Sans filtre : toutes les données.",
    ),
) -> bytes:
    """Détail Accise TICFE en Arrow IPC stream (table PDL × mois, cas technique)."""
    return arrow_stream(accise_par_contrat_service(odoo, trimestre))


# =============================================================================
# CTA (Contribution Tarifaire d'Acheminement)
# =============================================================================


@xlsx_endpoint(router, "/taxes/cta/rapport.xlsx", filename="cta_rapport{trimestre}.xlsx", requires_odoo=True)
@with_odoo
def export_cta_rapport_xlsx(
    odoo,
    trimestre: str | None = Query(
        default=None,
        examples=["2025-T1"],
        description="Filtre par trimestre au format YYYY-TX. Sans filtre : toutes les données.",
    ),
) -> bytes:
    """Livrable facturiste : CTA en XLSX multi-onglets (Résumé / Par taux / Détail)."""
    return xlsx_multi_sheet(feuilles_rapport_taxe(rapport_cta_service(odoo, trimestre)))


@xlsx_endpoint(router, "/taxes/cta/detail.xlsx", filename="cta_detail{trimestre}.xlsx", requires_odoo=True)
@with_odoo
def export_cta_detail_xlsx(
    odoo,
    trimestre: str | None = Query(
        default=None,
        examples=["2025-T1"],
        description="Filtre par trimestre au format YYYY-TX. Sans filtre : toutes les données.",
    ),
) -> bytes:
    """Détail CTA mensuel en XLSX mono-onglet (PDL × mois, cas technique)."""
    return xlsx_multi_sheet({"Détail": cta_par_contrat_service(odoo, trimestre)})


@arrow_endpoint(router, "/taxes/cta/detail.arrow", requires_odoo=True)
@with_odoo
def export_cta_detail_arrow(
    odoo,
    trimestre: str | None = Query(
        default=None,
        examples=["2025-T1"],
        description="Filtre par trimestre au format YYYY-TX. Sans filtre : toutes les données.",
    ),
) -> bytes:
    """Détail CTA mensuel en Arrow IPC stream (PDL × mois, cas technique)."""
    return arrow_stream(cta_par_contrat_service(odoo, trimestre))
