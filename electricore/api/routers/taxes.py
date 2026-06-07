"""Router des endpoints taxes énergétiques : Accise TICFE et CTA (issue #82)."""

from fastapi import APIRouter, Query

from electricore.api.decorators import arrow_endpoint, xlsx_endpoint
from electricore.api.serializers import arrow_stream, xlsx_multi_sheet
from electricore.integrations.odoo.decorators import with_odoo
from electricore.integrations.odoo.taxes import (
    accise_par_contrat,
    cta_par_contrat,
    feuilles_rapport_accise,
    feuilles_rapport_cta,
    rapport_accise,
    rapport_cta,
)

router = APIRouter(tags=["taxes"])


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
    return xlsx_multi_sheet(feuilles_rapport_accise(rapport_accise(odoo, trimestre)))


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
    return xlsx_multi_sheet({"Détail": accise_par_contrat(odoo, trimestre)})


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
    return arrow_stream(accise_par_contrat(odoo, trimestre))


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
    return xlsx_multi_sheet(feuilles_rapport_cta(rapport_cta(odoo, trimestre)))


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
    return xlsx_multi_sheet({"Détail": cta_par_contrat(odoo, trimestre)})


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
    return arrow_stream(cta_par_contrat(odoo, trimestre))
