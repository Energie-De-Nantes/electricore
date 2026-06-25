"""Router des endpoints facturation : rapprochement Odoo ↔ Enedis (issue #82)."""

import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException, Query

from electricore.api.config import settings
from electricore.api.decorators import arrow_endpoint, xlsx_endpoint
from electricore.api.security import get_current_api_key
from electricore.api.serializers import arrow_stream, xlsx_multi_sheet
from electricore.api.services.check_facturation_service import check_odoo_xlsx, verifier_odoo
from electricore.api.services.facturation_service import (
    documents_facturation_du_mois,
    facturation_du_mois,
    rapport_facturation,
)
from electricore.core.builds.rapport_facturation import feuilles_rapport_facturation
from electricore.integrations.odoo.decorators import with_odoo

logger = logging.getLogger(__name__)

router = APIRouter(tags=["facturation"])


@xlsx_endpoint(router, "/facturation/rapport.xlsx", filename="facturation_rapport{mois}.xlsx", requires_odoo=True)
@with_odoo
def export_facturation_rapport_xlsx(
    odoo,
    mois: str | None = Query(
        default=None,
        examples=["2025-01-01"],
        description="Mois au format YYYY-MM-DD (défaut : dernier mois disponible dans les données)",
    ),
) -> bytes:
    """Livrable facturiste : 3 onglets (Résumé / Lignes / Changements puissance)."""
    return xlsx_multi_sheet(feuilles_rapport_facturation(rapport_facturation(odoo, mois)))


@xlsx_endpoint(router, "/facturation/detail.xlsx", filename="facturation_detail{mois}.xlsx", requires_odoo=True)
@with_odoo
def export_facturation_detail_xlsx(
    odoo,
    mois: str | None = Query(
        default=None,
        examples=["2025-01-01"],
        description="Mois au format YYYY-MM-DD (défaut : dernier mois disponible dans les données)",
    ),
) -> bytes:
    """Lignes brutes du rapprochement Odoo↔Enedis en XLSX mono-onglet (cas technique)."""
    return xlsx_multi_sheet({"Détail": facturation_du_mois(odoo, mois)})


@arrow_endpoint(router, "/facturation/detail.arrow", requires_odoo=True)
@with_odoo
def export_facturation_detail_arrow(
    odoo,
    mois: str | None = Query(
        default=None,
        examples=["2025-01-01"],
        description="Mois au format YYYY-MM-DD (défaut : dernier mois disponible dans les données)",
    ),
) -> bytes:
    """Lignes brutes du rapprochement Odoo↔Enedis en Arrow IPC stream (cas technique)."""
    return arrow_stream(facturation_du_mois(odoo, mois))


@router.get("/facturation/check/odoo")
async def check_facturation_odoo(api_key: str = Depends(get_current_api_key)):
    """
    Vérifications pré-facturation côté Odoo.

    **Authentification requise. Nécessite Odoo configuré.**

    Pour tous les sale.order avec state='sale', retourne :
    - `rsc_manquante` : liste des sale.order sans `x_ref_situation_contractuelle`
    - `cfne_manquante` : liste des sale.order sans `x_date_cfne`
    - `invoicing_state_counts` : répartition des `x_invoicing_state`
    - `factures_draft` : factures encore en draft (anomalie après campagne)

    Chaque entrée contient un lien direct vers l'enregistrement Odoo (champ `url`).
    """
    if not settings.is_odoo_configured:
        raise HTTPException(
            501,
            "Odoo non configuré. Définissez ODOO__URL/DB/USERNAME/PASSWORD.",
        )
    try:
        result = await asyncio.get_event_loop().run_in_executor(None, verifier_odoo)
    except Exception as e:
        logger.exception("Erreur facturation/check/odoo")
        raise HTTPException(503, f"Erreur lors de la vérification Odoo : {e}")
    return result


@xlsx_endpoint(router, "/facturation/check/odoo.xlsx", filename="check_odoo.xlsx", requires_odoo=True)
def export_check_odoo_xlsx() -> bytes:
    """Détail complet des vérifications pré-facturation Odoo en XLSX multi-onglets.

    Même contrôle que `/facturation/check/odoo`, sérialisé pour le cas où les
    anomalies dépassent ce qu'un message texte peut lister (issue #150).
    Forme stable : 5 onglets, vides quand rien à signaler (issue #173).
    """
    return check_odoo_xlsx()


@xlsx_endpoint(router, "/facturation/documents.xlsx", filename="facturation{mois}.xlsx", requires_odoo=True)
@with_odoo
def export_facturation_documents(
    odoo,
    mois: str | None = Query(
        default=None,
        examples=["2025-01-01"],
        description="Mois au format YYYY-MM-DD (défaut : dernier mois disponible dans les données)",
    ),
) -> bytes:
    """Livrable XLSX multi-onglets de tous les documents utiles pour la facturation (cf. #78).

    Onglets :

    - **F15 complet** : flux F15 du mois
    - **F15 prestations** : F15 filtré sur les prestations (unite = 'UNITE')
    - **C15 complet** : flux C15 du mois
    - **C15 sorties** : C15 filtré sur les sorties (RES + CFNS)
    - **Réconciliation** : réconciliation Odoo ↔ Enedis
    - **Changements puissance** : lignes avec changement de puissance
    """
    documents, _suffix = documents_facturation_du_mois(odoo, mois)
    return xlsx_multi_sheet(documents)
