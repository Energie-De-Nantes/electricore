"""Router de lecture des méta-périodes mensuelles : Odoo tire d'electricore (ADR-0027).

Endpoint **ERP-agnostique** (zéro `integrations/odoo`, ADR-0016) : il expose un modèle
electricore-natif (la *méta-période mensuelle*), pas un miroir des modèles `souscription.*`.

Réponse en **JSONL streamé** (`application/jsonl`, une ligne = un `PeriodeMeta`) plutôt
qu'en enveloppe paginée (ADR-0043) : pas de pagination, le serveur streame toutes les
lignes, et les métadonnées (`contract_version`, `mois` résolu) remontent dans les en-têtes
de réponse. Les modèles de ligne (`PeriodeMeta`/`ObjetReleve`) sont **single-sourcés** dans
`electricore_client` — le router les importe, ne les redéfinit pas.
"""

from typing import Annotated

# Modèles single-sourcés dans le paquet client (ADR-0043).
from electricore_client.models import PeriodeMeta
from fastapi import APIRouter, Depends, Query

from electricore.api.security import get_current_api_key
from electricore.api.serializers.jsonl import jsonl_response
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
    page_size: int | None = Query(
        None,
        ge=1,
        description="Indication optionnelle de taille de lot (hint) — le flux n'est pas paginé",
    ),
    api_key: str = Depends(get_current_api_key),
):
    """Méta-périodes mensuelles `(ref_situation_contractuelle, debut, fin)` en **JSONL streamé**.

    **Authentification requise** (`X-API-Key`).

    Une ligne JSON = un `PeriodeMeta` (contrat v3) ; le flux n'est pas paginé. Les
    métadonnées sont dans les en-têtes : `X-Contract-Version` et `X-Mois` (mois résolu).
    Charge utile non valorisée aux prix fournisseur : quantités physiques + montants réseau.
    Odoo construit/upsert ses `souscription.periode` à partir de ce flux.
    """
    mois_resolu, df = meta_periodes(mois=mois, rsc=rsc)
    headers = {
        "X-Contract-Version": str(CONTRAT_VERSION),
        "X-Mois": mois_resolu,
    }
    # Validate-then-stream (#426) : `jsonl_response` valide toutes les lignes en amont, donc
    # une ligne hors-contrat fait un 500 atomique avant tout octet (pas un 200 tronqué).
    return jsonl_response(df, valider=PeriodeMeta.model_validate, headers=headers)
