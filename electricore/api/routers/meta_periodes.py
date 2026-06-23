"""Router de lecture des méta-périodes mensuelles : Odoo tire d'electricore (ADR-0027).

Endpoint **ERP-agnostique** (zéro `integrations/odoo`, ADR-0016) : il expose un modèle
electricore-natif (la *méta-période mensuelle*), pas un miroir des modèles `souscription.*`.

Réponse en **JSONL streamé** (`application/jsonl`, une ligne = un `PeriodeMeta`) plutôt
qu'en enveloppe paginée (ADR-0043) : pas de pagination, le serveur streame toutes les
lignes, et les métadonnées (`contract_version`, `mois` résolu) remontent dans les en-têtes
de réponse. Les modèles de ligne (`PeriodeMeta`/`ObjetReleve`) sont **single-sourcés** dans
`electricore_client` — le router les importe, ne les redéfinit pas.
"""

import datetime as dt
from collections.abc import Iterator
from typing import Annotated

# Modèles single-sourcés dans le paquet client (ADR-0043).
from electricore_client.models import PeriodeMeta
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from electricore.api.security import get_current_api_key
from electricore.api.services.meta_periodes_service import CONTRAT_VERSION, meta_periodes

router = APIRouter(tags=["facturation"])

MEDIA_TYPE_JSONL = "application/jsonl"


def _stringifier_dates(ligne: dict) -> dict:
    """Rend les `datetime`/`date` de la ligne en ISO8601 (le contrat porte des `str`)."""
    return {
        clef: (valeur.isoformat() if isinstance(valeur, (dt.datetime, dt.date)) else valeur)
        for clef, valeur in ligne.items()
    }


def _lignes_jsonl(rows: list[dict]) -> Iterator[bytes]:
    """Construit un `PeriodeMeta` par ligne et l'émet comme une ligne JSONL.

    La construction par modèle **valide** chaque ligne (le serveur ne peut pas servir
    une ligne hors-contrat) et normalise la sérialisation (alias, types).
    """
    for row in rows:
        periode = PeriodeMeta.model_validate(_stringifier_dates(row))
        yield (periode.model_dump_json() + "\n").encode()


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
    rows = df.to_dicts()
    headers = {
        "X-Contract-Version": str(CONTRAT_VERSION),
        "X-Mois": mois_resolu,
    }
    return StreamingResponse(_lignes_jsonl(rows), media_type=MEDIA_TYPE_JSONL, headers=headers)
