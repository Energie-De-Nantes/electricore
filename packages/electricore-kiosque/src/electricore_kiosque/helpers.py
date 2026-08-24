"""Helpers partagés par les notebooks Kiosque : clé API → client `electricore-client`.

Zéro secret côté serveur (ADR-0057) : la clé vit dans la session navigateur du
notebook, jamais stockée ici. Un notebook construit son client via
`construire_client(cle)`, récupère ses données via une fonction `recuperer_*` —
toute la logique de fetch/erreur vit ici pour que le notebook reste de la
présentation pure au-dessus des helpers (voir `exports.py`).
"""

from __future__ import annotations

import httpx
from electricore_client import ElectricoreClient

from electricore_kiosque.config import api_url

_STATUTS_CLE_REFUSEE = {401, 403}


class CleApiRefusee(Exception):
    """La clé API saisie est invalide ou révoquée — message actionnable, pas de stacktrace."""

    def __init__(self) -> None:
        super().__init__("Clé API refusée : contacte ton admin.")


def construire_client(cle: str, *, http_client: httpx.Client | None = None) -> ElectricoreClient:
    """Client `electricore-client` configuré depuis une clé saisie + `KIOSQUE__API_URL`."""
    return ElectricoreClient(url=api_url(), api_key=cle, http_client=http_client)


def recuperer_meta_periodes(client: ElectricoreClient) -> list[dict]:
    """Méta-périodes mensuelles (facturation), aplaties en lignes tabulaires pour l'UI.

    `releves_utilises` (trace d'index imbriquée) est exclu : la table Kiosque
    reste plate — les néophytes consultent des montants/consos, pas la trace
    légale détaillée.

    Raises:
        CleApiRefusee: clé API invalide ou révoquée (401/403 côté API).
    """
    try:
        with client.meta_periodes() as stream:
            return [ligne.model_dump(exclude={"releves_utilises"}) for ligne in stream]
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in _STATUTS_CLE_REFUSEE:
            raise CleApiRefusee() from exc
        raise
