"""Helpers partagés par les notebooks Kiosque : clé API → client `electricore-client`.

Zéro secret côté serveur (ADR-0057) : la clé vit dans la session navigateur du
notebook, jamais stockée ici. Un notebook appelle directement une fonction
`recuperer_*(cle)` — elle ouvre et referme son propre client (`construire_client`/
`construire_client_arrow`, toujours publics pour les cas qui ont besoin du client
nu) — toute la logique de fetch/erreur/cycle de vie vit ici pour que le notebook
reste de la présentation pure au-dessus des helpers (voir `exports.py`).

Les relevés/flux bruts passent par `ElectricoreArrowClient` (extra `[arrow]`,
amendement 2026-08-24 à l'ADR-0057) : polars entre dans le Kiosque via cet extra
public du client, jamais via le moteur `electricore`.
"""

from __future__ import annotations

import httpx
from electricore_client import ElectricoreClient
from electricore_client.arrow import ElectricoreArrowClient

from electricore_kiosque.config import api_url

_STATUTS_CLE_REFUSEE = {401, 403}

# Bandeau « vue tronquée » de l'onglet Relevés (#720) : plafond dur côté kiosque,
# jamais de transfert du mart entier. Heuristique de troncature volontairement
# simple : lignes retournées == la limite ⇒ probablement tronqué (faux positif
# possible si le mart contient exactement ce compte, sans conséquence pratique).
LIMITE_RELEVES = 100_000


class CleApiRefusee(Exception):
    """La clé API saisie est invalide ou révoquée — message actionnable, pas de stacktrace."""

    def __init__(self) -> None:
        super().__init__("Clé API refusée : contacte ton admin.")


class TableFluxAbsente(Exception):
    """La table de flux demandée n'existe pas sur cette box — message propre, pas de 404 brut."""

    def __init__(self, table: str) -> None:
        super().__init__(f"Table de flux « {table} » absente de cette box.")


def construire_client(cle: str, *, http_client: httpx.Client | None = None) -> ElectricoreClient:
    """Client `electricore-client` configuré depuis une clé saisie + `KIOSQUE__API_URL`."""
    return ElectricoreClient(url=api_url(), api_key=cle, http_client=http_client)


def construire_client_arrow(cle: str, *, http_client: httpx.Client | None = None) -> ElectricoreArrowClient:
    """Client Arrow (`electricore-client[arrow]`) configuré depuis une clé saisie + `KIOSQUE__API_URL`.

    Même seam que `construire_client` — injection de `http_client` pour les tests.
    """
    return ElectricoreArrowClient(url=api_url(), api_key=cle, http_client=http_client)


def recuperer_meta_periodes(cle: str, *, http_client: httpx.Client | None = None) -> list[dict]:
    """Méta-périodes mensuelles (facturation), aplaties en lignes tabulaires pour l'UI.

    `releves_utilises` (trace d'index imbriquée) est exclu : la table Kiosque
    reste plate — les néophytes consultent des montants/consos, pas la trace
    légale détaillée.

    Ouvre et referme son propre client (context manager `ElectricoreClient`) :
    le kiosque est un process long-vécu multi-visiteurs, chaque appel referme
    sa connexion plutôt que de laisser trainer un pool par entrée de clé.

    Raises:
        CleApiRefusee: clé API invalide ou révoquée (401/403 côté API).
        config.ApiUrlManquante: `KIOSQUE__API_URL` absente (via `construire_client`).
    """
    with construire_client(cle, http_client=http_client) as client:
        try:
            with client.meta_periodes() as stream:
                return [ligne.model_dump(exclude={"releves_utilises"}) for ligne in stream]
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in _STATUTS_CLE_REFUSEE:
                raise CleApiRefusee() from exc
            raise


def recuperer_releves(
    cle: str,
    *,
    prm: str | None = None,
    debut: str | None = None,
    fin: str | None = None,
    http_client: httpx.Client | None = None,
) -> tuple[list[dict], bool]:
    """Mart de relevés canonique harmonisé (ADR-0029), plafonné à `LIMITE_RELEVES`.

    Retourne `(lignes, tronque)` — `tronque=True` quand la limite dure a été
    atteinte : le notebook affiche alors un bandeau « resserre tes filtres ».
    Jamais de transfert du mart entier (#720).

    Ouvre et referme son propre client (même patron que `recuperer_meta_periodes`).

    Raises:
        CleApiRefusee: clé API invalide ou révoquée (401/403 côté API).
        config.ApiUrlManquante: `KIOSQUE__API_URL` absente (via `construire_client_arrow`).
    """
    with construire_client_arrow(cle, http_client=http_client) as client:
        try:
            df = client.releves(prm=prm, debut=debut, fin=fin, limit=LIMITE_RELEVES)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in _STATUTS_CLE_REFUSEE:
                raise CleApiRefusee() from exc
            raise
    lignes = df.to_dicts()
    return lignes, len(lignes) >= LIMITE_RELEVES


def recuperer_flux(
    cle: str,
    table: str,
    *,
    prm: str | None = None,
    http_client: httpx.Client | None = None,
) -> list[dict]:
    """Contenu brut d'une table de flux Enedis, fidèle à la source (pas d'harmonisation).

    Ouvre et referme son propre client (même patron que `recuperer_meta_periodes`).

    Raises:
        CleApiRefusee: clé API invalide ou révoquée (401/403 côté API).
        TableFluxAbsente: la table n'existe pas sur cette box (404 côté API).
        config.ApiUrlManquante: `KIOSQUE__API_URL` absente (via `construire_client_arrow`).
    """
    with construire_client_arrow(cle, http_client=http_client) as client:
        try:
            df = client.flux(table, prm=prm)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in _STATUTS_CLE_REFUSEE:
                raise CleApiRefusee() from exc
            if exc.response.status_code == 404:
                raise TableFluxAbsente(table) from exc
            raise
    return df.to_dicts()
