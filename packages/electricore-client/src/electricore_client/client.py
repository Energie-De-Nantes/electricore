"""Client de la facturiste electricore (httpx + pydantic, sans polars).

`ElectricoreClient` assemble le substrat de transport (`_BaseClient`) et les
méthodes d'endpoint. Ce module reste **polars-free** : le client Arrow
historique vit dans le sous-module `arrow` (extra `[arrow]`), jamais importé
ici au top-level.
"""

from __future__ import annotations

from .transport import _BaseClient


class ElectricoreClient(_BaseClient):
    """Client synchrone vers une instance de l'API facturiste electricore.

    Construit avec une `url` de base et une `api_key` (en-tête `X-API-Key`).
    Les méthodes de lecture (méta-périodes, chronologie) streament du JSONL ;
    `turpe_variable` est un POST RPC. Le client Arrow (DataFrames polars) est
    fourni séparément via l'extra `[arrow]` (`electricore_client.arrow`).
    """
