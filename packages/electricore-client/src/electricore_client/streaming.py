"""Flux JSONL typé : un context-manager qui valide chaque ligne en un modèle.

Les endpoints de lecture (méta-périodes, chronologie) streament leur résultat
en JSONL — une ligne = un objet JSON, validé à la volée par un modèle pydantic.
Les métadonnées (version de contrat, mois/grain) arrivent en en-têtes ; la garde
de version est appliquée à l'ouverture (`__enter__`). Le flux est lazy (rien
n'est matérialisé tant qu'on n'itère pas) et libère la connexion à la sortie du
`with`, **même mi-consommé**.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from typing import Generic, TypeVar

import httpx

from .headers import EnTetesMeta

T = TypeVar("T")

# Type-construit une ligne (dict JSON décodé) → modèle typé. En pratique
# `Model.model_validate`, mais on garde la signature ouverte (unions discriminées).
Validateur = Callable[[dict], T]


class JsonlStream(Generic[T]):
    """Itérateur paresseux sur un flux JSONL, validé ligne à ligne.

    Construit autour d'un `httpx.Client.stream(...)` (context-manager de requête).
    `__enter__` ouvre la réponse, parse les en-têtes de métadonnées et applique la
    garde de version ; l'itération décode et valide chaque ligne non vide. À la
    sortie du `with`, la réponse est fermée (connexion rendue au pool) qu'elle ait
    été entièrement consommée ou non.
    """

    def __init__(
        self,
        *,
        client: httpx.Client,
        method: str,
        url: str,
        params: dict | None,
        headers: dict[str, str],
        validateur: Validateur[T],
        version_attendue: int,
        verifier_version: Callable[[int, int], None],
        raise_for_status: Callable[[httpx.Response], None],
    ) -> None:
        self._request_cm = client.stream(method, url, params=params, headers=headers)
        self._response: httpx.Response | None = None
        self._validateur = validateur
        self._version_attendue = version_attendue
        self._verifier_version = verifier_version
        self._raise_for_status = raise_for_status
        self._meta: EnTetesMeta | None = None
        self._closed = False

    # -- context manager ------------------------------------------------------

    def __enter__(self) -> JsonlStream[T]:
        response = self._request_cm.__enter__()
        self._response = response
        self._raise_for_status(response)
        self._meta = EnTetesMeta.from_headers(response.headers)
        # Garde de version appliquée à l'ouverture (avant toute consommation).
        self._verifier_version(self._version_attendue, self._meta.contract_version)
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Libère le flux (connexion rendue au pool), idempotent et mi-consommé-safe."""
        if self._closed:
            return
        self._closed = True
        self._request_cm.__exit__(None, None, None)

    # -- métadonnées (en-têtes) ----------------------------------------------

    @property
    def meta(self) -> EnTetesMeta:
        if self._meta is None:
            raise RuntimeError("Flux non ouvert : utiliser `with client.<endpoint>(...) as stream:`")
        return self._meta

    @property
    def contract_version(self) -> int:
        return self.meta.contract_version

    @property
    def mois(self) -> str | None:
        return self.meta.mois

    @property
    def grain(self) -> str | None:
        return self.meta.grain

    # -- itération ------------------------------------------------------------

    def __iter__(self) -> Iterator[T]:
        if self._response is None:
            raise RuntimeError("Flux non ouvert : utiliser `with client.<endpoint>(...) as stream:`")
        for ligne in self._response.iter_lines():
            if not ligne.strip():
                continue
            yield self._validateur(json.loads(ligne))

    def collect(self) -> list[T]:
        """Matérialise tout le flux en liste (convenance)."""
        return list(self)
