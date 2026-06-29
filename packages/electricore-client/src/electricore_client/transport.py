"""Substrat de transport partagé par toutes les méthodes du client.

`_BaseClient` factorise tout ce qui est commun aux endpoints : URL de base,
en-tête `X-API-Key`, construction du `httpx.Client` (timeout), conversion
d'erreur HTTP et la garde de version de contrat.

Contrat X-Error-Kind (#424) — `_raise_for_status` discrimine les erreurs via
l'en-tête `X-Error-Kind` de la réponse, pas le code HTTP seul :
- `ingestion-lock` (503) → `IngestionEnCours` : verrou writer DuckDB, réessayable.
- `precondition`  (422) → `PreconditionNonRemplie` : précondition métier, actionnable.
- header absent → `httpx.HTTPStatusError` (raise_for_status standard).

Les méthodes d'endpoint (méta-périodes, chronologie, turpe variable, Arrow)
sont montées par-dessus dans `client.py`.
"""

from __future__ import annotations

import warnings

import httpx

from .exceptions import ContractVersionError, IngestionEnCours, PreconditionNonRemplie

# Timeout généreux en lecture : les flux JSONL peuvent durer (gros parc).
DEFAULT_TIMEOUT = httpx.Timeout(30.0, read=300.0)


class _BaseClient:
    """Transport partagé : URL de base, auth, timeout, gestion d'erreur, garde de version."""

    def __init__(
        self,
        url: str,
        api_key: str,
        *,
        http_client: httpx.Client | None = None,
        timeout: httpx.Timeout | None = None,
    ) -> None:
        self.url = url.rstrip("/")
        self.api_key = api_key
        self._http = http_client or httpx.Client(timeout=timeout or DEFAULT_TIMEOUT)

    # -- cycle de vie ---------------------------------------------------------

    def close(self) -> None:
        """Ferme le client HTTP sous-jacent."""
        self._http.close()

    def __enter__(self) -> _BaseClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- helpers transport ----------------------------------------------------

    @property
    def _headers(self) -> dict[str, str]:
        return {"X-API-Key": self.api_key}

    def _raise_for_status(self, response: httpx.Response) -> None:
        """Convertit les erreurs HTTP en exceptions du client via X-Error-Kind (#424).

        Priorité à l'en-tête `X-Error-Kind` pour discriminer les cas :
        - `ingestion-lock` → `IngestionEnCours` (503 verrou writer, réessayable).
        - `precondition`   → `PreconditionNonRemplie` (422, action requise côté Odoo).
        - absent           → `httpx.HTTPStatusError` (raise_for_status standard).
        """
        kind = response.headers.get("X-Error-Kind")
        if kind == "ingestion-lock":
            raise IngestionEnCours(
                "L'API electricore est momentanément indisponible : un cycle d'ingestion "
                "verrouille la base. Réessayer après le checkpoint."
            )
        if kind == "precondition":
            try:
                detail = response.json().get("detail", "Précondition métier non remplie.")
            except Exception:
                detail = "Précondition métier non remplie."
            raise PreconditionNonRemplie(detail)
        response.raise_for_status()

    @staticmethod
    def _verifier_version(*, attendue: int, servie: int) -> None:
        """Garde asymétrique sur la version de contrat (en-tête `X-Contract-Version`).

        - serveur **en avance** (servie > attendue) → `warn` : le client tolère
          l'additif (`model_config = extra="ignore"`), pas besoin d'échouer ;
        - serveur **en retard** (servie < attendue) → `ContractVersionError` : le
          client réclamerait des champs que le serveur n'émet plus.
        """
        if servie > attendue:
            warnings.warn(
                f"Le serveur sert le contrat v{servie}, le client attend v{attendue} : "
                f"des champs additifs peuvent être ignorés. Mettez le client à jour.",
                stacklevel=3,
            )
        elif servie < attendue:
            raise ContractVersionError(
                f"Le serveur sert le contrat v{servie}, le client attend v{attendue} : "
                f"trop ancien, des champs attendus sont absents. Mettez le serveur à jour."
            )
