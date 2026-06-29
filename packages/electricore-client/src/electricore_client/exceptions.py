"""Exceptions du client electricore.

Toutes dérivent d'`ElectricoreClientError` pour qu'un appelant puisse attraper
*toute* erreur du client en une seule clause.

Contrat X-Error-Kind (#424) — les erreurs sont discriminées par l'en-tête de
réponse `X-Error-Kind`, pas par le code HTTP seul :
- `ingestion-lock` (503) → `IngestionEnCours` : verrou writer, réessayable.
- `precondition`  (422) → `PreconditionNonRemplie` : précondition métier non
  remplie, l'appelant doit agir (pas retryable).
- header absent → `httpx.HTTPStatusError` : erreur inattendue.
"""

from __future__ import annotations


class ElectricoreClientError(Exception):
    """Racine de toutes les erreurs levées par le client."""


class IngestionEnCours(ElectricoreClientError):
    """L'API a répondu 503 + X-Error-Kind: ingestion-lock : la base est verrouillée
    par un cycle d'ingestion (#424).

    Transitoire — l'API se rétablit d'elle-même après le checkpoint. L'appelant
    peut réessayer après un délai court (cf. `main.verrou_duckdb_en_503`, #171).
    """


class PreconditionNonRemplie(ElectricoreClientError):
    """L'API a répondu 422 + X-Error-Kind: precondition : une précondition métier
    n'est pas remplie (#424).

    Non retryable — l'appelant doit corriger la situation côté Odoo/données avant
    de réessayer (ex. : réconcilier les RSC avant de facturer le mois).
    Le message contient le détail actionnable fourni par le serveur.
    """


class ContractVersionError(ElectricoreClientError):
    """Le contrat servi par le serveur est plus **ancien** que celui attendu.

    Garde asymétrique : un serveur *en avance* (version > attendue) ne fait que
    `warn` (le client tolère l'additif via `extra="ignore"`) ; un serveur *en
    retard* (version < attendue) lève cette erreur — le client réclamerait des
    champs absents.
    """
