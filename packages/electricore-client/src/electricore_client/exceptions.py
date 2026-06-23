"""Exceptions du client electricore.

Toutes dérivent d'`ElectricoreClientError` pour qu'un appelant puisse attraper
*toute* erreur du client en une seule clause. `IngestionEnCours` est le cas
métier distingué : la base DuckDB est verrouillée par un cycle d'ingestion
(l'API répond **503**), et l'appelant peut simplement réessayer plus tard.
"""

from __future__ import annotations


class ElectricoreClientError(Exception):
    """Racine de toutes les erreurs levées par le client."""


class IngestionEnCours(ElectricoreClientError):
    """L'API a répondu 503 : la base est verrouillée par un cycle d'ingestion.

    Transitoire — l'API se rétablit d'elle-même après le checkpoint. L'appelant
    peut réessayer après un délai court (cf. `main.verrou_duckdb_en_503`, #171).
    """


class ContractVersionError(ElectricoreClientError):
    """Le contrat servi par le serveur est plus **ancien** que celui attendu.

    Garde asymétrique : un serveur *en avance* (version > attendue) ne fait que
    `warn` (le client tolère l'additif via `extra="ignore"`) ; un serveur *en
    retard* (version < attendue) lève cette erreur — le client réclamerait des
    champs absents.
    """
