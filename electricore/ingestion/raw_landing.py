"""Landing brut : dépose un document Enedis intégral en colonne JSON DuckDB (ADR-0020).

Fork α de #123 : dlt garde `decrypt | unzip` puis dépose une ligne par
document, contenu préservé en colonne JSON (pas d'explosion en tables filles).
La linéarisation (sélection + pivot) vit ensuite dans les modèles dbt, qui
récupèrent les structs via un cast staging.

Primitive réutilisable :
- par les tests (injection d'une fixture déchiffrée),
- par le câblage production (#4 : sortie de la chaîne SFTP→decrypt→unzip),
- par le chemin XML (#124 : document XML converti en dict avant landing).
"""

from collections.abc import Iterable, Mapping
from typing import Any

import dlt


def lander_documents_bruts(
    pipeline: dlt.Pipeline,
    table: str,
    documents: Iterable[Mapping[str, Any]],
) -> Any:
    """Lande des documents bruts dans une table DuckDB, contenu en colonne JSON.

    Args:
        pipeline: pipeline dlt configuré (destination DuckDB, dataset = schéma brut).
        table: nom de la table cible (ex. `raw_r64`).
        documents: itérable de dicts portant au minimum
            `file_name` (str), `modification_date` (str ISO ou datetime),
            `content` (dict — le document parsé, déposé tel quel en JSON).

    Returns:
        Le `LoadInfo` dlt du run.
    """

    @dlt.resource(
        name=table,
        # `json` empêche dlt d'exploser le document en tables filles :
        # une ligne par fichier, document intégral préservé.
        columns={"content": {"data_type": "json"}},
        write_disposition="merge",
        primary_key="file_name",
    )
    def _documents() -> Any:
        yield from documents

    return pipeline.run(_documents())
