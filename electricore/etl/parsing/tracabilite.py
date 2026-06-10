"""Métadonnées de traçabilité accompagnant chaque enregistrement linéarisé."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TracabiliteFlux:
    """Provenance d'un fichier flux, propagée dans chaque enregistrement.

    Les enregistrements émis portent ces valeurs sous les clés `_source_zip`,
    `_flux_type`, `modification_date` et `_xml_name` / `_json_name` selon le
    format — ces colonnes font partie du contrat des tables DuckDB.

    `modification_date` est une string ISO (normalisée par la resource SFTP,
    cf. `create_sftp_resource`) ou un datetime selon la source.
    """

    source_zip: str
    nom_fichier: str
    flux_type: str
    modification_date: object
