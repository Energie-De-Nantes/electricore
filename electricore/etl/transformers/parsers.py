"""Adapters DLT autour du noyau pur de linéarisation (`etl/parsing/`, issue #121).

Chaque transformer est mince : il déballe l'item DLT (shape `extracted_file`
du transformer archive, ou fichier déchiffré direct), construit la
`TracabiliteFlux`, délègue au noyau pur, et porte la politique d'erreur
d'ingestion (log + skip — le fichier sera re-tenté au prochain run DLT).
"""

import logging
from collections.abc import Iterator

import dlt

from electricore.etl.parsing import ConfigFluxXml, TracabiliteFlux, parser_flux_r64, parser_flux_xml

logger = logging.getLogger(__name__)


def create_xml_parser_transformer(config: ConfigFluxXml, flux_type: str = "unknown"):
    """
    Factory pour créer un transformer de linéarisation XML configuré.

    Args:
        config: Contrat de sélection (cf. `ConfigFluxXml.depuis_yaml`)
        flux_type: Type de flux pour traçabilité

    Returns:
        Transformer DLT configuré
    """

    @dlt.transformer
    def configured_xml_parser(extracted_file: dict) -> Iterator[dict]:
        tracabilite = TracabiliteFlux(
            source_zip=extracted_file["source_zip"],
            nom_fichier=extracted_file["extracted_file_name"],
            flux_type=flux_type,
            modification_date=extracted_file["modification_date"],
        )
        try:
            yield from parser_flux_xml(extracted_file["extracted_content"], config, tracabilite)
        except Exception as e:
            logger.error("Erreur parsing XML %s: %s", tracabilite.nom_fichier, e)
            return

    return configured_xml_parser


def create_json_r64_transformer(flux_type: str = "R64"):
    """
    Factory pour créer un transformer R64 spécialisé avec format WIDE.

    Args:
        flux_type: Type de flux (R64)

    Returns:
        Transformer DLT configuré pour R64
    """

    @dlt.transformer
    def configured_r64_parser(extracted_file: dict) -> Iterator[dict]:
        # Deux shapes possibles : JSON extrait d'un ZIP (transformer archive)
        # ou JSON déchiffré direct du SFTP (transformer crypto).
        if "source_zip" in extracted_file:
            tracabilite = TracabiliteFlux(
                source_zip=extracted_file["source_zip"],
                nom_fichier=extracted_file["extracted_file_name"],
                flux_type=flux_type,
                modification_date=extracted_file["modification_date"],
            )
            json_content = extracted_file["extracted_content"]
        else:
            tracabilite = TracabiliteFlux(
                source_zip=extracted_file["file_name"],
                nom_fichier=extracted_file["file_name"],
                flux_type=flux_type,
                modification_date=extracted_file["modification_date"],
            )
            json_content = extracted_file["decrypted_content"]

        try:
            yield from parser_flux_r64(json_content, tracabilite)
        except Exception as e:
            logger.error("Erreur parsing JSON R64 %s: %s", tracabilite.nom_fichier, e)
            return

    return configured_r64_parser
