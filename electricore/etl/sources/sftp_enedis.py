"""
Source DLT pour les flux Enedis via SFTP avec architecture modulaire.
Utilise le chaînage de transformers DLT pour une architecture propre.
"""

import logging
import os
import re

import dlt
from dlt.sources.filesystem import filesystem

logger = logging.getLogger(__name__)


def mask_password_in_url(url: str) -> str:
    """
    Masque le mot de passe dans une URL source pour les logs.

    Gère les URLs avec authentification (SFTP, FTP, ...) ainsi que les URLs
    locales sans authentification (file://) — ces dernières sont retournées
    inchangées.

    Args:
        url: URL source (sftp://, file://, ...)

    Returns:
        URL avec mot de passe masqué le cas échéant

    Examples:
        >>> mask_password_in_url("sftp://user:pass@host:22/path")
        "sftp://user:****@host:22/path"
        >>> mask_password_in_url("file:///var/enedis/")
        "file:///var/enedis/"
    """
    # Capture: protocole://user:password@host (uniquement si user:password présent)
    pattern = r"([a-z][a-z0-9+.-]*://[^:/@]+:)[^@]+(@.+)"
    return re.sub(pattern, r"\1****\2", url)


# Imports des transformers modulaires
from electricore.etl.parsing import ConfigFluxXml
from electricore.etl.transformers.archive import create_unzip_transformer
from electricore.etl.transformers.crypto import create_decrypt_transformer
from electricore.etl.transformers.parsers import (
    create_json_r64_transformer,
    create_xml_parser_transformer,
)


def create_sftp_resource(flux_type: str, table_name: str, file_pattern: str, sftp_url: str, max_files: int = None):
    """
    Crée une resource SFTP réutilisable avec limitation optionnelle.

    Args:
        flux_type: Type de flux (R15, C15, etc.)
        table_name: Nom de la table cible (pour un état incrémental unique)
        file_pattern: Pattern pour les fichiers (ZIP ou JSON directs)
        sftp_url: URL du serveur SFTP
        max_files: Nombre max de fichiers à traiter
    """

    @dlt.resource(
        name=f"sftp_files_{table_name}",  # Nom unique par table pour état incrémental indépendant
        write_disposition="append",
    )
    def sftp_files_resource():

        files = filesystem(bucket_url=sftp_url, file_glob=file_pattern).with_name(
            f"filesystem_{table_name}"
        )  # Nom unique pour état incrémental indépendant

        # Appliquer l'incrémental sur la date de modification.
        # modification_date est un pendulum.DateTime dans le filesystem source DLT,
        # mais DLT stocke last_value comme string ISO dans son state.
        # La comparaison str > DateTime échoue → on normalise en ISO string via add_map
        # pour que DLT compare str >= str (les dates ISO sont triables lexicographiquement).
        # IMPORTANT : on mute l'item en place pour conserver le type FileItemDict
        # (et donc la méthode .open() nécessaire au déchiffrement).
        def _normalize_date(item):
            if hasattr(item.get("modification_date"), "isoformat"):
                item["modification_date"] = item["modification_date"].isoformat()
            return item

        files.add_map(_normalize_date)

        files.apply_hints(incremental=dlt.sources.incremental("modification_date"))

        # Limiter le nombre de fichiers si spécifié
        file_count = 0
        for file_item in files:
            if max_files and file_count >= max_files:
                break
            file_count += 1
            yield file_item

    return sftp_files_resource


@dlt.source(name="flux_enedis")
def flux_enedis(flux_config: dict, max_files: int = None):
    """
    Source DLT refactorée avec architecture modulaire pour tous les flux Enedis.

    Architecture unifiée :
    - XML: SFTP → Decrypt → Unzip → XML Parse → Table
    - CSV: SFTP → Decrypt → Unzip → CSV Parse → Table

    Args:
        flux_config: Configuration des flux depuis config/settings.py
    """
    # Configuration SFTP : env var SFTP__URL (chargée depuis .env) ou secrets.toml [sftp] url
    sftp_url = os.environ.get("SFTP__URL") or dlt.secrets["sftp"]["url"]

    logger.info("Source URL: %s", mask_password_in_url(sftp_url))

    # Créer les transformers communs une seule fois (optimisation)
    decrypt_transformer = create_decrypt_transformer()

    # Traiter chaque type de flux
    for flux_type, flux_config_data in flux_config.items():
        file_pattern = flux_config_data["file_pattern"]

        # === FLUX XML ===
        if "xml_configs" in flux_config_data:
            xml_configs = flux_config_data["xml_configs"]

            for xml_config in xml_configs:
                table_name = xml_config["name"]
                file_regex = xml_config.get("file_regex", "*.xml")

                # 1. Resource SFTP
                sftp_resource = create_sftp_resource(flux_type, table_name, file_pattern, sftp_url, max_files)

                # 2. Transformer unzip configuré pour ce flux
                unzip_transformer = create_unzip_transformer(".xml", file_regex)

                # 3. Transformer XML parser configuré (typos YAML détectées ici)
                xml_parser = create_xml_parser_transformer(
                    config=ConfigFluxXml.depuis_yaml(xml_config),
                    flux_type=flux_type,
                )

                # 4. 🎯 CHAÎNAGE MODULAIRE
                xml_pipeline = (sftp_resource | decrypt_transformer | unzip_transformer | xml_parser).with_name(
                    table_name
                )

                xml_pipeline.apply_hints(write_disposition="append")
                yield xml_pipeline

        # === FLUX JSON (R64 uniquement — pas de linéarisation JSON générique, cf. #121) ===
        if "json_configs" in flux_config_data:
            for json_config in flux_config_data["json_configs"]:
                table_name = json_config["name"]
                file_regex = json_config.get("file_regex", "*.json")
                transformer_type = json_config.get("transformer_type", "standard")
                primary_key = json_config.get("primary_key", [])

                if transformer_type != "r64_timeseries":
                    raise ValueError(
                        f"transformer_type {transformer_type!r} non supporté pour {table_name} "
                        "(seul 'r64_timeseries' existe — le parser JSON générique a été retiré, cf. #121)"
                    )

                sftp_resource = create_sftp_resource(flux_type, table_name, file_pattern, sftp_url, max_files)
                unzip_transformer = create_unzip_transformer(".json", file_regex)
                json_parser = create_json_r64_transformer(flux_type=flux_type)

                json_pipeline = (sftp_resource | decrypt_transformer | unzip_transformer | json_parser).with_name(
                    table_name
                )

                if primary_key:
                    json_pipeline.apply_hints(primary_key=primary_key, write_disposition="merge")
                else:
                    json_pipeline.apply_hints(write_disposition="append")

                yield json_pipeline
