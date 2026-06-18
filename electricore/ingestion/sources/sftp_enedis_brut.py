"""Source DLT « brut » : dépose les documents Enedis intégraux en colonne JSON (ADR-0020).

Même chaîne de mouvement que la source legacy (`sftp | decrypt | unzip`), mais le
dernier étage ne linéarise PAS : il convertit le document en dict générique
(`xml_vers_dict` pour les flux XML, `json.loads` pour R64) et le dépose tel quel dans
une table `raw_<flux>`. La linéarisation (sélection + pivot + typage) vit ensuite dans
les modèles dbt (`electricore/ingestion/dbt/`).

Une table brute par TYPE de flux (raw_r15 sert flux_r15 ET flux_r15_acc), clé primaire
`file_name` en merge : les re-livraisons Enedis (même XML dans plusieurs zips, contenus
identiques observés) sont dédoublonnées par construction — le legacy, lui, les
double-comptait.
"""

import json
import logging
from collections.abc import Iterator

import dlt

from electricore.config import runtime
from electricore.ingestion.parsing.xml import xml_vers_dict
from electricore.ingestion.sources.sftp_enedis import create_sftp_resource, mask_password_in_url
from electricore.ingestion.transformers.archive import create_unzip_transformer
from electricore.ingestion.transformers.crypto import (
    StatsDechiffrement,
    create_decrypt_transformer,
    load_aes_key_chain,
)

logger = logging.getLogger(__name__)


def _create_brut_transformer(flux_type: str, est_json: bool):
    """Transformer final : fichier extrait → document brut (colonne JSON)."""

    @dlt.transformer
    def vers_document_brut(extracted_file: dict) -> Iterator[dict]:
        contenu: bytes = extracted_file["extracted_content"]
        document = json.loads(contenu) if est_json else xml_vers_dict(contenu)
        yield {
            "file_name": extracted_file["extracted_file_name"],
            "modification_date": extracted_file["modification_date"],
            "_source_zip": extracted_file["source_zip"],
            "_flux_type": flux_type,
            "content": document,
        }

    return vers_document_brut


@dlt.source(name="flux_enedis_brut")
def flux_enedis_brut(flux_config: dict, max_files: int = None, stats: dict[str, StatsDechiffrement] | None = None):
    """Source DLT déposant le brut JSON de chaque flux dans `raw_<flux>`.

    Args:
        flux_config: Sections de flux.yaml (C15, R15, …) : file_pattern (glob SFTP),
            format (xml/json) et file_regex (fichiers à extraire des zips).
        max_files: Limitation par flux (smoke tests).
        stats: Dict mutable {flux: StatsDechiffrement} peuplé par flux (escalade per-flux,
            ADR-0037). Le caller (runner) le crée et le relit après le run pour décider
            d'un échec de job. None → dict interne jetable (callers indifférents au comptage).
    """
    sftp_url = runtime.sftp().url
    logger.info("Source URL: %s", mask_password_in_url(sftp_url))

    stats = stats if stats is not None else {}
    # Trousseau résolu une seule fois (pas par flux) — partagé entre les transformers.
    key_chain = load_aes_key_chain()
    logger.info("%d clé(s) AES chargée(s) : %s", len(key_chain), [name for name, _, _ in key_chain])

    for flux_type, config_flux in flux_config.items():
        file_pattern = config_flux["file_pattern"]
        table = f"raw_{flux_type.lower()}"

        est_json = config_flux.get("format") == "json"
        extension = ".json" if est_json else ".xml"
        file_regex = config_flux.get("file_regex", f"*{extension}")

        sftp_resource = create_sftp_resource(flux_type, table, file_pattern, sftp_url, max_files)
        stats_flux = stats.setdefault(flux_type, StatsDechiffrement())
        decrypt_transformer = create_decrypt_transformer(key_chain=key_chain, stats=stats_flux)
        unzip_transformer = create_unzip_transformer(extension, file_regex)
        brut = _create_brut_transformer(flux_type, est_json)

        pipeline_brut = (sftp_resource | decrypt_transformer | unzip_transformer | brut).with_name(table)
        pipeline_brut.apply_hints(
            primary_key="file_name",
            write_disposition="merge",
            # `json` empêche dlt d'exploser le document en tables filles.
            columns={"content": {"data_type": "json"}},
        )
        yield pipeline_brut
