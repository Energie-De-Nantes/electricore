"""Brique SFTP partagée : listing incrémental des fichiers Enedis.

La source de production est `sftp_enedis_brut.flux_enedis_brut` (landing brut,
ADR-0020) ; ce module ne porte plus que la resource de mouvement réutilisable
(`create_sftp_resource`) et l'aide de log `mask_password_in_url`.
"""

import logging
import re

import dlt
from dlt.sources.filesystem import filesystem

logger = logging.getLogger(__name__)

# Namespace d'état incrémental stable (issue #346).
#
# Le curseur incrémental vit sur la resource `filesystem` interne, qui n'est PAS
# liée à notre `@dlt.source`. Sa clé d'état (`source_state_key`) se résout sinon
# différemment selon le nombre de flux du run : mono-flux → nom de la source,
# multi-flux → nom dérivé du pipeline. Deux namespaces → un `ingestion <flux>` seul
# repart d'un curseur vide et RE-TÉLÉCHARGE tout le flux. On épingle donc le
# `source_name` de la resource filesystem sur cette constante : l'état atterrit
# toujours au même endroit. C'est aussi le nom de la source (`flux_enedis_brut`),
# pour que mono- et multi-flux partagent un seul namespace.
ESPACE_ETAT_INCREMENTAL = "flux_enedis_brut"


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

        # Épingle le namespace d'état : sans ça, dlt résout la clé d'état de cette
        # resource selon le nombre de flux du run (mono vs multi) → curseurs orphelins
        # et re-téléchargement complet (#346). `source_name` est le 1er terme de la
        # résolution de `source_state_key` (dlt resource.py).
        files.source_name = ESPACE_ETAT_INCREMENTAL

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
