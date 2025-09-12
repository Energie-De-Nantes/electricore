"""
Resource DLT de base pour les sources SFTP avec incrémental.
Fournit une resource réutilisable pour tous les flux Enedis.
"""

import dlt
from dlt.sources.filesystem import filesystem
from typing import Iterator
from dlt.common.storages.fsspec_filesystem import FileItemDict


@dlt.resource(write_disposition="append")
def sftp_files_resource(
    sftp_url: str,
    file_pattern: str,
    resource_name: str = "sftp_files"
) -> Iterator[FileItemDict]:
    """
    Resource DLT réutilisable pour récupérer des fichiers depuis SFTP.
    
    Args:
        sftp_url: URL de connexion SFTP
        file_pattern: Pattern de fichiers (ex: "**/*_R151_*.zip")
        resource_name: Nom unique pour la resource (pour éviter conflits d'état)
    
    Yields:
        FileItemDict: Métadonnées des fichiers SFTP avec incrémental
    """
    # Nom unique pour éviter les conflits d'état incrémental
    filesystem_name = f"filesystem_{resource_name}"
    
    print(f"🔍 SFTP Resource {resource_name}: {sftp_url}")
    print(f"📁 Pattern: {file_pattern}")
    
    # Source filesystem DLT avec incrémental automatique
    encrypted_files = filesystem(
        bucket_url=sftp_url,
        file_glob=file_pattern
    ).with_name(filesystem_name)
    
    # Appliquer l'incrémental sur la date de modification
    encrypted_files.apply_hints(
        incremental=dlt.sources.incremental("modification_date")
    )
    
    # Yield les fichiers avec leur métadonnées
    for file_item in encrypted_files:
        yield file_item


def create_sftp_resource(
    flux_type: str,
    zip_pattern: str,
    sftp_url: str
) -> Iterator[FileItemDict]:
    """
    Factory pour créer une resource SFTP spécifique à un flux.
    
    Args:
        flux_type: Type de flux (R151, C15, etc.)
        zip_pattern: Pattern des fichiers ZIP
        sftp_url: URL SFTP
    
    Returns:
        Resource DLT configurée pour le flux
    """
    resource_name = f"sftp_{flux_type.lower()}"
    
    return sftp_files_resource(
        sftp_url=sftp_url,
        file_pattern=zip_pattern,
        resource_name=resource_name
    )