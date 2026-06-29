"""
Transformer DLT pour l'extraction de fichiers depuis des archives ZIP.
Inclut les fonctions pures d'extraction et le transformer DLT.
"""

import io
import logging
import zipfile
from collections.abc import Iterator

import dlt

from electricore.ingestion.transformers.chaine import StatsChaine, etape_chaine

logger = logging.getLogger(__name__)


# =============================================================================
# FONCTIONS PURES D'EXTRACTION ZIP
# =============================================================================


def extract_files_from_zip(zip_data: bytes, file_extension: str = ".xml") -> list[tuple[str, bytes]]:
    """
    Extrait les fichiers d'une extension donnée d'un ZIP.

    Args:
        zip_data: Contenu du fichier ZIP
        file_extension: Extension des fichiers à extraire (ex: '.xml', '.csv')

    Returns:
        List[Tuple[str, bytes]]: Liste de (nom_fichier, contenu)

    Raises:
        zipfile.BadZipFile: Si le ZIP est corrompu
    """
    files = []

    with zipfile.ZipFile(io.BytesIO(zip_data), "r") as zip_ref:
        for file_info in zip_ref.filelist:
            if file_info.filename.lower().endswith(file_extension.lower()) and not file_info.is_dir():
                try:
                    content = zip_ref.read(file_info.filename)
                    files.append((file_info.filename, content))
                except Exception as e:
                    logger.warning("Erreur lecture %s: %s", file_info.filename, e)
                    continue

    return files


import fnmatch


def match_xml_pattern(nom_fichier: str, pattern: str | None) -> bool:
    """Vrai si le nom de fichier correspond au pattern glob (fnmatch).

    Tous les `file_regex` de flux.yaml sont des globs (`*`/`?`/`[…]`) → fnmatch seul.
    """
    return fnmatch.fnmatch(nom_fichier, pattern) if pattern else True


# =============================================================================
# TRANSFORMER DLT
# =============================================================================


@etape_chaine(
    succes="extraits",
    echec="echecs_extraction",
    libelle="extraction",
    cle_item="file_name",
    niveau_log=logging.ERROR,
)
def _unzip(decrypted_file: dict, file_extension: str, file_regex: str | None) -> Iterator[dict]:
    """Étage unzip : ZIP déchiffré → fichiers internes (un par yield).

    Ne déclare que **son travail** (ouvrir le ZIP, filtrer par regex, émettre chaque fichier
    interne) : un ZIP illisible **lève** → la discipline le compte `echecs_extraction` au niveau
    ERROR et le saute (pas de poison-pill). Le combinateur compte **chaque yield** (`extraits`) —
    un N-yields, contrairement aux étages 1-yield. Un fichier filtré par `file_regex` est sauté
    AVANT le yield → ni succès ni échec. Un ZIP vide par nature (0 fichier interne, sans lever)
    ne compte aucun échec : seul le `warning` « Aucun fichier… » le signale.

    Yields:
        dict: {
            'source_zip': str,
            'modification_date': datetime,
            'extracted_file_name': str,
            'extracted_content': bytes,
            'file_size': int
        }
    """
    zip_name = decrypted_file["file_name"]
    zip_modified = decrypted_file["modification_date"]
    decrypted_content = decrypted_file["decrypted_content"]

    # Extraire les fichiers de l'extension souhaitée — un ZIP corrompu lève → échec compté.
    extracted_files = extract_files_from_zip(decrypted_content, file_extension)

    for file_name, file_content in extracted_files:
        # Filtrer par regex si spécifié : un fichier filtré est sauté avant le yield (ni
        # succès ni échec, invisible aux stats).
        if file_regex and not match_xml_pattern(file_name, file_regex):
            continue

        yield {
            "source_zip": zip_name,
            "modification_date": zip_modified,
            "extracted_file_name": file_name,
            "extracted_content": file_content,
            "file_size": len(file_content),
        }

    if not extracted_files:
        # Vide par nature (0 fichier interne) : pas un échec — le prédicat aveugle s'appuie
        # sur l'absence d'échec pour ne PAS escalader (R64 de l'ère CSV, smoke max_files).
        logger.warning("Aucun fichier %s trouvé dans %s", file_extension, zip_name)


def create_unzip_transformer(
    file_extension: str = ".xml", file_regex: str | None = None, *, stats: StatsChaine | None = None
):
    """
    Factory pour créer un transformer d'extraction ZIP configuré.

    Args:
        file_extension: Extension des fichiers à extraire
        file_regex: Pattern optionnel pour filtrer les noms
        stats: Compteur de chaîne du flux courant (escalade per-flux, ADR-0037 étendu) —
            partagé avec les étages decrypt et parse du même flux

    Returns:
        Transformer DLT configuré
    """
    stats = stats if stats is not None else StatsChaine()

    @dlt.transformer
    def configured_unzip_transformer(decrypted_file: dict) -> Iterator[dict]:
        return _unzip(decrypted_file, file_extension, file_regex, stats)  # stats : dernier positionnel

    return configured_unzip_transformer
