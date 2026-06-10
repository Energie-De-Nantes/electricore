"""Linéarisation des flux JSON R64 en format wide (noyau pur, sans DLT).

`parser_flux_r64` transforme les timeseries R64 en enregistrements plats
« 1 PDL × 1 date × tous les cadrans », cohérents avec R15/R151.
"""

import json
import logging
from collections.abc import Iterator
from typing import Any

from electricore.etl.parsing.tracabilite import TracabiliteFlux

logger = logging.getLogger(__name__)


def extract_header_metadata(data: dict) -> dict:
    """
    Extrait les métadonnées du header JSON R64.

    Args:
        data: Données JSON R64 complètes

    Returns:
        dict: Métadonnées du header
    """
    header = data.get("header", {})
    return {
        "id_demande": header.get("idDemande"),
        "si_demandeur": header.get("siDemandeur"),
        "code_flux": header.get("codeFlux"),
        "format": header.get("format"),
    }


def is_valid_calendrier(calendrier: dict) -> bool:
    """
    Vérifie si le calendrier est un calendrier distributeur valide.

    Args:
        calendrier: Données du calendrier

    Returns:
        bool: True si calendrier distributeur valide
    """
    id_calendrier = calendrier.get("idCalendrier")
    libelle_calendrier = calendrier.get("libelleCalendrier", "").lower()

    # Filtrer par ID calendrier ou par libellé distributeur
    valid_ids = {"DI000001", "DI000002", "DI000003"}
    return id_calendrier in valid_ids or "distributeur" in libelle_calendrier


def is_valid_data_point(point: dict) -> bool:
    """
    Vérifie si un point de données est valide (iv == 0).

    Args:
        point: Point de données avec 'd', 'v', 'iv'

    Returns:
        bool: True si point valide
    """
    return point.get("d") and point.get("v") is not None and point.get("iv") == 0


def should_process_grandeur(grandeur: dict) -> bool:
    """
    Vérifie si une grandeur doit être traitée (CONS + EA).

    Args:
        grandeur: Données de grandeur

    Returns:
        bool: True si grandeur à traiter
    """
    return grandeur.get("grandeurMetier") == "CONS" and grandeur.get("grandeurPhysique") == "EA"


def build_base_record(mesure: dict, contexte: dict, grandeur: dict, header_meta: dict) -> dict:
    """
    Construit l'enregistrement de base avec toutes les métadonnées.

    Args:
        mesure: Données de mesure (PDL)
        contexte: Données de contexte
        grandeur: Données de grandeur
        header_meta: Métadonnées du header

    Returns:
        dict: Enregistrement de base avec métadonnées
    """
    return {
        # Métadonnées de base
        "pdl": mesure.get("idPrm"),
        # Métadonnées contexte
        "etape_metier": contexte.get("etapeMetier"),
        "contexte_releve": contexte.get("contexteReleve"),
        "type_releve": contexte.get("typeReleve"),
        # Métadonnées grandeur
        "grandeur_physique": grandeur.get("grandeurPhysique"),
        "grandeur_metier": grandeur.get("grandeurMetier"),
        "unite": grandeur.get("unite"),
        # Métadonnées header
        **header_meta,
    }


def collect_timeseries_data(mesure: dict, base_record: dict) -> dict:
    """
    Collecte toutes les données de timeseries d'une mesure en format wide.

    Args:
        mesure: Données de mesure complète
        base_record: Enregistrement de base avec métadonnées

    Returns:
        dict: Données par date avec colonnes de cadrans
    """
    values_by_date = {}

    for contexte in mesure.get("contexte", []):
        for grandeur in contexte.get("grandeur", []):
            if not should_process_grandeur(grandeur):
                continue

            for calendrier in grandeur.get("calendrier", []):
                if not is_valid_calendrier(calendrier):
                    continue

                # Traiter les classes temporelles
                for classe in calendrier.get("classeTemporelle", []):
                    id_classe = classe.get("idClasseTemporelle")
                    if not id_classe:
                        continue

                    # Convention de nommage: index_{cadran}_kwh
                    cadran = id_classe.lower()
                    col_name = f"index_{cadran}_kwh"

                    # Traiter chaque point de données
                    for point in classe.get("valeur", []):
                        if not is_valid_data_point(point):
                            continue

                        date_str = point.get("d")
                        valeur = point.get("v")

                        # Initialiser l'enregistrement pour cette date
                        if date_str not in values_by_date:
                            values_by_date[date_str] = {**base_record, "date_releve": date_str}

                        # Ajouter la valeur pour cette classe
                        values_by_date[date_str][col_name] = valeur

    return values_by_date


def process_single_mesure(mesure: dict, header_meta: dict) -> Iterator[dict]:
    """
    Traite une mesure unique et génère les enregistrements wide.

    Args:
        mesure: Données d'une mesure (PDL)
        header_meta: Métadonnées du header

    Yields:
        dict: Enregistrements wide par date
    """
    pdl = mesure.get("idPrm")

    # Pour trouver le premier contexte/grandeur valide pour les métadonnées
    base_record = None

    for contexte in mesure.get("contexte", []):
        for grandeur in contexte.get("grandeur", []):
            if should_process_grandeur(grandeur):
                base_record = build_base_record(mesure, contexte, grandeur, header_meta)
                break
        if base_record:
            break

    if not base_record:
        logger.warning("Aucune grandeur CONS/EA trouvée pour PDL %s", pdl)
        return

    # Collecter toutes les données timeseries
    values_by_date = collect_timeseries_data(mesure, base_record)

    # Générer les enregistrements
    for _date_str, row_data in values_by_date.items():
        yield row_data


def r64_timeseries_to_wide_format(json_bytes: bytes, flux_type: str) -> Iterator[dict[str, Any]]:
    """
    Transforme les timeseries R64 en format WIDE pour cohérence avec R15/R151.

    Chaque enregistrement de sortie = 1 PDL + 1 date + tous les cadrans.

    Args:
        json_bytes: Contenu JSON R64 en bytes
        flux_type: Type de flux pour traçabilité

    Yields:
        dict: Enregistrements en format wide par date
    """
    try:
        # Parser JSON R64
        data = json.loads(json_bytes.decode("utf-8"))

        # Extraire métadonnées du header
        header_metadata = extract_header_metadata(data)

        # Traiter chaque mesure (PDL)
        for mesure in data.get("mesures", []):
            yield from process_single_mesure(mesure, header_metadata)

    except json.JSONDecodeError as e:
        logger.error("Erreur parsing JSON R64: %s", e)
        return
    except Exception as e:
        logger.error("Erreur transformation R64: %s", e)
        return


def parser_flux_r64(json_bytes: bytes, tracabilite: TracabiliteFlux) -> Iterator[dict]:
    """Linéarise un fichier JSON R64 en enregistrements wide enrichis de traçabilité.

    Les dicts émis sont, aux métadonnées DLT près, les lignes de la table
    `flux_r64` — c'est le seam des tests de caractérisation (#121).

    Args:
        json_bytes: Contenu du fichier JSON R64.
        tracabilite: Provenance, propagée dans chaque enregistrement.

    Yields:
        dict: Enregistrements avec colonnes de traçabilité
        (`_source_zip`, `_flux_type`, `_json_name`, `modification_date`).
    """
    for record in r64_timeseries_to_wide_format(json_bytes, tracabilite.flux_type):
        yield {
            **record,
            "modification_date": tracabilite.modification_date,
            "_source_zip": tracabilite.source_zip,
            "_flux_type": tracabilite.flux_type,
            "_json_name": tracabilite.nom_fichier,
        }
