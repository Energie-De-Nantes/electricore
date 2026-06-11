"""Linéarisation sélective des flux XML Enedis (noyau pur, sans DLT).

`parser_flux_xml` transforme les bytes d'un fichier XML en enregistrements
plats selon une `ConfigFluxXml` (le contrat de sélection, chargé depuis
`flux.yaml`) — cf. `etl/CONTEXT.md`, entrées *Linéarisation sélective* et
*Configuration de flux*.
"""

import fnmatch
import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from lxml import etree

from electricore.etl.parsing.tracabilite import TracabiliteFlux

logger = logging.getLogger(__name__)

# Clés autorisées d'une entrée xml_configs de flux.yaml. Les clés hors
# sélection (name, file_regex, primary_key) sont le câblage DLT, consommées
# par sources/sftp_enedis.py.
_CLES_YAML_AUTORISEES = frozenset(
    {"name", "file_regex", "row_level", "metadata_fields", "data_fields", "nested_fields", "primary_key"}
)


@dataclass(frozen=True, slots=True)
class ConfigFluxXml:
    """Contrat de sélection d'une table cible : quoi extraire du XML.

    Attributes:
        row_level: XPath des éléments devenant chacun un enregistrement.
        metadata_fields: champ → XPath absolu, extrait une fois par fichier.
        data_fields: champ → XPath relatif à la ligne.
        nested_fields: extractions conditionnelles (cadrans avant_/apres_…).
    """

    row_level: str
    metadata_fields: dict[str, str] = field(default_factory=dict)
    data_fields: dict[str, str] = field(default_factory=dict)
    nested_fields: tuple[dict[str, Any], ...] = ()

    @classmethod
    def depuis_yaml(cls, entry: dict) -> "ConfigFluxXml":
        """Construit la config depuis une entrée `xml_configs` de flux.yaml.

        Les typos de clés explosent ici, au chargement, plutôt que par une
        extraction silencieusement vide à l'exécution.
        """
        inconnues = set(entry) - _CLES_YAML_AUTORISEES
        if inconnues:
            raise ValueError(
                f"Clés inconnues dans la configuration de flux {entry.get('name', '?')!r} : "
                f"{sorted(inconnues)} (autorisées : {sorted(_CLES_YAML_AUTORISEES)})"
            )
        if "row_level" not in entry:
            raise ValueError(f"Configuration de flux {entry.get('name', '?')!r} : row_level manquant")
        return cls(
            row_level=entry["row_level"],
            metadata_fields=entry.get("metadata_fields") or {},
            data_fields=entry.get("data_fields") or {},
            nested_fields=tuple(entry.get("nested_fields") or ()),
        )


def xml_vers_dict(xml_bytes: bytes) -> dict:
    """Convertit un document XML en dict imbriqué, sans connaissance du flux.

    Brique de landing partagée par tous les flux XML (ADR-0020) : le dict est
    landé en colonne JSON, puis linéarisé par un modèle dbt. La seule règle est
    structurelle — des balises sœurs de même nom deviennent une liste, une balise
    unique reste scalaire, une feuille texte devient sa valeur (str).

    Args:
        xml_bytes: Contenu du fichier XML.

    Returns:
        Le contenu de l'élément racine en dict (les enfants de la racine,
        la balise racine elle-même n'étant pas ré-emballée).
    """
    return _element_vers_dict(etree.fromstring(xml_bytes))


def _element_vers_dict(element: Any) -> Any:
    enfants = list(element)
    if not enfants:
        # Feuille : sa valeur texte (None si vide, pour rester sérialisable).
        return element.text.strip() if element.text and element.text.strip() else None
    resultat: dict[str, Any] = {}
    for enfant in enfants:
        valeur = _element_vers_dict(enfant)
        if len(enfant) > 0:
            # Conteneur = forme répétable → toujours une liste (cast dbt uniforme),
            # même unique.
            resultat.setdefault(enfant.tag, []).append(valeur)
        elif enfant.tag in resultat:
            # Feuille répétée → liste.
            existant = resultat[enfant.tag]
            if isinstance(existant, list):
                existant.append(valeur)
            else:
                resultat[enfant.tag] = [existant, valeur]
        else:
            resultat[enfant.tag] = valeur
    return resultat


def match_xml_pattern(xml_name: str, pattern: str | None) -> bool:
    """
    Vérifie si un nom de fichier correspond au pattern (wildcard ou regex).

    Args:
        xml_name: Nom du fichier
        pattern: Pattern wildcard (*,?) ou regex, ou None

    Returns:
        bool: True si le fichier match (ou si pas de pattern)
    """
    if pattern is None:
        return True  # Pas de pattern = accepte tout

    try:
        # Si le pattern contient des wildcards (* ou ?), utiliser fnmatch
        if "*" in pattern or "?" in pattern:
            return fnmatch.fnmatch(xml_name, pattern)
        else:
            # Sinon, traiter comme regex
            return bool(re.search(pattern, xml_name))
    except re.error:
        # En cas d'erreur regex, essayer comme wildcard en fallback
        try:
            return fnmatch.fnmatch(xml_name, pattern)
        except Exception:
            logger.warning("Pattern invalide '%s' pour %s", pattern, xml_name)
            return False


def _xml_to_dict_from_bytes(
    xml_bytes: bytes, row_level: str, metadata_fields: dict = None, data_fields: dict = None, nested_fields: list = None
) -> Iterator[dict]:
    """
    Version lxml de xml_to_dict qui parse directement des bytes - SANS écriture disque.

    Args:
        xml_bytes: Contenu XML en bytes
        row_level: XPath pour les lignes
        metadata_fields: Champs de métadonnées
        data_fields: Champs de données
        nested_fields: Champs imbriqués

    Yields:
        dict: Enregistrements extraits
    """
    # Initialiser les paramètres par défaut
    metadata_fields = metadata_fields or {}
    data_fields = data_fields or {}
    nested_fields = nested_fields or []

    # Parser directement depuis bytes avec lxml - très efficace !
    root = etree.fromstring(xml_bytes)

    # Extraire les métadonnées une seule fois avec XPath
    meta: dict[str, str] = {}
    for field_name, field_xpath in metadata_fields.items():
        elements = root.xpath(field_xpath)
        if elements and hasattr(elements[0], "text") and elements[0].text:
            meta[field_name] = elements[0].text

    # Parcourir chaque ligne avec XPath (plus puissant qu'ElementTree)
    for row in root.xpath(row_level):
        # Extraire les champs de données principaux avec XPath relatif
        row_data: dict[str, Any] = {}

        for field_name, field_xpath in data_fields.items():
            elements = row.xpath(field_xpath)
            if elements and hasattr(elements[0], "text") and elements[0].text:
                row_data[field_name] = elements[0].text

        # Extraire les champs imbriqués avec conditions (logique identique à xml_to_dict)
        for nested in nested_fields:
            prefix = nested.get("prefix", "")
            child_path = nested["child_path"]
            id_field = nested["id_field"]
            value_field = nested["value_field"]
            conditions = nested.get("conditions", [])
            additional_fields = nested.get("additional_fields", {})

            # Parcourir les éléments enfants avec XPath
            for nr in row.xpath(child_path):
                # Vérifier toutes les conditions
                all_conditions_met = True

                for cond in conditions:
                    cond_xpath = cond["xpath"]
                    cond_value = cond["value"]
                    cond_elements = nr.xpath(cond_xpath)

                    if (
                        not cond_elements
                        or not hasattr(cond_elements[0], "text")
                        or cond_elements[0].text != cond_value
                    ):
                        all_conditions_met = False
                        break

                # Si toutes les conditions sont remplies
                if all_conditions_met:
                    key_elements = nr.xpath(id_field)
                    value_elements = nr.xpath(value_field)

                    if (
                        key_elements
                        and value_elements
                        and hasattr(key_elements[0], "text")
                        and hasattr(value_elements[0], "text")
                        and key_elements[0].text
                        and value_elements[0].text
                    ):
                        # Ajouter la valeur principale avec préfixe et convention de nommage
                        # Format: {prefix}index_{cadran}_kwh (ex: "avant_index_hp_kwh", "index_base_kwh")
                        cadran = key_elements[0].text.lower()  # HP → hp, BASE → base
                        field_key = f"{prefix}index_{cadran}_kwh"
                        row_data[field_key] = value_elements[0].text

                        # Ajouter les champs additionnels
                        for add_field_name, add_field_xpath in additional_fields.items():
                            add_field_key = f"{prefix}{add_field_name}"

                            # Éviter d'écraser si déjà présent
                            if add_field_key not in row_data:
                                add_elements = nr.xpath(add_field_xpath)
                                if add_elements and hasattr(add_elements[0], "text") and add_elements[0].text:
                                    row_data[add_field_key] = add_elements[0].text

        # Fusionner métadonnées et données de ligne
        final_record = {**row_data, **meta}

        yield final_record


def parser_flux_xml(
    xml_bytes: bytes,
    config: ConfigFluxXml,
    tracabilite: TracabiliteFlux,
) -> Iterator[dict]:
    """Linéarise un fichier XML en enregistrements plats enrichis de traçabilité.

    Les dicts émis sont, aux métadonnées DLT près, les lignes de la table
    DuckDB cible — c'est le seam des tests de caractérisation (#121).

    Args:
        xml_bytes: Contenu du fichier XML.
        config: Contrat de sélection (cf. flux.yaml).
        tracabilite: Provenance, propagée dans chaque enregistrement.

    Yields:
        dict: Enregistrements avec colonnes de traçabilité
        (`_source_zip`, `_flux_type`, `_xml_name`, `modification_date`).
    """
    for record in _xml_to_dict_from_bytes(
        xml_bytes,
        row_level=config.row_level,
        metadata_fields=config.metadata_fields,
        data_fields=config.data_fields,
        nested_fields=list(config.nested_fields),
    ):
        yield {
            **record,
            "_source_zip": tracabilite.source_zip,
            "_flux_type": tracabilite.flux_type,
            "_xml_name": tracabilite.nom_fichier,
            "modification_date": tracabilite.modification_date,
        }
