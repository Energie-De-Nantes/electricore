"""Conversion XML → dict générique (brique du landing brut, ADR-0020).

`xml_vers_dict` est l'unique étage Python entre le fichier Enedis et la colonne
JSON : il convertit n'importe quel XML en dict, sans connaissance du flux. La
sélection, le pivot et le typage vivent dans les modèles dbt
(`electricore/ingestion/dbt/models/`).
"""

from typing import Any

from lxml import etree


def xml_vers_dict(xml_bytes: bytes) -> dict:
    """Convertit un document XML en dict imbriqué, sans connaissance du flux.

    Politique structurelle « conteneur = liste » : un élément à enfants est la
    forme répétable → toujours un tableau, même unique (le cast/unnest dbt aval
    est uniforme, les chemins `[0]` sont stables) ; une feuille texte devient sa
    valeur (str) ; une feuille répétée devient une liste.

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
        # Feuille : sa valeur texte (None si vide, pour rester sérialisable). Les
        # attributs d'une feuille sont ignorés — aucun flux n'en porte de signifiants,
        # et renvoyer un dict casserait le type scalaire attendu par l'aval element-only.
        return element.text.strip() if element.text and element.text.strip() else None
    # Conteneur : ses attributs sous une clé préfixée `@` (X12/X13 portent l'id
    # d'affaire et les codes statut/objet/état en attributs). Le préfixe évite toute
    # collision avec un tag enfant et reste sélectionnable en JSON path (`."@code"`).
    resultat: dict[str, Any] = {f"@{cle}": val for cle, val in element.attrib.items()}
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
