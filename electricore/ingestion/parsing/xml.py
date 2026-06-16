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
    texte = element.text.strip() if element.text and element.text.strip() else None
    if not enfants:
        # Feuille SANS attribut : sa valeur texte (None si vide), scalaire — zéro
        # régression pour les flux element-only.
        if not element.attrib:
            return texte
        # Feuille PORTEUSE d'attribut (ex X12 : <statut code="COURS"/> sans libelle) :
        # l'attribut est de la donnée → nœud `{"@attr": …}` (+ `#text` si texte présent),
        # jamais un scalaire qui perdrait l'attribut.
        noeud = {f"@{cle}": val for cle, val in element.attrib.items()}
        if texte is not None:
            noeud["#text"] = texte
        return noeud
    # Conteneur : ses attributs sous une clé préfixée `@` (X12/X13 portent l'id
    # d'affaire et les codes statut/objet/état en attributs). Le préfixe évite toute
    # collision avec un tag enfant et reste sélectionnable en JSON path (`."@code"`).
    resultat: dict[str, Any] = {f"@{cle}": val for cle, val in element.attrib.items()}
    for enfant in enfants:
        # Commentaires / instructions de traitement : lxml en fait des nœuds dont `.tag`
        # est une fonction (etree.Comment/PI), pas une chaîne — on les ignore (sinon clé
        # de dict non-sérialisable au landing JSON).
        if not isinstance(enfant.tag, str):
            continue
        valeur = _element_vers_dict(enfant)
        if len(enfant) > 0 or enfant.attrib:
            # Forme répétable — enfants OU attributs → toujours une liste (cast/unnest
            # dbt uniforme). Un <statut code=…/> (feuille à attribut) est ainsi listé
            # comme un <statut code=…><libelle/></statut> : `statut[0]."@code"` marche
            # dans les deux cas.
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
