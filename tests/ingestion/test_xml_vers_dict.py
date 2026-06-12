"""Convertisseur XML→dict générique (ADR-0020, issue #124).

Seam de landing XML partagé par tous les flux : transforme un document en dict
imbriqué sans aucune connaissance du flux, prêt à être landé en colonne JSON puis
linéarisé par un modèle dbt. La seule règle est structurelle.
"""

from electricore.ingestion.parsing.xml import xml_vers_dict


def test_feuille_scalaire_conteneur_toujours_liste():
    # Politique structurelle (flux-agnostique) imposée par le cast dbt aval :
    # une feuille texte → sa valeur (str), scalaire ; un élément à enfants est la
    # forme *répétable* → toujours une liste, même unique, pour que le cast
    # `struct(...)[]` soit uniforme quel que soit le nombre d'occurrences.
    xml = b"""<root>
        <a>1</a>
        <b><c>x</c><c>y</c></b>
        <d><e>z</e></d>
    </root>"""

    assert xml_vers_dict(xml) == {
        "a": "1",  # feuille unique → scalaire
        "b": [{"c": ["x", "y"]}],  # conteneur unique → liste ; feuille répétée → liste
        "d": [{"e": "z"}],  # conteneur unique → liste de 1
    }
