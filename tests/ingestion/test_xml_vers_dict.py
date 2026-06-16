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


def test_attributs_de_conteneur_captures_avec_prefixe():
    # X12/X13 portent leurs données dans des *attributs* XML (id d'affaire, codes
    # statut/objet/état) — les autres flux sont element-only, d'où l'ignorance
    # historique des attributs. Le convertisseur expose désormais les attributs d'un
    # *conteneur* sous une clé préfixée `@`, à côté des enfants, pour que le modèle dbt
    # les sélectionne (`$.statut[0]."@code"`). Les feuilles restent scalaires (zéro
    # régression : aucun flux element-only ne change).
    xml = b"""<root>
        <affaire id="G08TJ7VC">
            <statut code="TERMN"><libelle>Termine</libelle></statut>
        </affaire>
    </root>"""

    assert xml_vers_dict(xml) == {
        "affaire": [
            {
                "@id": "G08TJ7VC",
                "statut": [{"@code": "TERMN", "libelle": "Termine"}],
            }
        ]
    }


def test_feuille_porteuse_dattribut_devient_un_noeud_liste():
    # Observé en prod X12 : <statut code="COURS"/> SANS enfant libelle. Une feuille
    # porteuse d'attribut n'est pas un scalaire — l'attribut est de la donnée. Elle
    # devient un nœud `[{"@code": ...}]`, MÊME forme que le conteneur, pour que le modèle
    # dbt y accède uniformément (`statut[0]."@code"`) qu'il y ait un libelle ou non.
    # Une feuille SANS attribut reste scalaire (zéro régression element-only).
    assert xml_vers_dict(b'<root><statut code="COURS"/><vide/><texte>x</texte></root>') == {
        "statut": [{"@code": "COURS"}],  # feuille à attribut → nœud listé
        "vide": None,  # feuille sans attribut ni texte → scalaire None
        "texte": "x",  # feuille texte sans attribut → scalaire
    }


def test_commentaires_xml_ignores():
    # lxml expose un commentaire comme un nœud dont `.tag` est une fonction (pas une
    # chaîne) — utilisé tel quel en clé de dict, il casse le landing JSON. On l'ignore.
    assert xml_vers_dict(b"<root><!-- note --><a>1</a></root>") == {"a": "1"}
