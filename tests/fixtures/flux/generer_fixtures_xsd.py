"""Génère des fixtures XML schéma-valides depuis les XSD Enedis.

Filet d'ingestion : chaque fixture est une instance **maximale** (tous les champs
optionnels présents, occurrences multiples là où la politique le permet) construite
en marchant le XSD, puis **validée** contre lui. Les golden sont ensuite produits par
le parser legacy (`generer_golden.py`) et figés — le chemin dbt doit les reproduire.

Contrairement aux fixtures anonymisées (échantillons réels, couverture partielle),
celles-ci couvrent systématiquement les champs optionnels et les valeurs d'énumération
du schéma : si un modèle dbt ou le parser casse sur un champ que les échantillons
réels n'exercent pas, ce filet le voit.

Déterminisme : valeurs cycliques par occurrence (les enums tournent — c'est ce qui
donne naturellement avant/après sur C15 via Code_Qualification ∈ {1,2}), compteur
global pour les valeurs d'index (toutes distinctes → détecte les transpositions).
Regénération stable, diff git lisible.

Les XSD ne sont PAS dans le repo (docs Enedis) : la régénération exige
`~/Documents/guides_flux/` en local. Les XML générés et leurs golden sont commités —
la CI n'a pas besoin des XSD.

Usage :
    uv run python tests/fixtures/flux/generer_fixtures_xsd.py
    uv run python tests/fixtures/flux/generer_golden.py   # puis figer les golden
"""

import itertools
from dataclasses import dataclass, field
from pathlib import Path

from lxml import etree

ICI = Path(__file__).parent
RACINE_XSD = Path.home() / "Documents" / "guides_flux"

XSD_NS = "http://www.w3.org/2001/XMLSchema"


def _q(local: str) -> str:
    return f"{{{XSD_NS}}}{local}"


# Échantillons par pattern (l'ensemble des patterns des 5 XSD est fini ; un pattern
# inconnu lèvera KeyError et la validation finale attraperait toute dérive).
ECHANTILLONS_PATTERN = {
    "0[1-4]": "01",
    "032[1-8]": "0321",
    r"[0-9\+\(\)\s\.]{1,20}": "0102030405",
    r"[0-9\+\(\)\s\.]{1,25}": "0102030405",
    "[0-3][0-9]/[0-1][0-9]/[0-9]{4}": "15/06/2024",
    "[0-9]{14}": "99000000000001",
    "[0-9]{5}": "44000",
    "[0-9]{9}": "123456789",
    "[0-9A-Z]{2}": "AB",
    "[0-9A-Z]{4,8}": "ABCD",
    "[0-9A-Z]{5}": "ABCDE",
    "[0-9A-Za-z]{1,5}": "AB1",
    "[1-9][0-9]*.[0-9]+": "1.5",
    "[A-Z0-9]{1,15}": "ABC123",
    "[A-Z1-9]{1,15}": "ABC123",
    r"[A-Za-z0-9 _\-]{1,16}": "Fixture-1",
    r"\S{1,120}": "FIXTURE",
    r"[0-9a-zA-Z][\-._0-9a-zA-Z]{1,255}@[0-9a-zA-Z][\-._0-9a-zA-Z]{1,255}.[a-zA-Z]{2,6}": "fixture@example.org",
}

# Compteur global : chaque Valeur générée est distincte (détecte les transpositions).
_COMPTEUR_VALEUR = itertools.count(1000, 111)

# Surcharges par nom d'élément : valeurs métier plausibles là où le XSD ne contraint
# qu'une longueur. Callable(occurrence) → str. Prioritaires sur le type, PAS sur les
# énumérations (les valeurs légales du schéma priment).
SURCHARGES_NOM = {
    "Id_PRM": lambda occ: f"99{100000000000 + occ}",
    # Cadrans réels : la linéarisation en fait des noms de colonnes (index_<cadran>_kwh)
    # et flux_c15 agrège sur le domaine fermé des cadrans.
    "Id_Classe_Temporelle": lambda occ: ("BASE", "HP", "HC", "HPH")[occ % 4],
    "Valeur": lambda occ: str(next(_COMPTEUR_VALEUR)),
    "Valeur_Precedent": lambda occ: str(next(_COMPTEUR_VALEUR)),
}

VALEURS_BASE = {
    "string": "TEXTE",
    "date": "2024-06-15",
    "dateTime": "2024-06-15T00:01:00+02:00",
    "gYearMonth": "2024-06",
    "gYear": "2024",
    "time": "00:01:00",
    "boolean": "false",
    "integer": "7",
    "int": "7",
    "long": "7",
    "nonNegativeInteger": "7",
    "positiveInteger": "7",
    "decimal": "1.5",
    "float": "1.5",
    "double": "1.5",
}


@dataclass(frozen=True)
class SpecFixture:
    """Une fixture à générer : XSD source, élément racine, politique d'occurrences."""

    nom_sortie: str  # fichier généré sous tests/fixtures/flux/
    xsd: str  # chemin relatif à RACINE_XSD
    racine: str  # élément racine global du XSD
    # Occurrences par nom d'élément répétable. Défaut 1 (conservateur) : on ne monte
    # à 2+ que là où les deux chemins (legacy et dbt) gèrent la multiplicité.
    multiplicites: dict[str, int] = field(default_factory=dict)


SPECS: tuple[SpecFixture, ...] = (
    SpecFixture(
        "c15_xsd.xml",
        "Enedis SGE GUI 0300 Flux C15_v5.1.3/GRD.XSD.0301.Flux_C15_v5.2.0.xsd",
        "C15",
        # Donnees_Releve ×2 → Code_Qualification cycle {1,2} = avant/après.
        multiplicites={"PRM": 2, "Donnees_Releve": 2, "Classe_Temporelle_Distributeur": 2},
    ),
    SpecFixture(
        "r15_xsd.xml",
        "Guide Flux R15 Soutirage/ENEDIS.SGE.XSD.0293.Flux_R15_v2.3.2.xsd",
        "R15",
        # Donnees_Releve ×2 : le grain est le relevé (un PRM porte plusieurs relevés
        # à des dates différentes) — couvert depuis la correction de grain.
        multiplicites={"PRM": 2, "Donnees_Releve": 2, "Classe_Temporelle_Distributeur": 2},
    ),
    SpecFixture(
        "r151_xsd.xml",
        "Enedis.SGE.GUI.0317.Flux R151_v2.3.1/Enedis.SGE.XSD.0315.Flux_R151_v1.2.0.xsd",
        "R151",
        multiplicites={"PRM": 2, "Donnees_Releve": 2, "Classe_Temporelle_Distributeur": 2},
    ),
    SpecFixture(
        "f12_xsd.xml",
        "Enedis.SGE.GUI.0124.Flux F12_v1.14.3/Enedis.SGE.XSD.0126.F12_Donnees_Detail_1.20.3.xsd",
        "Detail_Facturation",
        multiplicites={"Sous_Lot": 2, "Element_Valorise": 2},
    ),
    SpecFixture(
        "f15_xsd.xml",
        "Enedis.SGE.GUI.0298.Flux F15_v4.1.3/GRD.XSD.0299.Flux_F15_Donnees_Detail_v4.0.4.xsd",
        "F15_Detail_Facturation",
        multiplicites={"Donnees_Valorisation": 2, "Groupe_Valorise": 2, "Element_Valorise": 2},
    ),
)


class GenerateurXsd:
    """Construit une instance XML maximale et déterministe depuis un XSD Enedis.

    Couvre les constructions effectivement utilisées par les XSD des flux : éléments
    globaux/inline, complexType à sequence, complexContent/extension (alias), simpleType
    restriction (enumeration, pattern, longueurs, totalDigits/fractionDigits,
    maxInclusive), types builtins xsd:*. Pas de namespaces ni d'attributs (absents
    des schémas flux).
    """

    def __init__(self, chemin_xsd: Path, multiplicites: dict[str, int]):
        self.schema_tree = etree.parse(str(chemin_xsd))
        self.schema = etree.XMLSchema(self.schema_tree)
        racine = self.schema_tree.getroot()
        self.multiplicites = multiplicites
        self.elements_globaux = {e.get("name"): e for e in racine.findall(_q("element"))}
        self.types_nommes = {t.get("name"): t for tag in ("complexType", "simpleType") for t in racine.findall(_q(tag))}

    def instance(self, nom_racine: str) -> etree._Element:
        element = self._genere(self.elements_globaux[nom_racine], occ=0)
        self.schema.assertValid(element.getroottree())
        return element

    # -- structure ----------------------------------------------------------

    def _genere(self, declaration: etree._Element, occ: int) -> etree._Element:
        nom = declaration.get("name")
        noeud = etree.Element(nom)
        type_attr = declaration.get("type")
        if type_attr and type_attr.startswith("xsd:") or type_attr and type_attr.startswith("xs:"):
            noeud.text = self._valeur_builtin(nom, type_attr.split(":")[1], occ)
            return noeud
        if type_attr:
            self._remplis_par_type(noeud, self.types_nommes[type_attr], occ)
            return noeud
        complexe = declaration.find(_q("complexType"))
        if complexe is not None:
            self._remplis_par_type(noeud, complexe, occ)
            return noeud
        simple = declaration.find(_q("simpleType"))
        if simple is not None:
            noeud.text = self._valeur_simple(nom, simple, occ)
            return noeud
        # Élément sans type : chaîne libre.
        noeud.text = self._valeur_builtin(nom, "string", occ)
        return noeud

    def _remplis_par_type(self, noeud: etree._Element, type_def: etree._Element, occ: int) -> None:
        if type_def.tag == _q("simpleType"):
            noeud.text = self._valeur_simple(noeud.tag, type_def, occ)
            return
        extension = type_def.find(f"{_q('complexContent')}/{_q('extension')}")
        if extension is not None:
            self._remplis_par_type(noeud, self.types_nommes[extension.get("base")], occ)
            contenu = extension
        else:
            contenu = type_def
        for modele in contenu:
            if modele.tag in (_q("sequence"), _q("choice")):
                self._genere_contenu(noeud, modele, occ)

    def _genere_contenu(self, noeud: etree._Element, conteneur: etree._Element, occ: int) -> None:
        """Marche un modèle de contenu (sequence/choice imbriqués) et émet les éléments."""
        est_choice = conteneur.tag == _q("choice")
        for enfant in conteneur:
            if enfant.tag == _q("element"):
                maxi = enfant.get("maxOccurs", "1")
                repetable = maxi == "unbounded" or int(maxi) > 1
                compte = self.multiplicites.get(enfant.get("name"), 1) if repetable else 1
                for i in range(compte):
                    # L'occurrence se propage : les enfants d'un élément répété
                    # héritent de sa position (fait cycler leurs énumérations).
                    noeud.append(self._genere(enfant, occ=i if repetable else occ))
            elif enfant.tag in (_q("sequence"), _q("choice")):
                self._genere_contenu(noeud, enfant, occ)
            if est_choice:
                # choice : première branche uniquement.
                break

    # -- valeurs ------------------------------------------------------------

    def _valeur_simple(self, nom: str, simple: etree._Element, occ: int) -> str:
        restriction = simple.find(_q("restriction"))
        if restriction is None:
            return self._valeur_builtin(nom, "string", occ)
        enums = [e.get("value") for e in restriction.findall(_q("enumeration"))]
        if enums:
            return enums[occ % len(enums)]
        if nom in SURCHARGES_NOM:
            return SURCHARGES_NOM[nom](occ)
        pattern = restriction.find(_q("pattern"))
        if pattern is not None:
            return ECHANTILLONS_PATTERN[pattern.get("value")]
        base = restriction.get("base", "xsd:string").split(":")[-1]
        if base in ("integer", "int", "long", "decimal", "float", "double"):
            return self._valeur_numerique(restriction, base)
        valeur = VALEURS_BASE.get(base, "TEXTE")
        return self._ajuste_longueur(valeur, restriction)

    def _valeur_numerique(self, restriction: etree._Element, base: str) -> str:
        maxi = restriction.find(_q("maxInclusive"))
        if maxi is not None:
            return maxi.get("value")
        fraction = restriction.find(_q("fractionDigits"))
        if base == "decimal" or fraction is not None:
            return "1.5"
        return "7"

    def _ajuste_longueur(self, valeur: str, restriction: etree._Element) -> str:
        longueur = restriction.find(_q("length"))
        if longueur is not None:
            n = int(longueur.get("value"))
            return (valeur * n)[:n]
        mini = restriction.find(_q("minLength"))
        maxi = restriction.find(_q("maxLength"))
        n_min = int(mini.get("value")) if mini is not None else 1
        n_max = int(maxi.get("value")) if maxi is not None else max(len(valeur), n_min)
        if len(valeur) < n_min:
            valeur = (valeur * n_min)[:n_min]
        return valeur[:n_max]

    def _valeur_builtin(self, nom: str, base: str, occ: int) -> str:
        if nom in SURCHARGES_NOM:
            return SURCHARGES_NOM[nom](occ)
        return VALEURS_BASE.get(base, "TEXTE")


def main() -> None:
    if not RACINE_XSD.is_dir():
        raise SystemExit(f"XSD Enedis introuvables : {RACINE_XSD} (régénération impossible sans eux)")
    for spec in SPECS:
        generateur = GenerateurXsd(RACINE_XSD / spec.xsd, spec.multiplicites)
        element = generateur.instance(spec.racine)
        contenu = etree.tostring(element, pretty_print=True, xml_declaration=True, encoding="UTF-8")
        (ICI / spec.nom_sortie).write_bytes(contenu)
        print(f"{spec.nom_sortie}: généré et validé contre {Path(spec.xsd).name}")


if __name__ == "__main__":
    main()
