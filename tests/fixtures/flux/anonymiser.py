"""Anonymisation de fichiers flux Enedis réels en fixtures committables (#121).

Pseudonymisation déterministe *dans un run* (sel aléatoire non persisté) :
la cohérence référentielle est préservée (un même PDL donne le même PDL fictif
dans tout le run), mais rien n'est réversible ni rejouable d'un run à l'autre.

Stratégies par balise :
- pseudonymisation format-préservante (PDL 14 chiffres, références numériques) ;
- placeholders fixes (identité, adresse, contacts) ;
- codes EIC fictifs (le dépôt est public — l'EIC identifie le fournisseur).

Tout le reste (dates, index, cadrans, FTA, puissances, codes événement,
montants) est conservé tel quel : c'est la matière des tests golden.

Marqueurs visibles pour la relecture : les PDL fictifs commencent par `99`
(préfixe jamais attribué par Enedis), les EIC fictifs par `17XFICTIF`. Les
autres pseudonymes (références numériques) sont indistinguables à l'œil —
utiliser le mode `--verifier` qui prouve balise par balise que chaque valeur
mappée diffère de la source.

Usage :
    uv run python tests/fixtures/flux/anonymiser.py SOURCE.xml DESTINATION.xml
    uv run python tests/fixtures/flux/anonymiser.py --verifier SOURCE.xml FIXTURE.xml

⚠️ Relecture humaine du résultat obligatoire avant commit (HITL, cf. #121).
"""

import hashlib
import os
import sys
from pathlib import Path

from lxml import etree

_SEL = os.urandom(16)  # non persisté : pseudonymisation non rejouable


def _pseudo_chiffres(valeur: str, longueur: int | None = None) -> str:
    """Pseudonyme numérique stable (dans le run), de même longueur que la valeur."""
    longueur = longueur or len(valeur)
    h = hashlib.sha256(_SEL + valeur.encode()).hexdigest()
    return "".join(str(int(c, 16) % 10) for c in h)[:longueur]


def _pseudo_pdl(valeur: str) -> str:
    """PDL fictif : préfixe 99 (jamais attribué) + 12 chiffres stables."""
    return "99" + _pseudo_chiffres(valeur, 12)


def _pseudo_eic(valeur: str) -> str:
    """Code EIC visiblement fictif, au format 16 caractères (préfixe 17X = zone France)."""
    h = hashlib.sha256(_SEL + valeur.encode()).hexdigest().upper()
    return "17XFICTIF" + h[:6] + "X"


# Balise → pseudonymisation format-préservante (références croisées cohérentes)
PSEUDONYMES = {
    "Id_PRM": _pseudo_pdl,
    "Ref_Situation_Contractuelle": _pseudo_chiffres,
    "Ref_Demandeur": _pseudo_chiffres,
    "Ref_Fournisseur": _pseudo_chiffres,
    "Id_Affaire": _pseudo_chiffres,
    "Id_Releve": _pseudo_chiffres,
    "Id_Releve_Precedent": _pseudo_chiffres,
    "Num_Valorisation": _pseudo_chiffres,
    "Identifiant_Contrat": _pseudo_chiffres,
    "Identifiant": _pseudo_chiffres,
    "Numero_Abonnement": _pseudo_chiffres,
    "Num_Serie": _pseudo_chiffres,
    "Num_Facture": _pseudo_chiffres,
    "Identifiant_Emetteur": _pseudo_eic,
    "Identifiant_Destinataire": _pseudo_eic,
    "Code_EIC_Fournisseur": _pseudo_eic,
    "Code_EIC_Responsable_Equilibre": _pseudo_eic,
}

# Balise → placeholder fixe (identité, adresse, contacts)
PLACEHOLDERS = {
    "Civilite": "M",
    "Nom": "ANONYME",
    "Prenom": "Anonyme",
    "Email": "anonyme@example.org",
    "Telephone1_Num": "0600000000",
    "Num_Depannage": "0000000000",
    "Rue": "1 RUE DE L'EXEMPLE",
    "Etage": "RDC",
    "Batiment": "BAT A",
    "Escalier": "ESC A",
    "Porte": "PORTE 1",
    "Lieu_Dit": "LIEU-DIT EXEMPLE",
    "Ligne_1": "ANONYME",
    "Ligne_2": "1 RUE DE L'EXEMPLE",
    "Ligne_3": "1 RUE DE L'EXEMPLE",
    "Ligne_4": "LIGNE ADRESSE",
    "Ligne_5": "LIGNE ADRESSE",
    "Ligne_6": "00000 EXEMPLEVILLE",
    "Complement_Localisation": "",
    "Libelle_Commune": "EXEMPLEVILLE",
    "Code_Postal": "00000",
    "Code_Commune": "00000",
    "Code_Departement": "00",
    "Raison_Sociale": "FOURNISSEUR EXEMPLE",
}


def anonymiser(xml_bytes: bytes) -> tuple[bytes, dict[str, int]]:
    """Anonymise un document flux. Retourne (xml_anonymisé, rapport balise→nb)."""
    root = etree.fromstring(xml_bytes)
    rapport: dict[str, int] = {}

    for element in root.iter():
        tag = element.tag
        if element.text is None or not element.text.strip():
            continue
        if tag in PSEUDONYMES:
            element.text = PSEUDONYMES[tag](element.text.strip())
            rapport[tag] = rapport.get(tag, 0) + 1
        elif tag in PLACEHOLDERS:
            element.text = PLACEHOLDERS[tag]
            rapport[tag] = rapport.get(tag, 0) + 1

    return etree.tostring(root, xml_declaration=True, encoding="UTF-8", pretty_print=True), rapport


def verifier(source: Path, fixture: Path) -> bool:
    """Prouve, balise mappée par balise mappée, que la fixture diffère de la source.

    N'affiche jamais les valeurs source — seulement la valeur fixture et le
    verdict. Retourne False (et signale 🚨) à la moindre valeur identique.
    """
    racine_source = etree.parse(str(source)).getroot()
    racine_fixture = etree.fromstring(fixture.read_bytes())
    ok = True
    for tag in sorted(PSEUDONYMES):
        valeurs_source = [e.text for e in racine_source.iter(tag) if e.text and e.text.strip()]
        valeurs_fixture = [e.text for e in racine_fixture.iter(tag) if e.text and e.text.strip()]
        for i, (vs, vf) in enumerate(zip(valeurs_source, valeurs_fixture, strict=True)):
            if vs == vf:
                ok = False
                print(f"🚨 {tag}[{i}] IDENTIQUE À LA SOURCE : {vf!r}")
            else:
                print(f"✅ {tag}[{i}] = {vf!r} (≠ source)")
    print("\n✅ fixture pseudonymisée" if ok else "\n🚨 FUITE — ne pas committer")
    return ok


def main() -> None:
    if sys.argv[1] == "--verifier":
        sys.exit(0 if verifier(Path(sys.argv[2]), Path(sys.argv[3])) else 1)

    source, destination = Path(sys.argv[1]), Path(sys.argv[2])
    contenu, rapport = anonymiser(source.read_bytes())
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(contenu)
    print(f"{source.name} → {destination}")
    for tag, nb in sorted(rapport.items()):
        print(f"  {tag}: {nb} occurrence(s) anonymisée(s)")


if __name__ == "__main__":
    main()
