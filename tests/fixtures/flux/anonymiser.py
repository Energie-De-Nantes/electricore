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
    uv run python tests/fixtures/flux/anonymiser.py --r64-zip SOURCE.zip DESTINATION.json

Le mode `--r64-zip` fait la chaîne complète sur un R64 chiffré : déchiffrement
AES (trousseau lu dans AES__TROUSSEAU__<label>__KEY/IV, jamais persisté),
extraction ZIP, anonymisation JSON, et rapport d'audit (clés rencontrées +
alarmes sur valeurs suspectes non mappées) pour la relecture.

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


# Clés JSON R64 → pseudonymisation (mêmes stratégies que les balises XML)
PSEUDONYMES_JSON = {
    "idPrm": _pseudo_pdl,
    "idDemande": _pseudo_chiffres,
    "siDemandeur": _pseudo_chiffres,
    "idAffaire": _pseudo_chiffres,
}

_MOTIFS_SUSPECTS = (r"^\d{14}$", r"^0[1-9]\d{8}$", r"@")


def anonymiser_r64(json_bytes: bytes) -> tuple[bytes, dict[str, int], list[tuple[str, str]]]:
    """Anonymise un document JSON R64.

    Returns:
        (json_anonymisé, rapport clé→nb, alarmes [(clé, valeur suspecte non mappée)])
    """
    import json as json_module
    import re

    data = json_module.loads(json_bytes.decode("utf-8"))
    rapport: dict[str, int] = {}
    alarmes: list[tuple[str, str]] = []

    def _walk(node):
        if isinstance(node, dict):
            for cle, valeur in node.items():
                if cle in PSEUDONYMES_JSON and isinstance(valeur, str) and valeur.strip():
                    node[cle] = PSEUDONYMES_JSON[cle](valeur.strip())
                    rapport[cle] = rapport.get(cle, 0) + 1
                elif isinstance(valeur, str) and any(re.search(m, valeur) for m in _MOTIFS_SUSPECTS):
                    alarmes.append((cle, valeur))
                else:
                    _walk(valeur)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(data)
    return json_module.dumps(data, ensure_ascii=False, indent=2).encode() + b"\n", rapport, alarmes


def anonymiser_r64_zip(source: Path, destination: Path) -> None:
    """Chaîne complète sur un R64 chiffré : AES → unzip → anonymisation → audit."""
    from electricore.ingestion.transformers.archive import extract_files_from_zip
    from electricore.ingestion.transformers.crypto import decrypt_with_key_chain, load_aes_key_chain

    chain = load_aes_key_chain()
    dechiffre, cle = decrypt_with_key_chain(source.read_bytes(), chain)
    print(f"déchiffré avec la clé '{cle}'")

    fichiers = extract_files_from_zip(dechiffre, ".json")
    if not fichiers:
        import io
        import zipfile

        noms = zipfile.ZipFile(io.BytesIO(dechiffre)).namelist()
        raise ValueError(f"aucun .json dans le ZIP déchiffré (contenu : {noms})")
    nom, contenu = fichiers[0]
    if len(fichiers) > 1:
        print(f"⚠️ {len(fichiers)} JSON dans le ZIP — seul {nom} est utilisé")

    anonyme, rapport, alarmes = anonymiser_r64(contenu)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(anonyme)
    print(f"{nom} → {destination}")
    for cle_json, nb in sorted(rapport.items()):
        print(f"  {cle_json}: {nb} occurrence(s) anonymisée(s)")
    if alarmes:
        print("\n🚨 VALEURS SUSPECTES NON MAPPÉES — ne pas committer sans arbitrage :")
        for cle_json, valeur in alarmes:
            print(f"  {cle_json}: {valeur!r}")
    else:
        print("✅ aucune valeur suspecte hors clés mappées")


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
    if sys.argv[1] == "--r64-zip":
        anonymiser_r64_zip(Path(sys.argv[2]), Path(sys.argv[3]))
        return

    source, destination = Path(sys.argv[1]), Path(sys.argv[2])
    contenu, rapport = anonymiser(source.read_bytes())
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(contenu)
    print(f"{source.name} → {destination}")
    for tag, nb in sorted(rapport.items()):
        print(f"  {tag}: {nb} occurrence(s) anonymisée(s)")


if __name__ == "__main__":
    main()
