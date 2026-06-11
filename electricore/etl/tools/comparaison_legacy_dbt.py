"""Comparaison legacy vs dbt sur les mêmes fichiers XML (validation ADR-0020).

Pour chaque flux : les mêmes XML sont parsés par le parseur legacy
(`parser_flux_xml` + `flux.yaml`) ET par le chemin dbt (`xml_vers_dict` → landing
JSON → `dbt build`), puis les deux jeux de records sont comparés en multiset
d'**empreintes canoniques** — modulo traçabilité et représentation (dates → instant
UTC, nombres → float). Aucun golden : on compare deux implémentations entre elles.

C'est ce harnais qui a débusqué le bug de grain (relevés agrégés par PDL au lieu de
l'occurrence de PRM) et le mélange index/conso R15 (PR #130) — tous deux invisibles
aux fixtures golden (une occurrence par PDL).

Usage CLI (cache réel, ~4400 XML, quelques minutes) :

    uv run python -m electricore.etl.tools.comparaison_legacy_dbt \
        [racine_xml]   # défaut : ~/data/flux_enedis_v2

Usage test : tests/etl/test_comparaison_legacy_dbt.py (fixtures en CI, réel en local).
"""

import glob
import json
import os
import re
import tempfile
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from electricore.etl.parsing import ConfigFluxXml, TracabiliteFlux, parser_flux_xml
from electricore.etl.parsing.xml import xml_vers_dict

RACINE_PROJET = Path(__file__).parents[3]
PROJET_DBT = RACINE_PROJET / "electricore" / "etl" / "dbt"
FLUX_YAML = RACINE_PROJET / "electricore" / "etl" / "config" / "flux.yaml"

# Colonnes de traçabilité : hors périmètre (ajoutées hors linéarisation).
TRACABILITE = {"_source_zip", "_flux_type", "_xml_name", "_json_name", "modification_date"}

_RE_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}T")
_RE_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class CasComparaison:
    """Un cas : (flux legacy, modèle dbt, table brute, index dans xml_configs)."""

    flux: str  # clé flux.yaml (C15, R15…) — aussi le sous-dossier du cache réel
    model: str  # modèle dbt = table matérialisée
    source: str  # table brute (raw_*)
    idx: int  # index dans xml_configs du flux


# Les 6 tables XML (R64 est du JSON, hors périmètre de ce harnais).
CAS: tuple[CasComparaison, ...] = (
    CasComparaison("C15", "flux_c15", "raw_c15", 0),
    CasComparaison("R15", "flux_r15", "raw_r15", 0),
    CasComparaison("R15", "flux_r15_acc", "raw_r15", 1),
    CasComparaison("R151", "flux_r151", "raw_r151", 0),
    CasComparaison("F12", "flux_f12_detail", "raw_f12", 0),
    CasComparaison("F15", "flux_f15_detail", "raw_f15", 0),
)


@dataclass(frozen=True)
class ResultatComparaison:
    """Bilan d'un cas : volumes et divergences (multiset d'empreintes)."""

    cas: CasComparaison
    nb_fichiers: int
    nb_legacy: int
    nb_dbt: int
    nb_identiques: int
    nb_seul_legacy: int
    nb_seul_dbt: int

    @property
    def divergences(self) -> int:
        return self.nb_seul_legacy + self.nb_seul_dbt

    def __str__(self) -> str:
        return (
            f"{self.cas.model}: {self.nb_fichiers} fichiers | "
            f"legacy={self.nb_legacy} dbt={self.nb_dbt} | identiques={self.nb_identiques} | "
            f"seul_legacy={self.nb_seul_legacy} seul_dbt={self.nb_seul_dbt}"
        )


def canonique(valeur: Any) -> Any:
    """Forme canonique d'une valeur, pour comparer modulo représentation.

    Dates/horodatages → instant UTC ISO ; nombres (str ou non) → float ;
    le reste → str. Le tag de type évite les collisions inter-domaines.
    """
    if valeur is None:
        return None
    if isinstance(valeur, datetime):
        return ("dt", _heure_mur_utc(valeur))
    if isinstance(valeur, date):
        return ("d", valeur.isoformat())
    texte = str(valeur)
    try:
        return ("n", float(texte))
    except ValueError:
        pass
    if _RE_DATETIME.match(texte):
        try:
            return ("dt", _heure_mur_utc(datetime.fromisoformat(texte)))
        except ValueError:
            pass
    if _RE_DATE.match(texte):
        return ("d", texte)
    # Modulo espaces de bord : xml_vers_dict strip les feuilles, le legacy garde les
    # espaces Enedis (« Composante de relevé résiduel  » a un espace final en source).
    return ("s", texte.strip())


def _heure_mur_utc(valeur: datetime) -> str:
    """Horodatage canonique : heure-mur UTC, sans offset.

    Les aware sont convertis en UTC puis l'offset tombe ; les naïfs restent tels
    quels. Équivaut « naïf == UTC » — c'est la convention dlt du legacy (les dates
    R64 sans offset sont stockées comme instants UTC), et pour les sources à offset
    (C15 +02:00) les deux côtés convergent vers la même heure-mur UTC : la
    comparaison reste une comparaison d'instants.
    """
    if valeur.tzinfo:
        return valeur.astimezone(UTC).replace(tzinfo=None).isoformat()
    return valeur.isoformat()


def empreinte(record: dict) -> frozenset:
    """Empreinte canonique d'un record (hors traçabilité et valeurs nulles)."""
    return frozenset((cle, canonique(val)) for cle, val in record.items() if cle not in TRACABILITE and val is not None)


def _records_legacy(fichiers: list[str], cas: CasComparaison) -> list[dict]:
    entry = yaml.safe_load(FLUX_YAML.read_text())[cas.flux]["xml_configs"][cas.idx]
    config = ConfigFluxXml.depuis_yaml(entry)
    records: list[dict] = []
    for chemin in fichiers:
        trac = TracabiliteFlux(
            source_zip="comparaison", nom_fichier=Path(chemin).name, flux_type=cas.flux, modification_date="x"
        )
        records.extend(parser_flux_xml(Path(chemin).read_bytes(), config, trac))
    return records


def _records_dbt(fichiers: list[str], cas: CasComparaison) -> list[dict]:
    import dlt
    import duckdb
    from dbt.cli.main import dbtRunner

    from electricore.etl.raw_landing import lander_documents_bruts

    dossier = tempfile.mkdtemp(prefix="cmp_dbt_")
    db_path = os.path.join(dossier, "flux.duckdb")
    documents = [
        {"file_name": Path(f).name, "modification_date": "x", "content": xml_vers_dict(Path(f).read_bytes())}
        for f in fichiers
    ]
    pipeline = dlt.pipeline(
        # Nom unique par run : l'état local dlt est par pipeline_name, un nom fixe
        # pourrait recoller l'état d'un run précédent sur une autre base.
        pipeline_name=f"cmp_{cas.model}_{Path(dossier).name}",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="flux_raw",
    )
    lander_documents_bruts(pipeline, cas.source, documents)
    os.environ["DBT_DUCKDB_PATH"] = db_path
    resultat = dbtRunner().invoke(
        [
            "build",
            "--select",
            f"+{cas.model}",
            "--project-dir",
            str(PROJET_DBT),
            "--profiles-dir",
            str(PROJET_DBT),
            "--target-path",
            os.path.join(dossier, "target"),
        ]
    )
    if not resultat.success:
        raise RuntimeError(f"dbt build {cas.model} a échoué : {resultat.exception}")
    con = duckdb.connect(db_path)
    curseur = con.execute(f"select * from main.{cas.model}")
    colonnes = [d[0] for d in curseur.description]
    lignes = [dict(zip(colonnes, ligne, strict=True)) for ligne in curseur.fetchall()]
    con.close()
    return lignes


def comparer_cas(cas: CasComparaison, fichiers: list[str]) -> ResultatComparaison:
    """Compare legacy et dbt sur une liste de fichiers XML d'un même flux."""
    legacy = _records_legacy(fichiers, cas)
    dbt_rows = _records_dbt(fichiers, cas)
    compte_legacy = Counter(empreinte(r) for r in legacy)
    compte_dbt = Counter(empreinte(r) for r in dbt_rows)
    return ResultatComparaison(
        cas=cas,
        nb_fichiers=len(fichiers),
        nb_legacy=len(legacy),
        nb_dbt=len(dbt_rows),
        nb_identiques=sum((compte_legacy & compte_dbt).values()),
        nb_seul_legacy=sum((compte_legacy - compte_dbt).values()),
        nb_seul_dbt=sum((compte_dbt - compte_legacy).values()),
    )


def fichiers_du_flux(racine: Path, flux: str) -> list[str]:
    """Les XML d'un flux dans une racine type cache réel (`<racine>/<FLUX>/**/*.xml`).

    Dédoublonnés par nom de fichier : Enedis re-livre parfois le même XML dans
    plusieurs zips (6 cas F15 observés, contenus identiques au bit près). Le landing
    dbt dédoublonne par construction (merge sur file_name) ; le parseur legacy, lui,
    double-compte ces re-livraisons — on compare le parsing, pas la politique de
    livraison, donc chaque document unique n'entre qu'une fois.
    """
    fichiers = sorted(glob.glob(str(racine / flux / "**" / "*.xml"), recursive=True))
    vus: set[str] = set()
    uniques = []
    for chemin in fichiers:
        nom = Path(chemin).name
        if nom not in vus:
            vus.add(nom)
            uniques.append(chemin)
    return uniques


def main() -> None:
    import sys

    racine = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.home() / "data" / "flux_enedis_v2"
    if not racine.is_dir():
        raise SystemExit(f"Racine XML introuvable : {racine}")
    bilan = {}
    for cas in CAS:
        fichiers = fichiers_du_flux(racine, cas.flux)
        if not fichiers:
            print(f"{cas.model}: aucun fichier sous {racine / cas.flux}, ignoré")
            continue
        resultat = comparer_cas(cas, fichiers)
        print(resultat)
        bilan[cas.model] = resultat.divergences
    print("\nBudgets (divergences par modèle) :", json.dumps(bilan, indent=2))


if __name__ == "__main__":
    main()
