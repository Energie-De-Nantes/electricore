"""Relais de flux Enedis déchiffrés vers le SFTP d'un partenaire (#637).

Runtime **indépendant** de l'ingestion (pipeline dlt distinct, destination
DuckDB distincte) mais code **co-localisé** : réutilise les briques stables
`decrypt_with_key_chain` / `extract_files_from_zip` / `load_aes_key_chain` /
`create_sftp_resource`. N'importe PAS le runner d'ingestion, son curseur, son
état ni sa DuckDB — garde vérifiée par `tests/ingestion/test_relais_independance.py`.

Boucle de **balayage réconciliant** (pas inotify) : re-liste TOUTE la source à
chaque run (`incremental=False` sur `create_sftp_resource`) — un curseur
high-water avancerait au LISTING et sauterait à jamais un zip listé mais dont
le push échoue (le bug de classe R67, cf. `ingestion/sources/sftp_enedis.py`).

État = `dlt.current.resource_state(NOM_RESOURCE).setdefault("zips_livrés", [])`
— un membership set committé atomiquement par dlt à la fin d'un run réussi. Un
zip n'y entre **qu'après** un push réussi (`_pousser_et_enregistrer`) : la
seule direction d'échec possible est le « re-push », jamais le « oubli ». Le
nom de resource est **épingle explicitement** (`NOM_RESOURCE`, passé à
`resource_state()`) plutôt que laissé à la résolution automatique : dans une
chaîne pipée (`sftp | filtre | decrypt | push`), chaque étage a sa propre
identité de pipe tant que dlt ne l'a pas figée — sans épingle, `_filtre_zip`
et `_pousser_et_enregistrer` écriraient dans deux états DIFFÉRENTS (même bug
de classe que `ESPACE_ETAT_INCREMENTAL` dans `sources/sftp_enedis.py`, #346).

Chaîne : `sftp_resource | filtre | decrypt | push_et_enregistrer`. Le filtre
(déjà-livré / flux / date) tourne AVANT le déchiffrement — il ne coûte qu'un
listing, pas un decrypt, sur les zips déjà relayés. `decrypt` (brique stable)
catch déjà ValueError → clé incorrecte = sauté, retry au run suivant. Le push
est un effet de bord : une exception (cible injoignable…) est attrapée dans
`_pousser_et_enregistrer`, loggée, PAS enregistrée → retry au run suivant
(même discipline "attraper → compter → continuer" que la chaîne d'ingestion,
`ingestion/transformers/chaine.py::etape_chaine`, sans en dépendre).
"""

import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import dlt
import fsspec

from electricore.config import runtime
from electricore.ingestion.sources.sftp_enedis import create_sftp_resource
from electricore.ingestion.transformers.archive import extract_files_from_zip
from electricore.ingestion.transformers.crypto import (
    create_decrypt_transformer,
    load_aes_key_chain,
)

logger = logging.getLogger(__name__)

NOM_RESOURCE = "relais_livraisons"
NOM_DATASET = "journal"  # ≠ nom de la base ("relais.duckdb") : évite l'ambiguïté catalog/dataset DuckDB


def _match_flux(file_name: str, flux_filtres: set[str] | None) -> bool:
    """Vrai si le nom de zip porte un des codes flux filtrés (convention Enedis `..._<FLUX>_...`).

    `flux_filtres` vide/None → filtre désactivé, tout matche (phase 1 : liste de flux en config).
    """
    if not flux_filtres:
        return True
    upper = file_name.upper()
    return any(f"_{code}_" in upper for code in flux_filtres)


def _filtre_zip(zip_item: dict, flux_filtres: set[str] | None, depuis: str) -> Iterator[dict]:
    """Étage filtre : déjà-livré (resource_state) / flux / fenêtre date — avant tout décryptage.

    La comparaison date est une comparaison de **chaînes ISO-8601** (`depuis` = date
    `YYYY-MM-DD`, `modification_date` déjà normalisée en ISO string par
    `create_sftp_resource`) : un préfixe de date est lexicographiquement inférieur à tout
    horodatage du même jour ou plus tard — pas besoin de parser en `datetime`.
    """
    livres = dlt.current.resource_state(NOM_RESOURCE).setdefault("zips_livrés", [])
    nom = zip_item["file_name"]
    if nom in livres:
        return
    if not _match_flux(nom, flux_filtres):
        return
    if zip_item["modification_date"] < depuis:
        return
    yield zip_item


def _create_filtre_transformer(flux_filtres: set[str] | None, depuis: str):
    @dlt.transformer
    def filtre_zip(zip_item: dict) -> Iterator[dict]:
        return _filtre_zip(zip_item, flux_filtres, depuis)

    return filtre_zip


def pousser_vers_partenaire(fichiers: list[tuple[str, bytes]], partner_url: str) -> None:
    """Pousse les fichiers extraits vers la cible partenaire (fsspec-agnostic : file://, sftp://).

    Effet de bord : une cible injoignable (permission, réseau…) **lève** — direction
    d'échec sûre, le zip n'est alors PAS enregistré comme livré (cf. `_pousser_et_enregistrer`).
    """
    fs, base_path = fsspec.core.url_to_fs(partner_url)
    fs.makedirs(base_path, exist_ok=True)
    base = base_path.rstrip("/")
    for nom, contenu in fichiers:
        with fs.open(f"{base}/{nom}", "wb") as f:
            f.write(contenu)


def _pousser_et_enregistrer(decrypted_file: dict, partner_url: str) -> Iterator[dict]:
    """Étage push : extrait (tous fichiers, pas seulement .xml — le relais est agnostique au
    contenu), pousse, puis enregistre APRÈS succès seulement.

    Discipline « attraper → compter → continuer » (mêmes garanties que
    `etape_chaine`, sans en dépendre) : toute exception d'extraction ou de push est
    attrapée ici, loggée, et fait sauter le yield — le zip n'entre PAS dans `zips_livrés`,
    il sera retenté au run suivant (balayage réconciliant, `incremental=False`).
    """
    zip_name = decrypted_file["file_name"]
    try:
        # file_extension="" : `str.endswith("")` est toujours vrai → extrait TOUT le
        # contenu du zip (xml et json compris), pas seulement les .xml.
        fichiers = extract_files_from_zip(decrypted_file["decrypted_content"], file_extension="")
        pousser_vers_partenaire(fichiers, partner_url)
    except Exception as e:  # noqa: BLE001 — direction d'échec sûre (#637) : jamais propagé, jamais enregistré
        logger.warning("Relais : échec push %s : %s", zip_name, e)
        return

    livres = dlt.current.resource_state(NOM_RESOURCE).setdefault("zips_livrés", [])
    livres.append(zip_name)
    yield {
        "zip": zip_name,
        "fichiers": [nom for nom, _ in fichiers],
        "at": datetime.now(UTC).isoformat(),
    }


def _create_push_transformer(partner_url: str):
    @dlt.transformer
    def push_et_enregistrer(decrypted_file: dict) -> Iterator[dict]:
        return _pousser_et_enregistrer(decrypted_file, partner_url)

    return push_et_enregistrer


@dlt.source(name="relais_flux_enedis")
def relais_source(
    source_url: str,
    partner_url: str,
    flux_filtres: set[str] | None,
    depuis: str,
    key_chain: list[tuple[str, bytes, bytes | None]],
):
    """Source dlt unique : listing (re-listé en entier, `incremental=False`) → filtre →
    déchiffrement → push+enregistrement. Une seule resource nommée `relais_livraisons`."""
    sftp_resource = create_sftp_resource("RELAIS", "relais", "**/*.zip", source_url, incremental=False)
    filtre = _create_filtre_transformer(flux_filtres, depuis)
    decrypt = create_decrypt_transformer(key_chain=key_chain)
    push = _create_push_transformer(partner_url)

    pipeline_relais = (sftp_resource | filtre | decrypt | push).with_name(NOM_RESOURCE)
    pipeline_relais.apply_hints(write_disposition="append")
    yield pipeline_relais


def executer(pipeline: "dlt.Pipeline | None" = None):
    """Point d'entrée programmatique : construit (ou reçoit, pour les tests) un pipeline
    dlt dédié et l'exécute contre la config du domaine runtime `relais` (#637)."""
    cfg = runtime.relais()
    key_chain = load_aes_key_chain()
    if pipeline is None:
        pipeline = dlt.pipeline(
            pipeline_name="relais_flux_enedis",
            destination=dlt.destinations.duckdb(str(cfg.destination_db)),
            dataset_name=NOM_DATASET,
        )
    return pipeline.run(relais_source(cfg.source_url, cfg.partner_url, cfg.flux_filtres(), cfg.depuis, key_chain))


def zips_non_relayes(source_url: str, db_path: Path) -> list[str]:
    """Vérification de complétude (#637) : zips de la source absents du journal de destination.

    Écart entre le listing source (fsspec, tous les `*.zip` récursivement) et la table
    `relais.relais_livraisons` (peuplée par `executer`) — une requête simple, indépendante
    du `resource_state` (qui gouverne seulement le skip du run suivant)."""
    import duckdb

    fs, base_path = fsspec.core.url_to_fs(source_url)
    zips_source = {Path(p).name for p in fs.glob(f"{base_path.rstrip('/')}/**/*.zip")}

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        livres = {row[0] for row in con.execute(f'select "zip" from "{NOM_DATASET}"."{NOM_RESOURCE}"').fetchall()}
    except duckdb.CatalogException:
        livres = set()
    finally:
        con.close()
    return sorted(zips_source - livres)
