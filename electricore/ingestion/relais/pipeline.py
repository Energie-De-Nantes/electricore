"""Relais de flux Enedis déchiffrés vers le SFTP d'un partenaire (#637, escalade #643).

Runtime **indépendant** de l'ingestion (pipeline dlt distinct, destination
DuckDB distincte) mais code **co-localisé** : réutilise les briques stables
`decrypt_with_key_chain` / `extract_files_from_zip` / `load_aes_key_chain` /
`create_sftp_resource`, ainsi que la discipline de chaîne `etape_chaine`
(`transformers/chaine.py`, stdlib-only, hors `MODULES_INTERDITS`). N'importe PAS
le runner d'ingestion, son curseur, son état ni sa DuckDB — garde vérifiée par
`tests/ingestion/test_relais_independance.py`.

Boucle de **balayage réconciliant** (pas inotify) : re-liste TOUTE la source à
chaque run (`incremental=False` sur `create_sftp_resource`) — un curseur
high-water avancerait au LISTING et sauterait à jamais un zip listé mais dont
le push échoue (le bug de classe R67, cf. `ingestion/sources/sftp_enedis.py`).

État = `dlt.current.resource_state(NOM_RESOURCE).setdefault("zips_livrés", [])`
— un membership set committé atomiquement par dlt à la fin d'un run réussi. Un
zip n'y entre **qu'après** un push réussi (`_pousser`, discipline `etape_chaine`)
ou un amorçage (`seed_avant`) : la seule direction d'échec possible côté push
est le « re-push », jamais le « oubli ». Le nom de resource est **épinglé
explicitement** (`NOM_RESOURCE`, passé à `resource_state()`) plutôt que laissé à
la résolution automatique : dans une chaîne pipée (`sftp | filtre | decrypt |
push`), chaque étage a sa propre identité de pipe tant que dlt ne l'a pas figée
— sans épingle, `_filtre_zip` et `_pousser` écriraient dans deux états
DIFFÉRENTS (même bug de classe que `ESPACE_ETAT_INCREMENTAL` dans
`sources/sftp_enedis.py`, #346).

Chaîne : `sftp_resource | filtre | decrypt | push`. Le filtre (déjà-livré /
flux) tourne AVANT le déchiffrement — il ne coûte qu'un listing, pas un
decrypt, sur les zips déjà relayés. `decrypt` (brique stable) catch déjà
ValueError → clé incorrecte = sauté, retry au run suivant. Le push applique la
MÊME discipline « attraper → compter → continuer » que les trois étages de
l'ingestion (`etape_chaine`) : le compteur `StatsRelais` alimente le prédicat
`relais_aveugle()` (même forme que `StatsChaine.flux_aveugle()`, ADR-0037 ext.
#445) — un run où TOUS les push échouent doit escalader (sortie non-zéro), pas
retenter en silence pour toujours (le reproche adressé à inotify dans #637).

Amorçage (`seed_avant`, #643) : remplace l'ancien filtre `depuis` (knob
permanent) par un acte UNIQUE — marquer les zips antérieurs à une date comme
livrés SANS les pousser, pour ne pas noyer le partenaire dans l'historique au
premier run. Écrit dans le MÊME état `zips_livrés` (donc le run normal ne les
repousse jamais) et dans le journal avec `statut='amorce'` (distinct de
`'pousse'`) — sans cette ligne de journal, `zips_non_relayes()` remonterait à
vie des milliers de faux positifs. Refuse par défaut si le journal contient
déjà des livraisons (`force=True` pour l'opérateur qui sait ce qu'il fait) :
lancé par erreur après la mise en service, un amorçage enterrerait
silencieusement tout ce qui restait à relayer.
"""

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import dlt
import fsspec

from electricore.config import runtime
from electricore.ingestion.sources.sftp_enedis import create_sftp_resource
from electricore.ingestion.transformers.archive import extract_files_from_zip
from electricore.ingestion.transformers.chaine import etape_chaine
from electricore.ingestion.transformers.crypto import (
    create_decrypt_transformer,
    load_aes_key_chain,
)

logger = logging.getLogger(__name__)

NOM_RESOURCE = "relais_livraisons"
NOM_DATASET = "journal"  # ≠ nom de la base ("relais.duckdb") : évite l'ambiguïté catalog/dataset DuckDB
NOM_PIPELINE = "relais_flux_enedis"


@dataclass
class StatsRelais:
    """Compteur de la chaîne du relais — escalade process-wide (#643, ADR-0037 ext.).

    Un seul objet mutable, partagé par l'étage push d'un run entier (le relais n'a qu'une
    seule resource, contrairement aux `StatsChaine` par-flux de l'ingestion). `candidats`
    est le pré-compte d'entrée (zips arrivés au push, hors discipline) ; `pousses`/
    `echecs_push` sont mutés par `etape_chaine` au fil des yields/exceptions.
    """

    candidats: int = 0
    pousses: int = 0
    echecs_push: int = 0

    def relais_aveugle(self) -> bool:
        """Même prédicat que `StatsChaine.flux_aveugle()` : 0 push réussi malgré ≥ 1 échec.

        Un échec **isolé** noyé dans des push réussis (`pousses > 0`) est toléré — retenté
        au run suivant par le balayage réconciliant, jamais une raison d'escalader. Un run
        SANS aucun candidat (source vide) n'est pas non plus aveugle (`echecs_push == 0`).
        """
        return self.pousses == 0 and self.echecs_push > 0


def _match_flux(file_name: str, flux_filtres: set[str] | None) -> bool:
    """Vrai si le nom de zip porte un des codes flux filtrés (convention Enedis `..._<FLUX>_...`).

    `flux_filtres` vide/None → filtre désactivé, tout matche.
    """
    if not flux_filtres:
        return True
    upper = file_name.upper()
    return any(f"_{code}_" in upper for code in flux_filtres)


def _filtre_zip(zip_item: dict, flux_filtres: set[str] | None) -> Iterator[dict]:
    """Étage filtre : déjà-livré (resource_state) / flux — avant tout décryptage."""
    livres = dlt.current.resource_state(NOM_RESOURCE).setdefault("zips_livrés", [])
    nom = zip_item["file_name"]
    if nom in livres:
        return
    if not _match_flux(nom, flux_filtres):
        return
    yield zip_item


def _create_filtre_transformer(flux_filtres: set[str] | None):
    @dlt.transformer
    def filtre_zip(zip_item: dict) -> Iterator[dict]:
        return _filtre_zip(zip_item, flux_filtres)

    return filtre_zip


def pousser_vers_partenaire(fichiers: list[tuple[str, bytes]], partner_url: str) -> None:
    """Pousse les fichiers extraits vers la cible partenaire (fsspec-agnostic : file://, sftp://).

    Effet de bord : une cible injoignable (permission, réseau…) **lève** — direction
    d'échec sûre, le zip n'est alors PAS enregistré comme livré (discipline `etape_chaine`
    autour de `_pousser`, ci-dessous).
    """
    fs, base_path = fsspec.core.url_to_fs(partner_url)
    fs.makedirs(base_path, exist_ok=True)
    base = base_path.rstrip("/")
    for nom, contenu in fichiers:
        with fs.open(f"{base}/{nom}", "wb") as f:
            f.write(contenu)


@etape_chaine(
    succes="pousses",
    echec="echecs_push",
    libelle="push",
    cle_item="file_name",
)
def _pousser(decrypted_file: dict, partner_url: str) -> Iterator[dict]:
    """Étage push (discipline `etape_chaine`) : extrait (tous fichiers, pas seulement .xml —
    le relais est agnostique au contenu), pousse, puis enregistre livré APRÈS succès
    seulement — l'ajout à `zips_livrés` et le yield sont dans le corps du travail (pas dans
    la discipline) : toute exception avant ce point (extraction ou push) fait sauter les deux,
    la discipline compte l'échec et ne propage pas (zip retenté au run suivant).
    """
    zip_name = decrypted_file["file_name"]
    # file_extension="" : `str.endswith("")` est toujours vrai → extrait TOUT le
    # contenu du zip (xml et json compris), pas seulement les .xml.
    fichiers = extract_files_from_zip(decrypted_file["decrypted_content"], file_extension="")
    pousser_vers_partenaire(fichiers, partner_url)

    livres = dlt.current.resource_state(NOM_RESOURCE).setdefault("zips_livrés", [])
    livres.append(zip_name)
    yield {
        "zip": zip_name,
        "fichiers": [nom for nom, _ in fichiers],
        "statut": "pousse",
        "at": datetime.now(UTC).isoformat(),
    }


def _pousser_et_enregistrer(decrypted_file: dict, partner_url: str, stats: StatsRelais) -> Iterator[dict]:
    """Pré-compte le candidat (`stats.candidats`, hors discipline — symétrie avec
    `stats.fichiers` dans `crypto.py::_decrypt_aes_transformer_base`) puis délègue à `_pousser`."""
    stats.candidats += 1
    yield from _pousser(decrypted_file, partner_url, stats)  # stats : dernier positionnel (injecté)


def _create_push_transformer(partner_url: str, stats: StatsRelais):
    @dlt.transformer
    def push_et_enregistrer(decrypted_file: dict) -> Iterator[dict]:
        return _pousser_et_enregistrer(decrypted_file, partner_url, stats)

    return push_et_enregistrer


@dlt.source(name=NOM_PIPELINE)
def relais_source(
    source_url: str,
    partner_url: str,
    flux_filtres: set[str] | None,
    key_chain: list[tuple[str, bytes, bytes | None]],
    stats: StatsRelais,
):
    """Source dlt unique : listing (re-listé en entier, `incremental=False`) → filtre →
    déchiffrement → push+enregistrement. Une seule resource nommée `relais_livraisons`."""
    sftp_resource = create_sftp_resource("RELAIS", "relais", "**/*.zip", source_url, incremental=False)
    filtre = _create_filtre_transformer(flux_filtres)
    decrypt = create_decrypt_transformer(key_chain=key_chain)
    push = _create_push_transformer(partner_url, stats)

    pipeline_relais = (sftp_resource | filtre | decrypt | push).with_name(NOM_RESOURCE)
    pipeline_relais.apply_hints(write_disposition="append")
    yield pipeline_relais


def _pipeline_par_defaut(cfg: "runtime.Relais") -> "dlt.Pipeline":
    """Pipeline dlt par défaut, `pipelines_dir` épinglé à `destination_db.parent` (#643) :
    sans épingle, dlt tombe sur `~/.dlt/pipelines` — dépendant du HOME de l'utilisateur qui
    lance. Un seed lancé en root pendant que le timer tourne en `User=electricore-relais`
    écrirait alors dans un état et lirait dans un autre (amorçage silencieusement sans effet
    pour le timer, qui repousserait tout l'historique au run suivant)."""
    return dlt.pipeline(
        pipeline_name=NOM_PIPELINE,
        destination=dlt.destinations.duckdb(str(cfg.destination_db)),
        dataset_name=NOM_DATASET,
        pipelines_dir=str(cfg.destination_db.parent),
    )


def executer(pipeline: "dlt.Pipeline | None" = None) -> tuple["dlt.common.pipeline.LoadInfo", StatsRelais]:
    """Point d'entrée programmatique : construit (ou reçoit, pour les tests) un pipeline
    dlt dédié et l'exécute contre la config du domaine runtime `relais` (#637).

    Retourne `(info, stats)` — `stats.relais_aveugle()` gouverne l'escalade côté CLI
    (`__main__.py`, #643) : le caller décide s'il sort en erreur.
    """
    cfg = runtime.relais()
    key_chain = load_aes_key_chain()
    stats = StatsRelais()
    if pipeline is None:
        pipeline = _pipeline_par_defaut(cfg)
    info = pipeline.run(relais_source(cfg.source_url, cfg.partner_url, cfg.flux_filtres(), key_chain, stats))
    return info, stats


def _amorcer(zip_item: dict, flux_filtres: set[str] | None, avant: str) -> Iterator[dict]:
    """Étage amorçage (#643) : marque le zip comme livré SANS le pousser — utilisé
    uniquement par `seed_avant`, jamais par le run normal. Même comparaison de chaînes
    ISO-8601 que l'ancien filtre `depuis` (`modification_date` déjà normalisée en ISO
    string par `create_sftp_resource`)."""
    nom = zip_item["file_name"]
    if not _match_flux(nom, flux_filtres):
        return
    if zip_item["modification_date"] >= avant:
        return

    livres = dlt.current.resource_state(NOM_RESOURCE).setdefault("zips_livrés", [])
    livres.append(nom)
    yield {
        "zip": nom,
        "fichiers": [],
        "statut": "amorce",
        "at": datetime.now(UTC).isoformat(),
    }


def _create_amorce_transformer(flux_filtres: set[str] | None, avant: str):
    @dlt.transformer
    def amorcer(zip_item: dict) -> Iterator[dict]:
        return _amorcer(zip_item, flux_filtres, avant)

    return amorcer


@dlt.source(name=NOM_PIPELINE)
def _seed_source(source_url: str, flux_filtres: set[str] | None, avant: str):
    """Source dlt de l'amorçage : même `NOM_PIPELINE`/`NOM_RESOURCE` que `relais_source` —
    partage le même `resource_state` (`zips_livrés`), sinon le run normal repousserait tout
    ce que le seed vient de marquer livré."""
    sftp_resource = create_sftp_resource("RELAIS", "relais", "**/*.zip", source_url, incremental=False)
    amorce = _create_amorce_transformer(flux_filtres, avant)

    pipeline_seed = (sftp_resource | amorce).with_name(NOM_RESOURCE)
    pipeline_seed.apply_hints(write_disposition="append")
    yield pipeline_seed


def _journal_deja_peuple(db_path: Path) -> bool:
    """Le journal (`relais_livraisons`) contient-il déjà au moins une livraison (push ou
    amorce) ? Garde-fou de `seed_avant` : le journal et l'état `zips_livrés` sont toujours
    peuplés ENSEMBLE (même transaction dlt, `_pousser`/`_amorcer` ci-dessus) — un journal
    non vide implique donc un état non vide, sans avoir à lire l'état dlt hors contexte de run."""
    import duckdb

    if not db_path.exists():
        return False
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        (n,) = con.execute(f'select count(*) from "{NOM_DATASET}"."{NOM_RESOURCE}"').fetchone()
        return n > 0
    except duckdb.CatalogException:
        return False
    finally:
        con.close()


def seed_avant(avant: str, *, force: bool = False, pipeline: "dlt.Pipeline | None" = None):
    """Amorçage explicite (#643) : marque tous les zips de la source antérieurs à `avant`
    (date ISO `YYYY-MM-DD`, comparée au mtime SFTP) comme livrés SANS les pousser — évite de
    noyer le partenaire dans l'historique au tout premier run. Remplace l'ancien filtre
    `depuis` (retiré du domaine runtime `Relais`), qui mélangeait un acte unique d'amorçage
    avec un knob de config permanent.

    Refuse par défaut si le journal contient déjà des livraisons (`force=True` pour
    l'opérateur qui sait ce qu'il fait) : lancé par erreur après la mise en service d'un
    relais déjà en route, l'amorçage enterrerait silencieusement tout ce qui restait à
    relayer — c'est la seule opération du relais capable de fabriquer un « oubli définitif ».
    """
    cfg = runtime.relais()
    if not force and _journal_deja_peuple(cfg.destination_db):
        raise RuntimeError(
            "Amorçage refusé : le journal contient déjà des livraisons — lancer le seed "
            "maintenant enterrerait silencieusement tout ce qui n'a pas encore été relayé "
            "(#643). Relancer avec --force si c'est délibéré."
        )
    if pipeline is None:
        pipeline = _pipeline_par_defaut(cfg)
    return pipeline.run(_seed_source(cfg.source_url, cfg.flux_filtres(), avant))


def zips_non_relayes(source_url: str, db_path: Path) -> list[str]:
    """Vérification de complétude (#637) : zips de la source absents du journal de destination.

    Écart entre le listing source (fsspec, tous les `*.zip` récursivement) et la table
    `relais.relais_livraisons` (peuplée par `executer`/`seed_avant`) — une requête simple,
    indépendante du `resource_state` (qui gouverne seulement le skip du run suivant). Un zip
    amorcé (`statut='amorce'`) est dans cette table au même titre qu'un zip poussé — pas de
    filtre sur `statut`, il ne remonte donc pas ici (#643)."""
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
