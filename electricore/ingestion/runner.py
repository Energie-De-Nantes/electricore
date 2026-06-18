"""Ingestion des flux Enedis : SFTP → landing brut JSON → modèles dbt (ADR-0020).

Chemin complet du prototype dbt sur données réelles :
1. DLT déplace (SFTP, déchiffrement AES, unzip) et dépose chaque document intégral
   en colonne JSON dans `flux_raw.raw_<flux>` (source `flux_enedis_brut`) ;
2. `dbt build` matérialise les tables `main.flux_*` (staging + linéarisation + data
   tests not_null).

Usage :
    uv run python -m electricore.ingestion test            # 2 fichiers/flux
    uv run python -m electricore.ingestion all             # tout
    uv run python -m electricore.ingestion r151 c15        # sélection
    uv run python -m electricore.ingestion all --db /tmp/flux_dbt.duckdb

Chemin de production (#134) : la base par défaut est la base de prod
(`flux_enedis_pipeline.duckdb`), les modèles se matérialisent dans le schéma
`flux_enedis` (mêmes tables que l'ex-legacy → l'aval ne voit rien), le brut vit
dans `flux_raw`. `--db` permet une base jetable pour les validations.
"""

import argparse
import logging
import os
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import dlt
import yaml

from electricore.config import runtime
from electricore.ingestion.sources.sftp_enedis_brut import flux_enedis_brut
from electricore.ingestion.transformers.crypto import StatsDechiffrement

# DLT écrit sur stderr — on laisse faire, on ne lit que stdout
logging.disable(logging.CRITICAL)

ICI = Path(__file__).parent
PROJET_DBT = ICI / "dbt"


def chemin_db_defaut() -> Path:
    """Base cible par défaut : DUCKDB_PATH (.env compris, volume Docker via compose)
    sinon la base de prod locale — résolution partagée avec l'API et les loaders
    core via le registre runtime (`runtime.duckdb().chemin`, #141/#146)."""
    return runtime.duckdb().chemin


@contextmanager
def pont_dbt_duckdb(db_path: str | Path):
    """Seul pont os.environ → dbt (ADR-0025) : pose DBT_DUCKDB_PATH le temps de
    l'invocation in-process, restaure l'état antérieur ensuite (try/finally) pour
    ne pas survivre dans un éventuel process API. `env_var()` est l'unique
    mécanisme de paramétrage de `profiles.yml` côté dbt."""
    ancienne = os.environ.get("DBT_DUCKDB_PATH")
    os.environ["DBT_DUCKDB_PATH"] = str(db_path)
    try:
        yield
    finally:
        if ancienne is None:
            os.environ.pop("DBT_DUCKDB_PATH", None)
        else:
            os.environ["DBT_DUCKDB_PATH"] = ancienne


def _out(msg: str) -> None:
    print(msg, flush=True)


@dataclass(frozen=True)
class PlanRun:
    """Interprétation des arguments de flux du CLI (contrat partagé avec l'API)."""

    selection: list[str] | None  # None = tous les flux
    max_files: int | None
    refresh: str | None  # "drop_sources" = resync (état incrémental purgé, tout re-téléchargé)
    rebuild: bool = False  # True = saute le landing, dbt build seul (zéro réseau)


def interpreter_flux(flux: list[str], max_files: int | None) -> PlanRun:
    """Traduit les arguments de flux (modes API compris) en plan d'exécution.

    - 'all'     → tous les flux ;
    - 'test'    → tous les flux, 2 fichiers chacun (smoke) ;
    - 'rebuild' → re-matérialise les tables depuis le brut, zéro réseau (~13 s) —
                  le geste standard après un changement de modèle dbt (#140) ;
    - 'resync'  → état incrémental dlt purgé, tout re-téléchargé (brut perdu/corrompu) ;
    - 'reset'   → déprécié, alias de resync ;
    - sinon     → liste de flux (r151 c15 …), upper-casée vers les clés de flux.yaml.
    """
    if flux == ["all"]:
        return PlanRun(selection=None, max_files=max_files, refresh=None)
    if flux == ["test"]:
        return PlanRun(selection=None, max_files=max_files or 2, refresh=None)
    if flux == ["rebuild"]:
        return PlanRun(selection=None, max_files=max_files, refresh=None, rebuild=True)
    if flux in (["resync"], ["reset"]):
        return PlanRun(selection=None, max_files=max_files, refresh="drop_sources")
    return PlanRun(selection=[f.upper() for f in flux], max_files=max_files, refresh=None)


def lander_brut(db_path: Path, plan: PlanRun) -> dict[str, StatsDechiffrement]:
    """Étape 1 : SFTP → tables raw_<flux> (colonne JSON) dans flux_raw.

    Retourne les stats de déchiffrement agrégées par flux (escalade per-flux, ADR-0037) :
    le caller décide ensuite si un flux aveugle doit faire échouer le job.
    """
    config = yaml.safe_load((ICI / "config" / "flux.yaml").read_text())
    if plan.selection:
        config = {k: v for k, v in config.items() if k in plan.selection}

    stats: dict[str, StatsDechiffrement] = {}
    pipeline = dlt.pipeline(
        pipeline_name=f"flux_brut_{db_path.stem}",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
        progress="log",
    )
    info = pipeline.run(flux_enedis_brut(config, max_files=plan.max_files, stats=stats), refresh=plan.refresh)
    for paquet in info.load_packages:
        for job in paquet.jobs.get("completed_jobs", []):
            _out(f"  landé : {job.job_file_info.table_name}")
    return stats


def flux_sans_dechiffrement(stats: dict[str, StatsDechiffrement]) -> list[str]:
    """Flux aveugles : des fichiers, mais 0 déchiffrement réussi (≥ 1 échec) → clé manquante.

    Cœur de l'escalade per-flux (ADR-0037) : Enedis fait évoluer le chiffrement par flux
    indépendamment, donc on capte exactement le flux qui bascule seul, sans noyer dans un
    seuil global. Un échec isolé (autres fichiers OK) n'y figure pas — il est toléré.
    """
    return [flux for flux, s in stats.items() if s.flux_aveugle()]


# Modèles dbt servis par chaque table brute (raw_r15 sert deux linéarisations).
MODELES_PAR_RAW = {
    "raw_c15": ["flux_c15"],
    "raw_r15": ["flux_r15", "flux_r15_acc"],
    "raw_r151": ["flux_r151"],
    "raw_f12": ["flux_f12_detail"],
    "raw_f15": ["flux_f15_detail"],
    "raw_r64": ["flux_r64"],
    "raw_affaires": ["flux_affaires"],
}


def construire_dbt(db_path: Path) -> bool:
    """Étape 2 : dbt build (modèles + data tests), restreint aux tables brutes landées.

    La restriction --select évite l'échec sur les sources absentes (sélection
    partielle de flux, smoke avec max_files tombant sur des zips sans contenu utile
    comme les R64 de l'ère CSV).
    """
    import duckdb
    from dbt.cli.main import dbtRunner

    con = duckdb.connect(str(db_path), read_only=True)
    presentes = {t for (t,) in con.execute("select table_name from information_schema.tables").fetchall()}
    con.close()
    selection = [f"+{modele}" for raw, modeles in MODELES_PAR_RAW.items() if raw in presentes for modele in modeles]
    # Le modèle de relevés canonique `releves` (mart, ADR-0029) est un DESCENDANT des
    # flux : les `+flux_*` (ancêtres) ne l'atteignent pas. On l'ajoute dès que ses trois
    # sources sont matérialisables (C15 + périodiques R151/R64) — sinon un arm d'union
    # pointerait un flux non construit. On sélectionne `+releves` (et non `releves` nu) :
    # le graph operator tire TOUS ses ancêtres, dont l'adapter intermédiaire
    # `int_releves__c15` qui n'est pas un `flux_*` (sinon : Catalog Error « int_releves__c15
    # does not exist », cf. régression test_runner_construit_releves). Les flux ancêtres,
    # déjà dans `selection` via `+flux_*`, sont dédoublonnés par dbt.
    if {"raw_c15", "raw_r151", "raw_r64"} <= presentes:
        selection.append("+releves")
    if not selection:
        _out("  aucune table brute landée — rien à construire")
        return False

    with pont_dbt_duckdb(db_path):
        resultat = dbtRunner().invoke(
            [
                "build",
                "--select",
                *selection,
                "--project-dir",
                str(PROJET_DBT),
                "--profiles-dir",
                str(PROJET_DBT),
                "--target-path",
                str(db_path.parent / f".dbt_target_{db_path.stem}"),
            ]
        )
    success = bool(resultat.success)
    if not success:
        # Surfacer les nœuds en échec : sinon le runner masque l'erreur dbt réelle
        # (OOM, data test, SQL) et le diagnostic exige de relancer dbt à la main.
        try:
            res = getattr(resultat, "result", None)
            for noeud in getattr(res, "results", res) or []:
                statut = str(getattr(noeud, "status", "")).lower()
                if "error" in statut or "fail" in statut:
                    nom = getattr(getattr(noeud, "node", None), "name", "?")
                    msg = " ".join(str(getattr(noeud, "message", "")).split())
                    _out(f"  ✗ {nom} [{statut}] — {msg}")
        except Exception:  # noqa: BLE001 — le diagnostic ne doit jamais masquer l'échec
            pass
    return success


def bilan(db_path: Path) -> None:
    """Comptes par table — brut et matérialisé."""
    import duckdb

    # Lecture-écriture : dbt vient d'écrire dans le même process, une connexion
    # read_only aurait une config DuckDB incompatible.
    con = duckdb.connect(str(db_path))
    lignes = con.execute(
        """
        select table_schema, table_name from information_schema.tables
        where table_name like 'raw_%' or table_name like 'flux_%' or table_name = 'releves'
        order by table_schema, table_name
        """
    ).fetchall()
    for schema, table in lignes:
        if table.startswith("_dlt"):
            continue
        n = con.execute(f'select count(*) from "{schema}"."{table}"').fetchone()[0]
        _out(f"  {schema}.{table}: {n}")
    con.close()


def main() -> None:
    parseur = argparse.ArgumentParser(
        prog="python -m electricore.ingestion", description="Ingestion : SFTP → brut JSON → modèles dbt"
    )
    parseur.add_argument(
        "flux",
        nargs="+",
        help="'all', 'test' (2 fichiers/flux), 'rebuild' (dbt seul, zéro réseau), "
        "'resync' (re-télécharge tout) ou liste de flux",
    )
    parseur.add_argument(
        "--db", type=Path, default=None, help="base DuckDB cible (défaut : $DUCKDB_PATH ou la base de prod locale)"
    )
    parseur.add_argument("--max-files", type=int, default=None, help="limite de fichiers par flux")
    args = parseur.parse_args()

    plan = interpreter_flux(args.flux, args.max_files)

    # Fail-fast par point d'entrée (ADR-0025) : rebuild ne touche ni SFTP ni AES.
    if plan.rebuild:
        runtime.valider(runtime.duckdb)
    else:
        runtime.valider(runtime.sftp, runtime.aes, runtime.duckdb)

    args.db = args.db or chemin_db_defaut()

    aveugles: list[str] = []
    if plan.rebuild:
        _out(f"↻ Rebuild : re-matérialisation depuis le brut de {args.db} (zéro réseau)")
    else:
        debut = time.time()
        _out(f"🚀 Landing brut → {args.db}")
        stats = lander_brut(args.db, plan)
        aveugles = flux_sans_dechiffrement(stats)
        _out(f"⏱️  Landing : {time.time() - debut:.1f}s")

    debut = time.time()
    _out("🔨 dbt build")
    if not construire_dbt(args.db):
        # Si rien n'a été construit alors que des flux sont aveugles, la vraie cause est
        # la clé AES manquante, pas dbt — surfacer le bon diagnostic.
        if aveugles:
            _out(f"❌ Aucun déchiffrement réussi pour : {', '.join(aveugles)} (clé AES manquante ?)")
        else:
            _out("❌ dbt build a échoué")
        sys.exit(1)
    _out(f"⏱️  dbt : {time.time() - debut:.1f}s")

    _out("📊 Bilan")
    bilan(args.db)

    # Escalade per-flux (ADR-0037) : un flux qui a des fichiers mais 0 déchiffrement réussi
    # fait échouer le job — la surveillance bot alerte alors sur un job `failed`. Placé après
    # le dbt build pour que les flux sains continuent de couler malgré le flux aveugle.
    if aveugles:
        _out(f"❌ Flux sans aucun déchiffrement réussi (clé AES manquante ?) : {', '.join(aveugles)}")
        sys.exit(1)
    _out("✅ Terminé")
