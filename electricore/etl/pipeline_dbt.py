"""Pipeline de production dbt : SFTP → landing brut JSON → modèles dbt (ADR-0020).

Chemin complet du prototype dbt sur données réelles :
1. DLT déplace (SFTP, déchiffrement AES, unzip) et dépose chaque document intégral
   en colonne JSON dans `flux_raw.raw_<flux>` (source `flux_enedis_brut`) ;
2. `dbt build` matérialise les tables `main.flux_*` (staging + linéarisation + data
   tests not_null).

Usage :
    uv run python electricore/etl/pipeline_dbt.py test            # 2 fichiers/flux
    uv run python electricore/etl/pipeline_dbt.py all             # tout
    uv run python electricore/etl/pipeline_dbt.py r151 c15        # sélection
    uv run python electricore/etl/pipeline_dbt.py all --db /tmp/flux_dbt.duckdb

La base par défaut est séparée de la production legacy
(`flux_enedis_dbt.duckdb` vs `flux_enedis_pipeline.duckdb`) : les deux chemins
peuvent tourner côte à côte pendant la période de validation.
"""

# Charger .env avant que DLT lise ses secrets depuis os.environ (SFTP__URL, AES__*).
import os as _os
from pathlib import Path as _Path

for _c in [_Path(".env"), _Path(__file__).parents[2] / ".env"]:
    if _c.exists():
        with open(_c) as _f:
            for _l in _f:
                _l = _l.strip()
                if _l and not _l.startswith("#") and "=" in _l:
                    _k, _, _v = _l.partition("=")
                    _os.environ.setdefault(_k.strip(), _v.strip())
        break

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import dlt
import yaml

from electricore.etl.sources.sftp_enedis_brut import flux_enedis_brut

# DLT écrit sur stderr — on laisse faire, on ne lit que stdout
logging.disable(logging.CRITICAL)

ICI = Path(__file__).parent
DB_DEFAUT = ICI / "flux_enedis_dbt.duckdb"
PROJET_DBT = ICI / "dbt"


def _out(msg: str) -> None:
    print(msg, flush=True)


def lander_brut(db_path: Path, flux_selection: list[str] | None, max_files: int | None) -> None:
    """Étape 1 : SFTP → tables raw_<flux> (colonne JSON) dans flux_raw."""
    config = yaml.safe_load((ICI / "config" / "flux.yaml").read_text())
    if flux_selection:
        config = {k: v for k, v in config.items() if k in flux_selection}

    pipeline = dlt.pipeline(
        pipeline_name=f"flux_brut_{db_path.stem}",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="flux_raw",
        progress="log",
    )
    info = pipeline.run(flux_enedis_brut(config, max_files=max_files))
    for paquet in info.load_packages:
        for job in paquet.jobs.get("completed_jobs", []):
            _out(f"  landé : {job.job_file_info.table_name}")


# Modèles dbt servis par chaque table brute (raw_r15 sert deux linéarisations).
MODELES_PAR_RAW = {
    "raw_c15": ["flux_c15"],
    "raw_r15": ["flux_r15", "flux_r15_acc"],
    "raw_r151": ["flux_r151"],
    "raw_f12": ["flux_f12_detail"],
    "raw_f15": ["flux_f15_detail"],
    "raw_r64": ["flux_r64"],
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
    if not selection:
        _out("  aucune table brute landée — rien à construire")
        return False

    os.environ["DBT_DUCKDB_PATH"] = str(db_path)
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
    return bool(resultat.success)


def bilan(db_path: Path) -> None:
    """Comptes par table — brut et matérialisé."""
    import duckdb

    # Lecture-écriture : dbt vient d'écrire dans le même process, une connexion
    # read_only aurait une config DuckDB incompatible.
    con = duckdb.connect(str(db_path))
    lignes = con.execute(
        """
        select table_schema, table_name from information_schema.tables
        where table_name like 'raw_%' or table_name like 'flux_%'
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
    parseur = argparse.ArgumentParser(description="Pipeline dbt : SFTP → brut JSON → modèles dbt")
    parseur.add_argument("flux", nargs="+", help="'all', 'test' (2 fichiers/flux) ou liste de flux (r151 c15 …)")
    parseur.add_argument("--db", type=Path, default=DB_DEFAUT, help=f"base DuckDB cible (défaut : {DB_DEFAUT})")
    parseur.add_argument("--max-files", type=int, default=None, help="limite de fichiers par flux")
    args = parseur.parse_args()

    selection: list[str] | None
    max_files = args.max_files
    if args.flux == ["all"]:
        selection = None
    elif args.flux == ["test"]:
        selection = None
        max_files = max_files or 2
    else:
        selection = [f.upper() for f in args.flux]

    debut = time.time()
    _out(f"🚀 Landing brut → {args.db}")
    lander_brut(args.db, selection, max_files)
    _out(f"⏱️  Landing : {time.time() - debut:.1f}s")

    debut = time.time()
    _out("🔨 dbt build")
    if not construire_dbt(args.db):
        _out("❌ dbt build a échoué")
        sys.exit(1)
    _out(f"⏱️  dbt : {time.time() - debut:.1f}s")

    _out("📊 Bilan")
    bilan(args.db)
    _out("✅ Terminé")


if __name__ == "__main__":
    main()
