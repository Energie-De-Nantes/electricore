"""Backfill manuel R67 depuis un ZIP portail M023 (clair — contourne l'AES SFTP).

Procédure batch rejouable (#543) : le portail SGE renvoie un ZIP clair contenant un
JSON R67 (mesures facturantes, ADR-0047). Ce script lande ce JSON dans
`flux_raw.raw_r67` avec la MÊME forme que le landing dlt (colonne `content` JSON +
`file_name` + `modification_date`), en MERGE sur `file_name` (idempotent : rejouer
n'ajoute pas de doublon). La linéarisation `flux_r67` se construit ensuite via le
runner :

    uv run python docs/spikes/nc/lander_r67_m023.py            # lande le(s) ZIP local/aux
    uv run python -m electricore.ingestion rebuild             # dbt build → flux_enedis.flux_r67

Chemin par défaut : les ZIP R67 du répertoire M023 local (non suivi par git, RGPD —
il contient des numéros de PDL). Passer un autre chemin en argument. Le script
n'imprime que des agrégats anonymes (nb PRM, nb mesures), jamais de PDL.

NB : le landing globe TOUS les ZIP R67 du dossier (ou le seul fichier passé en argument)
— il ne filtre pas sur une cohorte. Lander plusieurs demandes M023 peut donc amener des
PDL hors périmètre d'analyse ; c'est inoffensif (les spikes aval filtrent par cohorte),
mais `flux_r67` portera alors tous les PDL landés.

Repro : nécessite la base d'ingestion locale. Écrit dans la base (read/write).
"""

import os
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import duckdb

# Base d'ingestion (même défaut que les autres spikes NC / le runtime : $DUCKDB__PATH).
DB_DEFAUT = os.environ.get("DUCKDB__PATH", "electricore/ingestion/flux_enedis_pipeline.duckdb")

# Répertoire local des ZIP M023 (hors dépôt — RGPD). Override en argv[1].
SOURCE_DEFAUT = Path.home() / "data" / "r67_samples"

# Schéma identique à `raw_r64` (landing dlt) — seules content/file_name/modification_date
# sont lues par stg_r67 ; les colonnes _dlt_* / _source_zip sont conservées par parité.
DDL_RAW_R67 = """
create schema if not exists flux_raw;
create table if not exists flux_raw.raw_r67 (
    content json,
    file_name varchar,
    modification_date timestamptz,
    _source_zip varchar,
    _flux_type varchar,
    _dlt_load_id varchar,
    _dlt_id varchar
);
"""


def zips_r67(source: Path) -> list[Path]:
    if source.is_file() and source.suffix == ".zip":
        return [source]
    return sorted(source.glob("ENEDIS_R67_P_*.zip"))


def lander(db_path: Path, zips: list[Path]) -> int:
    con = duckdb.connect(str(db_path))
    con.execute(DDL_RAW_R67)
    n_landes = 0
    for z in zips:
        with zipfile.ZipFile(z) as zf:
            jsons = [n for n in zf.namelist() if n.lower().endswith(".json")]
            for nom in jsons:
                contenu = zf.read(nom).decode("utf-8")
                mtime = datetime.fromtimestamp(z.stat().st_mtime, tz=UTC)
                # MERGE sur file_name : rejouer remplace, n'accumule pas.
                con.execute("delete from flux_raw.raw_r67 where file_name = ?", [nom])
                con.execute(
                    """
                    insert into flux_raw.raw_r67
                        (content, file_name, modification_date, _source_zip, _flux_type, _dlt_load_id, _dlt_id)
                    values (?::json, ?, ?, ?, 'R67', 'backfill_m023', ?)
                    """,
                    [contenu, nom, mtime, z.name, nom],
                )
                n_landes += 1
    # Comptage de couverture (agrégat anonyme, aucun PDL imprimé).
    n_prm = con.execute(
        "select count(distinct m ->> '$.idPrm') "
        "from flux_raw.raw_r67, unnest(cast(content -> '$.mesures' as json[])) as t(m)"
    ).fetchone()[0]
    con.close()
    return n_landes, n_prm


if __name__ == "__main__":
    source = Path(sys.argv[1]) if len(sys.argv) > 1 else SOURCE_DEFAUT
    db = Path(DB_DEFAUT)
    zips = zips_r67(source)
    if not zips:
        print(f"Aucun ZIP R67 trouvé sous {source} — rien à lander.")
        sys.exit(0)
    n_landes, n_prm = lander(db, zips)
    print(f"Landé {n_landes} fichier(s) JSON R67 depuis {len(zips)} ZIP → flux_raw.raw_r67 ({db})")
    print(f"PRM distincts couverts : {n_prm}")
    print("→ construire flux_r67 :  uv run python -m electricore.ingestion rebuild")
