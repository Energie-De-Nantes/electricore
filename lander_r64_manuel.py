"""Lande manuellement les R64 du portail M023 (ZIP clairs, non chiffrés → contourne le
blocage AES-256 de la voie SFTP) dans la DB de test, puis rebuild dbt (flux_r64 → releves).

Réutilise les primitives du runner (même pipeline_name, merge sur file_name → upsert sûr,
les 32 docs existants restent). Usage : uv run --extra ingestion --extra dbt python lander_r64_manuel.py
"""

import json
import zipfile
from pathlib import Path

import dlt
import duckdb

from electricore.ingestion.raw_landing import lander_documents_bruts
from electricore.ingestion.runner import construire_dbt

DB = Path("/home/virgile/workspace/electricore/electricore/ingestion/flux_enedis_pipeline.duckdb")
G = Path("/home/virgile/Documents/guides_flux")
ZIPS = [
    "ENEDIS_R64_P_INDEX_M0AG4XG2_00001_20260617170022.zip",
    "ENEDIS_R64_P_INDEX_M0AG4XQL_00001_20260617170022.zip",
]


def compte(label):
    con = duckdb.connect(str(DB), read_only=True)
    con.execute("SET TimeZone='Europe/Paris'")
    raw = con.execute("select count(*) from flux_raw.raw_r64").fetchone()[0]
    r64 = con.execute("select count(*) from flux_enedis.releves where source='flux_R64'").fetchone()[0]
    con.close()
    print(f"[{label}] raw_r64 docs={raw} | releves R64={r64}")


compte("AVANT")

docs = []
for zf in ZIPS:
    z = zipfile.ZipFile(G / zf)
    name = z.namelist()[0]
    docs.append({"file_name": name, "modification_date": "2026-06-17T17:00:22", "content": json.loads(z.read(name))})
    print(f"  préparé : {name}")

pipeline = dlt.pipeline(
    pipeline_name=f"flux_brut_{DB.stem}",
    destination=dlt.destinations.duckdb(str(DB)),
    dataset_name="flux_raw",
)
lander_documents_bruts(pipeline, "raw_r64", docs)
print("landing OK")

ok = construire_dbt(DB)
print("dbt build OK:", ok)

compte("APRÈS")
