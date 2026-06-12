"""Compare deux bases DuckDB table par table, par empreintes canoniques.

Outil de validation de bascule (ADR-0020) : après un run complet des deux pipelines
sur le même SFTP (legacy → flux_enedis.*, dbt → main.*), compare chaque table flux_*
en multiset d'empreintes — modulo traçabilité, représentation (instants heure-mur UTC,
nombres) et exclusions documentées par table.

Verdict du 11/06/2026 (corpus SFTP complet, ~700k lignes par côté) : parité totale
7/7 tables ; seuls extras legacy = re-livraisons double-comptées (128 R15, 261 F15).

Usage :
    uv run python -m electricore.ingestion.tools.comparaison_bases \
        <db_legacy> <schema_legacy> <db_dbt> <schema_dbt>
    # ex. /tmp/flux_legacy_full.duckdb flux_enedis /tmp/flux_dbt_full.duckdb main
"""

import sys
from collections import Counter

import duckdb
from electricore.ingestion.tools.comparaison_legacy_dbt import TRACABILITE, canonique

TABLES = ["flux_c15", "flux_r15", "flux_r15_acc", "flux_r151", "flux_f12_detail", "flux_f15_detail", "flux_r64"]

# R64 : les fenêtres de livraison se chevauchent — le même relevé (pdl, type, date,
# valeurs IDENTIQUES) arrive sous plusieurs id_demande. Le « gagnant » du merge legacy
# est arbitraire (ordre interne dlt), celui de dbt est déterministe (livraison la plus
# récente). Vérifié : 27876/27876 divergences portaient uniquement sur id_demande.
EXCLUSIONS_PAR_TABLE = {"flux_r64": {"id_demande"}}


def empreintes(db, schema, table):
    exclues = EXCLUSIONS_PAR_TABLE.get(table, set())
    con = duckdb.connect(db, read_only=True)
    try:
        cur = con.execute(f'select * from "{schema}"."{table}"')
    except duckdb.CatalogException:
        con.close()
        return None
    cols = [d[0] for d in cur.description]
    compte = Counter()
    while True:
        rows = cur.fetchmany(50000)
        if not rows:
            break
        for r in rows:
            compte[
                frozenset(
                    (k, canonique(v))
                    for k, v in zip(cols, r, strict=True)
                    if k not in TRACABILITE and k not in exclues and not k.startswith("_dlt") and v is not None
                )
            ] += 1
    con.close()
    return compte


db_l, sch_l, db_d, sch_d = sys.argv[1:5]
for table in TABLES:
    cl = empreintes(db_l, sch_l, table)
    cd = empreintes(db_d, sch_d, table)
    if cl is None or cd is None:
        print(f"{table}: absente ({'legacy' if cl is None else ''}{'dbt' if cd is None else ''})")
        continue
    inter = sum((cl & cd).values())
    print(
        f"{table}: legacy={sum(cl.values())} dbt={sum(cd.values())} identiques={inter} "
        f"seul_legacy={sum((cl - cd).values())} seul_dbt={sum((cd - cl).values())}"
    )
