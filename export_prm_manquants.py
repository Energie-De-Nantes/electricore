"""Exporte la liste des PRM communicants dont le relevé manque à une borne mensuelle
donnée (1er du mois), pour une demande R64 « index quotidiens » M023 sur le site Enedis.

Usage : uv run python export_prm_manquants.py 2026-03-01
"""

import sys

import duckdb
import polars as pl

DB = "/home/virgile/workspace/electricore/electricore/ingestion/flux_enedis_pipeline.duckdb"
ENTREE = ["CFNE", "MES", "PMES"]
SORTIE = ["RES", "CFNS"]

cible = pl.lit(sys.argv[1] if len(sys.argv) > 1 else "2026-03-01").str.to_date()

con = duckdb.connect(DB, read_only=True)
con.execute("SET TimeZone='Europe/Paris'")

c15 = con.execute(
    """
    select pdl, date_evenement, evenement_declencheur, niveau_ouverture_services
    from flux_enedis.flux_c15
    """
).pl()
c15 = c15.with_columns(
    pl.col("niveau_ouverture_services").cast(pl.Int8).alias("niveau"),
    pl.col("date_evenement").dt.date().alias("jour"),
)
MAX_DATA = c15["jour"].max()
cible_d = c15.select(cible.alias("d")).item(0, 0)

# Fenêtre active par PDL.
fenetres = (
    c15.with_columns(
        pl.col("evenement_declencheur").is_in(ENTREE).alias("_e"),
        pl.col("evenement_declencheur").is_in(SORTIE).alias("_s"),
    )
    .group_by("pdl")
    .agg(
        pl.col("jour").filter(pl.col("_e")).min().alias("entree"),
        pl.col("jour").min().alias("premier_evt"),
        pl.col("jour").filter(pl.col("_s")).max().alias("sortie"),
    )
    .with_columns(
        pl.coalesce("entree", "premier_evt").alias("debut"),
        pl.col("sortie").fill_null(MAX_DATA).alias("fin"),
    )
)

# PDL actifs à la borne cible, avec leur niveau (dernier événement ≤ cible).
evts = c15.select("pdl", "jour", "niveau").sort(["pdl", "jour"])
actifs = (
    fenetres.filter((pl.col("debut") <= cible_d) & (pl.col("fin") >= cible_d))
    .select("pdl")
    .with_columns(cible.alias("borne"))
    .sort(["pdl", "borne"])
    .join_asof(evts, left_on="borne", right_on="jour", by="pdl", strategy="backward")
    .filter(pl.col("niveau") >= 1)  # communicants
)

# Relevé présent exactement à la borne (1er du mois) ?
releves_borne = (
    con.execute("select distinct pdl, date_releve::date as jour from flux_enedis.releves")
    .pl()
    .filter(pl.col("jour") == cible)
    .select("pdl")
    .with_columns(pl.lit(True).alias("a_releve"))
)

manquants = (
    actifs.join(releves_borne, on="pdl", how="left")
    .filter(pl.col("a_releve").is_null())
    .select("pdl")
    .unique()
    .sort("pdl")
)

con.close()

n = manquants.height
print(f"Borne : {cible_d}")
print(f"PDL communicants actifs sans relevé au {cible_d} : {n}")
sortie_csv = f"prm_manquants_{cible_d}.csv"
manquants.write_csv(sortie_csv, include_header=False)
print(f"CSV écrit : {sortie_csv} ({n} PRM, un par ligne, sans en-tête)")
print("Aperçu :")
for p in manquants["pdl"].head(5).to_list():
    print(f"  {p}")
