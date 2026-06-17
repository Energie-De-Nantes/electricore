"""Spike de faisabilité #315 — statut de communication sur données prod.

Non mergé : produit les chiffres pour l'ADR-0036 (#316). On ne mesure pas un délai
de flux, mais l'**alignement structurel** : distribution des niveaux, bascules,
alignement relevé-à-la-borne ⇆ niveau, population d'anomalies, seuil ≥1 vs ≥2 ;
et le caveat #314 (le CMAT d'activation entre-t-il dans la chronologie via
`impacte_energie` ?) tranché sur les vraies données.

Usage : uv run python spike_315.py
"""

import duckdb
import polars as pl

from electricore.core.loaders import c15 as c15_loader
from electricore.core.pipelines.historique import detecter_points_de_rupture

DB = "/home/virgile/workspace/electricore/electricore/ingestion/flux_enedis_pipeline.duckdb"
ENTREE = ["CFNE", "MES", "PMES"]
SORTIE = ["RES", "CFNS"]


def section(titre: str) -> None:
    print(f"\n{'=' * 78}\n{titre}\n{'=' * 78}")


con = duckdb.connect(DB, read_only=True)
con.execute("SET TimeZone='Europe/Paris'")

c15 = con.execute(
    """
    select pdl, ref_situation_contractuelle, date_evenement, evenement_declencheur,
           niveau_ouverture_services,
           avant_id_calendrier_distributeur, apres_id_calendrier_distributeur,
           avant_index_base_kwh, apres_index_base_kwh
    from flux_enedis.flux_c15
    """
).pl()

# Niveau en entier pour comparaisons (monotonie, seuils).
c15 = c15.with_columns(
    pl.col("niveau_ouverture_services").cast(pl.Int8).alias("niveau"),
    pl.col("date_evenement").dt.date().alias("jour"),
)

MAX_DATA = c15["jour"].max()

# =============================================================================
# 1. Distribution {0,1,2} sur le parc (niveau courant = dernier événement par PDL)
# =============================================================================
section("1. Distribution du niveau d'ouverture sur le parc (dernier niveau / PDL)")

parc = c15.sort("date_evenement").group_by("pdl").agg(pl.col("niveau").last().alias("niveau_courant"))
n_pdl = parc.height
dist = parc.group_by("niveau_courant").len().sort("niveau_courant")
print(f"PDL distincts : {n_pdl}")
for row in dist.iter_rows(named=True):
    print(f"  niveau {row['niveau_courant']} : {row['len']:5d}  ({100 * row['len'] / n_pdl:5.1f} %)")
comm1 = parc.filter(pl.col("niveau_courant") >= 1).height
comm2 = parc.filter(pl.col("niveau_courant") >= 2).height
print(f"Communicant (≥1) : {comm1} ({100 * comm1 / n_pdl:.1f} %)   |   (≥2) : {comm2} ({100 * comm2 / n_pdl:.1f} %)")

# =============================================================================
# 2. Bascules 0→≥1 et part non-monotone
# =============================================================================
section("2. Bascules de niveau et part non-monotone (par PDL)")

# Séquence temporelle de niveau par PDL (dédupliquée des répétitions consécutives).
seqs = c15.sort(["pdl", "date_evenement"]).group_by("pdl", maintain_order=True).agg(pl.col("niveau").alias("niveaux"))


def analyse_seq(niveaux: list[int]) -> dict:
    # Compacter les répétitions consécutives.
    compact = []
    for n in niveaux:
        if not compact or compact[-1] != n:
            compact.append(n)
    a_change = len(compact) > 1
    monte_0_vers_ge1 = any(
        compact[i] == 0 and compact[j] >= 1 for i in range(len(compact)) for j in range(i + 1, len(compact))
    )
    non_monotone = any(compact[i] > compact[i + 1] for i in range(len(compact) - 1))
    return {"a_change": a_change, "bascule_0_ge1": monte_0_vers_ge1, "non_monotone": non_monotone}


analyses = [analyse_seq(n) for n in seqs["niveaux"].to_list()]
a_change = sum(a["a_change"] for a in analyses)
bascule = sum(a["bascule_0_ge1"] for a in analyses)
non_mono = sum(a["non_monotone"] for a in analyses)
print(f"PDL dont le niveau change au moins une fois : {a_change} ({100 * a_change / n_pdl:.1f} %)")
print(f"PDL à bascule 0 → ≥1                        : {bascule} ({100 * bascule / n_pdl:.1f} %)")
print(f"PDL non-monotones (niveau qui redescend)    : {non_mono} ({100 * non_mono / n_pdl:.1f} %)")
if a_change:
    print(f"  → part non-monotone parmi les PDL qui changent : {100 * non_mono / a_change:.1f} %")

# =============================================================================
# 3. CAVEAT #314 — le CMAT d'activation entre-t-il via impacte_energie ?
# =============================================================================
section("3. Caveat #314 : impacte_energie sur les vrais événements CMAT")

hist = c15_loader(database_path=DB).collect().lazy()
enrichi = detecter_points_de_rupture(hist).collect()
cmat = enrichi.filter(pl.col("evenement_declencheur") == "CMAT")
print(f"Événements CMAT : {cmat.height}")
print("impacte_energie sur les CMAT :")
for row in cmat.group_by("impacte_energie").len().sort("impacte_energie").iter_rows(named=True):
    print(f"  impacte_energie={row['impacte_energie']} : {row['len']}")
print("\nVentilé par forme du calendrier avant→après :")
forme = (
    cmat.with_columns(
        pl.col("avant_id_calendrier_distributeur").is_null().alias("avant_null"),
        pl.col("apres_id_calendrier_distributeur").is_null().alias("apres_null"),
    )
    .group_by(["avant_null", "apres_null", "impacte_energie"])
    .len()
    .sort("len", descending=True)
)
for row in forme.iter_rows(named=True):
    print(
        f"  avant_null={row['avant_null']!s:5} apres_null={row['apres_null']!s:5} "
        f"impacte_energie={row['impacte_energie']!s:5} : {row['len']}"
    )

# Combien de CMAT portent un relevé C15 dans le mart, et combien entrent en chronologie
# (= impacte_energie True, car le semi-join est filtré dessus dans pipeline_energie).
cmat_entrent = cmat.filter(pl.col("impacte_energie")).height
print(f"\nCMAT dont le relevé entre dans la chronologie (impacte_energie=True) : {cmat_entrent} / {cmat.height}")

# =============================================================================
# 4. Alignement relevé-à-la-borne ⇆ niveau (par PDL × 1er du mois actif)
# =============================================================================
section("4. Alignement présence-d'un-relevé-au-1er ⇆ niveau (grain PDL × mois)")

# Fenêtre active par PDL : entrée = 1er événement d'entrée (sinon 1er événement),
# sortie = dernier événement de sortie (sinon dernière date des données).
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

# 1ers du mois dans [debut, fin].
bornes = (
    fenetres.with_columns(
        pl.col("debut").dt.month_start().alias("m0"),
        pl.col("fin").dt.month_start().alias("m1"),
    )
    .with_columns(pl.date_ranges("m0", "m1", interval="1mo").alias("borne"))
    .explode("borne")
    .filter((pl.col("borne") >= pl.col("debut")) & (pl.col("borne") <= pl.col("fin")))
    .select("pdl", "borne")
)

# Niveau à la borne : asof backward (dernier événement ≤ borne).
evts = c15.select("pdl", "jour", "niveau").sort(["pdl", "jour"])
bornes = bornes.sort(["pdl", "borne"]).join_asof(evts, left_on="borne", right_on="jour", by="pdl", strategy="backward")

# Présence d'un relevé au 1er, meilleure source (priorité C15 > R64 > R151).
rang = pl.when(pl.col("source") == "flux_C15").then(0).when(pl.col("source") == "flux_R64").then(1).otherwise(2)
releves_raw = con.execute("select pdl, date_releve::date as jour, source from flux_enedis.releves").pl()
releves_jour = (
    releves_raw.with_columns(rang.alias("_rang"))
    .sort(["pdl", "jour", "_rang"])
    .group_by(["pdl", "jour"], maintain_order=True)
    .agg(pl.col("source").first().alias("source_borne"))
)

# Présence "dans le mois calendaire de la borne" : distingue un trou de collecte réel
# d'un simple décalage de date (le pipeline matche à ±4 h du 1er, mesure exacte ci-dessus).
mois_presence = (
    releves_raw.select("pdl", pl.col("jour").dt.truncate("1mo").alias("borne"))
    .unique()
    .with_columns(pl.lit(True).alias("releve_dans_le_mois"))
)

aligne = (
    bornes.drop("jour")
    .join(releves_jour, left_on=["pdl", "borne"], right_on=["pdl", "jour"], how="left")
    .join(mois_presence, on=["pdl", "borne"], how="left")
    .with_columns(
        pl.col("source_borne").is_not_null().alias("releve_present"),
        pl.col("releve_dans_le_mois").fill_null(False),
        (pl.col("niveau") >= 1).alias("communicant"),
    )
    .filter(pl.col("niveau").is_not_null())
)

total_bornes = aligne.height
print(f"Bornes (PDL × 1er du mois actif) analysées : {total_bornes}")
print("\nPrésence d'un relevé au 1er, par niveau :")
tab = aligne.group_by("niveau", "releve_present").len().sort(["niveau", "releve_present"])
for row in tab.iter_rows(named=True):
    print(f"  niveau {row['niveau']}  relevé_present={row['releve_present']!s:5} : {row['len']}")

print("\nSource du relevé à la borne (toutes bornes avec relevé) :")
for row in (
    aligne.filter(pl.col("releve_present"))
    .group_by("source_borne")
    .len()
    .sort("len", descending=True)
    .iter_rows(named=True)
):
    print(f"  {row['source_borne']} : {row['len']}")

# =============================================================================
# 5. Population d'anomalies + seuil ≥1 vs ≥2
# =============================================================================
section("5. Anomalies (communicant ∧ relevé absent) et seuil ≥1 vs ≥2")

for seuil in (1, 2):
    comm = aligne.filter(pl.col("niveau") >= seuil)
    nc = aligne.filter(pl.col("niveau") < seuil)
    comm_n = comm.height
    comm_avec = comm.filter(pl.col("releve_present")).height
    anomalies = comm.filter(~pl.col("releve_present")).height
    nc_n = nc.height
    nc_absent = nc.filter(~pl.col("releve_present")).height
    print(f"\nSeuil communicant ≥ {seuil} :")
    print(f"  bornes communicantes : {comm_n}")
    print(f"    avec relevé au 1er : {comm_avec} ({100 * comm_avec / comm_n:.1f} %)  ← alignement positif")
    print(
        f"    SANS relevé au 1er (anomalie communicant∧incalculable) : {anomalies} ({100 * anomalies / comm_n:.2f} %)"
    )
    if nc_n:
        print(
            f"  bornes non-communicantes : {nc_n} ; sans relevé (attendu) : {nc_absent} ({100 * nc_absent / nc_n:.1f} %)"
        )

# Le « 25 % sans relevé au 1er » : trou de collecte réel, ou simple décalage de date ?
section("4bis. Caractérisation des bornes communicantes SANS relevé exact au 1er")
comm = aligne.filter(pl.col("communicant"))
sans_1er = comm.filter(~pl.col("releve_present"))
sans_1er_mais_mois = sans_1er.filter(pl.col("releve_dans_le_mois")).height
sans_du_tout = sans_1er.filter(~pl.col("releve_dans_le_mois")).height
print(f"Bornes communicantes sans relevé au 1er : {sans_1er.height}")
print(f"  …mais un relevé existe ailleurs dans le mois (décalage de date) : {sans_1er_mais_mois}")
print(f"  …aucun relevé du tout dans le mois (trou de collecte réel)      : {sans_du_tout}")
print("\nAnomalies 'aucun relevé dans le mois' par année (artefact de montée en charge ?) :")
temporel = (
    sans_1er.filter(~pl.col("releve_dans_le_mois"))
    .with_columns(pl.col("borne").dt.year().alias("annee"))
    .group_by("annee")
    .len()
    .sort("annee")
)
for row in temporel.iter_rows(named=True):
    print(f"  {row['annee']} : {row['len']}")
print("\nBornes communicantes — relevé présent quelque part dans le mois :")
dans_mois = comm.filter(pl.col("releve_dans_le_mois")).height
print(f"  {dans_mois} / {comm.height} ({100 * dans_mois / comm.height:.1f} %)")

# Concentration : par PDL communicant, part de mois sans relevé. Distingue un parc
# « globalement couvert avec quelques trous » d'un sous-ensemble de PDL « noirs ».
section("4ter. Concentration des trous (par PDL communicant) et effet mois récents")
par_pdl = (
    comm.group_by("pdl")
    .agg(pl.len().alias("mois"), pl.col("releve_dans_le_mois").sum().alias("mois_avec"))
    .with_columns((pl.col("mois") - pl.col("mois_avec")).alias("mois_sans"))
)
pdl_noirs = par_pdl.filter(pl.col("mois_avec") == 0).height
pdl_pleins = par_pdl.filter(pl.col("mois_sans") == 0).height
print(f"PDL communicants : {par_pdl.height}")
print(f"  jamais aucun relevé (PDL 'noirs')         : {pdl_noirs}")
print(f"  toujours couverts (aucun mois sans relevé): {pdl_pleins}")
print(f"  partiellement couverts                    : {par_pdl.height - pdl_noirs - pdl_pleins}")
print("\nTaux de trou (mois sans relevé / mois communicants) par mois de borne, 12 derniers mois :")
parm = (
    comm.group_by("borne")
    .agg(pl.len().alias("n"), (~pl.col("releve_dans_le_mois")).sum().alias("sans"))
    .sort("borne")
    .tail(12)
)
for row in parm.iter_rows(named=True):
    print(f"  {row['borne']} : {row['sans']:4d}/{row['n']:4d}  ({100 * row['sans'] / row['n']:5.1f} % sans)")

con.close()
