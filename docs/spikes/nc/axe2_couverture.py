"""Spike #545 (PRD #542, axe 2) — couverture data par NC → source de vérité moisniversaire.

SCOPE PARTIEL (volet R15/F15/C15) : le R67 n'est pas encore en base (campagne M023 #543
pas atterrie) — la section 4 le constate proprement et tolère l'arrivée future de la
table. Le verdict V2 (section 7) est donc explicitement PROVISOIRE : il sera révisé une
fois le R67 intégré.

Repro : uv run python docs/spikes/nc/axe2_couverture.py
(lecture seule ; nécessite la base d'ingestion locale peuplée en C15/R15/F15.)

RGPD : ce script n'imprime QUE des agrégats. Le détail par PDL part en sortie locale
(sorties-locales/axe2_couverture_nc.csv, non committée). Une assertion finale scanne le
rapport committé à la recherche de motifs de 14 chiffres (PDL) et échoue si trouvé.
"""

import re
from pathlib import Path

import duckdb
import polars as pl

DB = "electricore/ingestion/flux_enedis_pipeline.duckdb"
RAPPORT = Path("docs/spikes/nc/axe2-couverture.md")
DETAIL_CSV = Path("sorties-locales/axe2_couverture_nc.csv")

con = duckdb.connect(DB, read_only=True)
con.execute("use flux_enedis")

# Présence au périmètre (ADR-0052) : RSC sans événement de sortie {RES, CFNS}.
# NC : dernier niveau_ouverture_services non-nul par RSC = '0'.
NC_PDL_CTE = """
with presence as (
    select ref_situation_contractuelle rsc, any_value(pdl) pdl,
           max(case when evenement_declencheur in ('RES', 'CFNS') then 1 else 0 end) a_sorti
    from spine_contrat
    where ref_situation_contractuelle is not null and type_fait = 'evenement'
    group by 1
),
present as (
    select rsc, pdl from presence where a_sorti = 0
),
dernier_niveau as (
    select ref_situation_contractuelle rsc, niveau_ouverture_services as niveau
    from spine_contrat
    where ref_situation_contractuelle is not null
    qualify row_number() over (partition by ref_situation_contractuelle order by date_evenement desc) = 1
),
nc_pdl as (
    select distinct p.pdl
    from present p join dernier_niveau d using (rsc)
    where d.niveau = '0'
)
"""


def q(sql: str):
    return con.execute(sql).fetchall()


def qdf(sql: str) -> pl.DataFrame:
    return con.sql(sql).pl()


def cadran_depuis_libelle(libelle: str) -> str:
    """Extrait le cadran du libellé F15 ('.../Part variable HPH' -> 'hph', sans suffixe -> 'base')."""
    for suffixe, cadran in [
        ("HPH", "hph"),
        ("HPB", "hpb"),
        ("HCH", "hch"),
        ("HCB", "hcb"),
        ("HP", "hp"),
        ("HC", "hc"),
    ]:
        if libelle.endswith(suffixe):
            return cadran
    return "base"


# === 0. Cohorte NC ===================================================================
print("=== 0. Cohorte NC (présence ADR-0052 + niveau_ouverture_services='0') ===")
nb_nc = q(NC_PDL_CTE + "select count(*) from nc_pdl")[0][0]
print(f"PDL NC : {nb_nc}")
assert nb_nc == 25, f"cohorte NC attendue = 25 (calibrage 2026-07-03), obtenu {nb_nc} — la base a bougé ?"

# === 1. R15 cyclique ==================================================================
print("\n=== 1. R15 cyclique ===")
r15_cov = q(NC_PDL_CTE + "select count(distinct r.pdl) from flux_r15 r join nc_pdl using (pdl)")[0][0]
print(f"PDL NC avec ≥1 relevé R15 : {r15_cov}/{nb_nc}")

ecarts = qdf(
    NC_PDL_CTE
    + """
    select r.pdl, r.date_releve,
           date_diff('day', lag(r.date_releve) over (partition by r.pdl order by r.date_releve), r.date_releve) as ecart_jours
    from flux_r15 r join nc_pdl using (pdl)
    where r.motif_releve = 'CYCL'
    """
).drop_nulls("ecart_jours")

assert (ecarts["ecart_jours"] > 0).all(), (
    "INVARIANT violé : relevés CYCL non strictement croissants (doublon de date ?)"
)

print(
    f"Écart entre relevés CYCL : médian {ecarts['ecart_jours'].median():.0f} j "
    f"(min {ecarts['ecart_jours'].min()}, max {ecarts['ecart_jours'].max()}, n={ecarts.height} intervalles)"
)
nb_trous_40j = ecarts.filter(pl.col("ecart_jours") > 40).height
print(
    f"Intervalles > 40 j (cadence bimestrielle ou plus lâche) : {nb_trous_40j}/{ecarts.height} ({nb_trous_40j / ecarts.height:.0%})"
)

nature = qdf(
    NC_PDL_CTE
    + """
    select nature_index, count(*) as n from flux_r15 r join nc_pdl using (pdl)
    where motif_releve = 'CYCL' group by 1
    """
)
print("Nature d'index des relevés CYCL :", dict(zip(nature["nature_index"], nature["n"], strict=True)))

jours = qdf(
    NC_PDL_CTE
    + """
    select r.pdl, extract(day from r.date_releve) as jour
    from flux_r15 r join nc_pdl using (pdl) where motif_releve = 'CYCL'
    """
)
stab = (
    jours.group_by("pdl").agg(pl.col("jour").std().alias("stddev_jour"), pl.len().alias("n")).filter(pl.col("n") >= 3)
)
print(
    f"Stabilité du jour de relevé (moisniversaire), PDL avec ≥3 relevés CYCL (n={stab.height}/{nb_nc}) : "
    f"stddev médian {stab['stddev_jour'].median():.1f} j, max {stab['stddev_jour'].max():.1f} j"
)

# === 2. F15 ============================================================================
print("\n=== 2. F15 ===")
f15_cov = q(NC_PDL_CTE + "select count(distinct f.pdl) from flux_f15_detail f join nc_pdl using (pdl)")[0][0]
print(f"PDL NC avec ≥1 ligne F15 : {f15_cov}/{nb_nc}")

fenetres = qdf(
    NC_PDL_CTE
    + """
    select distinct f.pdl, f.date_debut, f.date_fin
    from flux_f15_detail f join nc_pdl using (pdl)
    where f.nature_ev = '01' and f.unite = 'kWh' and f.type_facturation = 'CYCL'
    """
).sort(["pdl", "date_debut"])

fenetres = fenetres.with_columns(pl.col("date_fin").shift(1).over("pdl").alias("fin_precedente"))
chevauchements = fenetres.filter(pl.col("date_debut") < pl.col("fin_precedente")).height
adjacentes = fenetres.filter(pl.col("date_debut") == pl.col("fin_precedente")).height
trous = fenetres.filter(pl.col("date_debut") > pl.col("fin_precedente")).height

print(f"Fenêtres F15 CYCL (kWh) : {fenetres.height} sur {f15_cov} PDL")
print(
    f"  paires consécutives : {adjacentes + trous + chevauchements} — adjacentes {adjacentes}, trous {trous}, chevauchements {chevauchements}"
)
assert chevauchements == 0, "INVARIANT violé : chevauchement de fenêtres F15 CYCL détecté"

durees = fenetres.with_columns((pl.col("date_fin") - pl.col("date_debut")).dt.total_days().alias("duree_j"))
print(
    f"Durée des fenêtres F15 : médiane {durees['duree_j'].median():.0f} j (min {durees['duree_j'].min()}, max {durees['duree_j'].max()})"
)

# Réconciliation somme-des-fenêtres vs total : par construction (0 recouvrement), la somme
# des durées de fenêtres + la somme des trous doit égaler l'amplitude totale par PDL.
recon = (
    fenetres.group_by("pdl")
    .agg(
        (pl.col("date_fin") - pl.col("date_debut")).dt.total_days().sum().alias("jours_couverts"),
        pl.col("date_debut").min().alias("debut_span"),
        pl.col("date_fin").max().alias("fin_span"),
    )
    .with_columns((pl.col("fin_span") - pl.col("debut_span")).dt.total_days().alias("amplitude"))
)
ecart_reconciliation = recon.filter(pl.col("jours_couverts") > pl.col("amplitude")).height
print(
    f"Réconciliation somme-des-fenêtres ≤ amplitude totale : {recon.height - ecart_reconciliation}/{recon.height} PDL conformes"
)
assert ecart_reconciliation == 0, "INVARIANT violé : somme des fenêtres F15 > amplitude totale observée sur le PDL"

# === 3. C15 — index aux événements ====================================================
print("\n=== 3. C15 — index aux événements ===")
c15_evt = q(NC_PDL_CTE + "select count(*), count(distinct c.pdl) from flux_c15 c join nc_pdl using (pdl)")[0]
print(f"Événements C15 pour NC : {c15_evt[0]} lignes sur {c15_evt[1]}/{nb_nc} PDL")

c15_idx = q(NC_PDL_CTE + "select count(*), count(distinct c.pdl) from int_releves__c15 c join nc_pdl using (pdl)")[0]
print(
    f"...dont porteurs d'un index (avant/après non-null, via int_releves__c15) : {c15_idx[0]} lignes, {c15_idx[1]}/{nb_nc} PDL"
)

evt_types = q(
    NC_PDL_CTE
    + "select evenement_declencheur, count(*) from flux_c15 c join nc_pdl using (pdl) group by 1 order by 2 desc"
)
print("Types d'événement C15 observés chez les NC :", {e: n for e, n in evt_types})
if c15_idx[0] == 0:
    print("→ 0 index C15 chez les NC : les événements observés (entrée CFNE/MES, bascule niveau MDPRM/MDACT)")
    print("  ne portent structurellement pas d'index avant/après sur ce corpus.")

# === 4. R67 — en attente campagne M023 #543 ===========================================
print("\n=== 4. R67 — en attente campagne M023 (#543) ===")
r67_existe = (
    q(
        """
    select count(*) from information_schema.tables
    where table_schema = 'flux_enedis' and table_name = 'flux_r67'
    """
    )[0][0]
    > 0
)
if not r67_existe:
    print("Table flux_r67 absente de la base — campagne M023 #543 pas encore atterrie.")
    print("Volet R67 DIFFÉRÉ (voir #545, blocked-by #543) : ce script tolère l'arrivée future de la table.")
else:
    r67_cov = q(NC_PDL_CTE + "select count(distinct r.pdl) from flux_r67 r join nc_pdl using (pdl)")[0][0]
    print(f"Table flux_r67 présente : couverture NC {r67_cov}/{nb_nc}")

# === 5. Fréquence des relevés Réels (relève physique) =================================
print("\n=== 5. Fréquence des relevés Réels chez les NC ===")
reels = qdf(
    NC_PDL_CTE
    + """
    select r.pdl, count(*) as n from flux_r15 r join nc_pdl using (pdl)
    where nature_index = 'réel' group by 1
    """
)
print(
    f"PDL NC avec ≥1 relevé Réel (R15, toutes sources) : {reels.height}/{nb_nc} ({nb_nc - reels.height} sans aucun relevé réel observé)"
)

span = qdf(
    NC_PDL_CTE
    + """
    select r.pdl, min(date_releve) as debut, max(date_releve) as fin
    from flux_r15 r join nc_pdl using (pdl) group by 1
    """
).with_columns((pl.col("fin") - pl.col("debut")).dt.total_days().alias("span_j"))
span_median = span["span_j"].median()
print(f"Fenêtre d'observation médiane par PDL : {span_median:.0f} j")
print(
    f"→ {reels['n'].sum()} relevé(s) Réel(s) au total sur {reels.height}/{nb_nc} PDL en ~{span_median:.0f} j "
    "d'observation : un solde \"propre\" sur bornes réelles n'est praticable, au mieux, "
    "qu'à une cadence largement supra-annuelle chez les NC — jamais mensuelle."
)

# === 6. Cohérence R15 vs F15 ===========================================================
print("\n=== 6. Cohérence R15 vs F15 (bornes + énergies) ===")

# flux_r15.sql (dbt) n'extrait que Classe_Temporelle_Distributeur : absent à 100% pour les
# NC (compteurs non-Linky) → index_*_kwh canonique est NULL. Repli spike (lecture stg_r15
# brute, HORS dbt, aucune modification de production) sur Classe_Temporelle (singulier),
# universellement présent, même filtre classe_mesure=1/sens_mesure=0 que le modèle dbt.
r15_idx_canonique = q(
    NC_PDL_CTE
    + """
    select count(*) from flux_r15 r join nc_pdl using (pdl)
    where coalesce(index_base_kwh, index_hp_kwh, index_hc_kwh, index_hph_kwh, index_hch_kwh, index_hpb_kwh, index_hcb_kwh) is not null
    """
)[0][0]
r15_lignes = q(NC_PDL_CTE + "select count(*) from flux_r15 r join nc_pdl using (pdl)")[0][0]
print(f"Index R15 canonique (dbt, colonnes index_*_kwh) chez les NC : {r15_idx_canonique}/{r15_lignes} lignes")
print(
    "→ écart d'ingestion identifié : Classe_Temporelle_Distributeur (source du modèle dbt) est absent "
    "de 100% des relevés NC (compteurs non-Linky) ; l'index existe pourtant sous Classe_Temporelle "
    "(singulier), non extrait par flux_r15.sql aujourd'hui — piste de suivi, hors scope de ce spike."
)

r15_fallback = qdf(
    NC_PDL_CTE
    + """
    , classes as (
        select s.pdl, cast(s.releve ->> '$.Date_Releve' as date) as date_releve,
               classe ->> '$.Classe_Mesure' as classe_mesure,
               classe ->> '$.Sens_Mesure' as sens_mesure,
               lower(classe ->> '$.Id_Classe_Temporelle') as cadran,
               cast(classe ->> '$.Valeur' as bigint) as valeur
        from stg_r15 s join nc_pdl on s.pdl = nc_pdl.pdl,
            unnest(cast((s.releve -> '$.Classe_Temporelle') as json[])) as c(classe)
        where s.releve ->> '$.Motif_Releve' = 'CYCL'
    )
    select pdl, date_releve, cadran, valeur
    from classes
    where classe_mesure = '1' and sens_mesure = '0'
    """
)

r15_wide = r15_fallback.pivot(values="valeur", index=["pdl", "date_releve"], on="cadran").sort(["pdl", "date_releve"])
cadran_cols = [c for c in r15_wide.columns if c not in ("pdl", "date_releve")]
r15_diff = r15_wide.with_columns([pl.col(c).diff().over("pdl").alias(f"delta_{c}") for c in cadran_cols])
r15_diff = r15_diff.with_columns(pl.col("date_releve").shift(1).over("pdl").alias("date_debut"))
r15_diff = r15_diff.rename({"date_releve": "date_fin"}).drop_nulls("date_debut")

f15_ev = qdf(
    NC_PDL_CTE
    + """
    select f.pdl, f.date_debut, f.date_fin, f.libelle_ev, f.quantite
    from flux_f15_detail f join nc_pdl using (pdl)
    where f.nature_ev = '01' and f.unite = 'kWh' and f.type_facturation = 'CYCL'
    """
)
f15_ev = f15_ev.with_columns(
    pl.col("libelle_ev").map_elements(cadran_depuis_libelle, return_dtype=pl.Utf8).alias("cadran")
)
f15_wide = (
    f15_ev.group_by(["pdl", "date_debut", "date_fin", "cadran"])
    .agg(pl.col("quantite").sum())
    .pivot(values="quantite", index=["pdl", "date_debut", "date_fin"], on="cadran")
)
f15_wide = f15_wide.rename({c: f"f15_{c}" for c in f15_wide.columns if c not in ("pdl", "date_debut", "date_fin")})

compare = r15_diff.join(f15_wide, on=["pdl", "date_debut", "date_fin"], how="inner")
print(f"Fenêtres à bornes identiques R15 (fallback Classe_Temporelle) ET F15 : {compare.height}")

for cadran in cadran_cols:
    delta_col, f15_col = f"delta_{cadran}", f"f15_{cadran}"
    if f15_col not in compare.columns:
        continue
    sous = compare.filter(pl.col(delta_col).is_not_null() & pl.col(f15_col).is_not_null())
    if sous.height == 0:
        continue
    ecart = (sous[delta_col] - sous[f15_col]).abs()
    exact = (ecart <= 1).sum()
    print(
        f"  cadran {cadran:4} : n={sous.height:3}, écart moyen {ecart.mean():.2f} kWh, max {ecart.max():.0f} kWh, {exact}/{sous.height} à ±1 kWh près"
    )

# === 7. Détail par PDL (sortie locale, jamais committée) ==============================
DETAIL_CSV.parent.mkdir(exist_ok=True)
detail = (
    qdf(NC_PDL_CTE + "select pdl from nc_pdl")
    .join(
        qdf(NC_PDL_CTE + "select r.pdl, count(*) as n_r15 from flux_r15 r join nc_pdl using (pdl) group by 1"),
        on="pdl",
        how="left",
    )
    .join(
        qdf(NC_PDL_CTE + "select f.pdl, count(*) as n_f15 from flux_f15_detail f join nc_pdl using (pdl) group by 1"),
        on="pdl",
        how="left",
    )
    .join(
        qdf(NC_PDL_CTE + "select c.pdl, count(*) as n_c15 from flux_c15 c join nc_pdl using (pdl) group by 1"),
        on="pdl",
        how="left",
    )
    .join(
        qdf(
            NC_PDL_CTE
            + "select r.pdl, count(*) as n_r15_reel from flux_r15 r join nc_pdl using (pdl) where nature_index='réel' group by 1"
        ),
        on="pdl",
        how="left",
    )
    .fill_null(0)
)
detail.write_csv(DETAIL_CSV)
print(f"\nDétail par PDL écrit dans {DETAIL_CSV} (non committé, RGPD).")

# === 8. V2 (provisoire) — source de vérité moisniversaire =============================
print("\n=== 7. V2 (PROVISOIRE) — source de vérité moisniversaire ===")
print(
    """
Verdict PROVISOIRE — le R67 n'est pas encore en base (campagne M023 #543) ; à réviser
une fois intégré.

Constats de ce spike :
  - R15 porte les DATES de relevé cyclique (25/25 PDL) mais son index canonique (dbt) est
    NUL à 100% pour les NC — écart d'ingestion (Classe_Temporelle_Distributeur absent des
    compteurs non-Linky), pas une absence Enedis : l'index existe sous Classe_Temporelle.
  - F15 porte les fenêtres ET les énergies facturées (kWh par cadran), sans recouvrement,
    quasi toujours adjacentes (peu de trous) — 24/25 PDL couverts.
  - C15 ne porte aucun index chez les NC sur ce corpus (0/25) : les événements observés
    (entrée, bascule de niveau) n'embarquent pas de relevé avant/après.
  - Les relevés Réels (relève physique) sont rarissimes chez les NC : cadence largement
    supra-annuelle, très en-deçà du besoin d'un solde mensuel "propre".

→ Candidate provisoire : F15 (bornes + énergies déjà agrégées par cadran, fiable et
  couvrant 24/25) complétée par les dates R15 CYCL pour les PDL sans F15 récent. R15 seul
  n'est PAS une source d'énergie exploitable en l'état (index non extrait pour les NC).
  Le R67, une fois en base, tranchera probablement en faveur d'une troisième source
  (énergie déjà différenciée par Enedis, cf. ADR-0047) — verdict final déféré à V2.
"""
)

# === 9. Garde RGPD — scan du rapport committé =========================================
print("=== 8. Garde RGPD — scan du rapport committé ===")
motif_pdl = re.compile(r"\b\d{14}\b")
if RAPPORT.exists():
    trouves = motif_pdl.findall(RAPPORT.read_text())
    assert not trouves, f"RGPD : motif de 14 chiffres trouvé dans {RAPPORT} : {trouves[:3]}"
    print(f"OK — aucun motif de 14 chiffres dans {RAPPORT}")
else:
    print(f"{RAPPORT} n'existe pas encore — l'écrire puis rejouer ce script pour valider le garde-fou RGPD.")

con.close()
print("\n=== Script terminé sans erreur d'invariant. ===")
