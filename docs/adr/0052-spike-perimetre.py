"""Spike ADR-0052 — présence au périmètre : le flag `etat_contractuel` concorde-t-il avec
les codes de sortie {RES, CFNS} pour fermer un span de présence ?

Expérience time-boxée (2026-07-01) contre le C15 réel local, qui a tranché la décision :
les deux signaux concordent à 100 % → on ferme la `fin` sur le CODE (événement unique/net)
et on garde le flag en invariant (test dbt `assert_c15_resilie_ssi_code_sortie.sql`).

Repro :  uv run python docs/adr/0052-spike-perimetre.py
(lecture seule ; nécessite la base d'ingestion locale peuplée en C15).
"""

import duckdb

DB = "electricore/ingestion/flux_enedis_pipeline.duckdb"

con = duckdb.connect(DB, read_only=True)
con.execute("use flux_enedis")


def q(sql):
    return con.execute(sql).fetchall()


print("=== 0. Volumétrie ===")
print("lignes flux_c15 :", q("select count(*) from flux_c15")[0][0])
print("PDL distincts   :", q("select count(distinct pdl) from flux_c15")[0][0])
print("RSC distincts   :", q("select count(distinct ref_situation_contractuelle) from flux_c15")[0][0])

print("\n=== 1. Valeurs de etat_contractuel (+ nulls) ===")
for etat, n in q("select etat_contractuel, count(*) from flux_c15 group by 1 order by 2 desc"):
    print(f"  {etat!r:20} : {n}")

print("\n=== 2. Croisement evenement_declencheur x etat_contractuel ===")
for ev, etat, n in q(
    "select evenement_declencheur, etat_contractuel, count(*) n from flux_c15 group by 1,2 order by 1,2"
):
    print(f"  {str(ev):8} | {str(etat):12} | {n}")

print("\n=== 3. Accord au grain RSC : flag RESILIE vs code sortie {RES,CFNS} ===")
for f, c, n in q(
    """
    with par_rsc as (
      select ref_situation_contractuelle rsc,
             max(case when etat_contractuel='RESILIE' then 1 else 0 end) a_flag_resilie,
             max(case when evenement_declencheur in ('RES','CFNS') then 1 else 0 end) a_code_sortie
      from flux_c15 where ref_situation_contractuelle is not null group by 1)
    select a_flag_resilie, a_code_sortie, count(*) n from par_rsc group by 1,2 order by 1,2
    """
):
    tag = "  <-- RESILIE sans code (casse codes-only)" if (f and not c) else ""
    tag = "  <-- code sans RESILIE (casse flag-only)" if (c and not f) else tag
    print(f"  flag_resilie={f} code_sortie={c} : {n}{tag}")

print("\n=== 4. Événements APRÈS la sortie dans une même RSC (réouverture ?) ===")
after = q(
    """
    with sortie as (
      select ref_situation_contractuelle rsc, min(date_evenement) date_sortie
      from flux_c15 where evenement_declencheur in ('RES','CFNS') group by 1)
    select count(*) from flux_c15 c join sortie s
      on c.ref_situation_contractuelle=s.rsc and c.date_evenement > s.date_sortie
    """
)[0][0]
print(f"  événements post-sortie (attendu 0) : {after}")

print("\n=== 5. Multi-RSC par PDL (ré-entrées) ===")
for k, nb in q(
    """
    with c as (select pdl, count(distinct ref_situation_contractuelle) k
               from flux_c15 where ref_situation_contractuelle is not null group by 1)
    select k, count(*) from c group by 1 order by 1
    """
):
    print(f"  {k} RSC : {nb} PDL")

print("\n=== 6. Périmètre actif AUJOURD'HUI : flag vs code (grain PDL) ===")
for methode, expr in [
    ("code (dernier evt ∉ {RES,CFNS})", "evenement_declencheur not in ('RES','CFNS')"),
    ("flag (dernier etat ≠ RESILIE)", "etat_contractuel is distinct from 'RESILIE'"),
]:
    n = q(
        f"""
        with ranked as (
          select pdl, {expr} actif,
                 row_number() over (partition by pdl order by date_evenement desc) rn
          from flux_c15)
        select count(*) from ranked where rn=1 and actif
        """
    )[0][0]
    print(f"  {methode:36} -> {n} PDL actifs")

con.close()
