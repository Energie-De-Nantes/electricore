-- Invariant ADR-0052 : au grain RSC, un état `RESILIE` ⟺ un code de sortie {RES, CFNS}.
-- La fermeture des spans de présence au périmètre (core/pipelines/perimetre.py) date la
-- `fin` sur le CODE sortie (événement unique et net) et garde le flag `etat_contractuel`
-- en garde-fou. Prouvé 100 % concordant au spike prod (2026-07-01 : 188 RSC résiliées =
-- 188 avec code de sortie, 0 désaccord). Le test échoue (lignes renvoyées) dès qu'une RSC
-- porte l'un sans l'autre — c.-à-d. si Enedis dissocie un jour le flag et le code.
--
-- Tag `garde_donnees_reelles` : invariant d'INTÉGRITÉ des données Enedis réelles, à exclure
-- des harnais golden synthétiques (les fixtures XSD « instance maximale » cyclent les
-- énumérations → RESILIE et sorties dissociés artificiellement). Tourne en prod
-- (`construire_dbt` → `+flux_c15`).
{{ config(tags=['garde_donnees_reelles']) }}

with par_rsc as (
    select
        ref_situation_contractuelle,
        max(case when etat_contractuel = 'RESILIE' then 1 else 0 end)             as a_flag_resilie,
        max(case when evenement_declencheur in ('RES', 'CFNS') then 1 else 0 end) as a_code_sortie
    from {{ ref('flux_c15') }}
    where ref_situation_contractuelle is not null
    group by ref_situation_contractuelle
)

select
    ref_situation_contractuelle,
    a_flag_resilie,
    a_code_sortie
from par_rsc
where a_flag_resilie <> a_code_sortie
