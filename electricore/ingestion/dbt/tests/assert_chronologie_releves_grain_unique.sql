-- Grain de la Chronologie des relevés (ADR-0041, #376) : 1 ligne par
-- (ref_situation_contractuelle, date_releve, ordre_index) — le dédoublonnage par priorité
-- de source (QUALIFY) le garantit. Le test échoue (lignes renvoyées) si un triplet duplique.

select
    ref_situation_contractuelle,
    date_releve,
    ordre_index,
    count(*) as n
from {{ ref('chronologie_releves') }}
group by ref_situation_contractuelle, date_releve, ordre_index
having count(*) > 1
