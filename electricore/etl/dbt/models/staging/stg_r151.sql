-- Éclatement R151 : une ligne par PRM, unité portée depuis l'en-tête de flux.

select
    file_name,
    modification_date,
    content ->> '$.En_Tete_Flux[0].Unite_Mesure_Index' as unite,
    p.unnest                                           as prm
from {{ source('flux_raw', 'raw_r151') }},
    unnest(cast(content -> '$.PRM' as json[])) as p
