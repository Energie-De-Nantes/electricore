-- Éclatement R15 : une ligne par PRM, unité portée depuis l'en-tête de flux.
--
-- Partagé par flux_r15 (relevés, cadran unique) et flux_r15_acc (autoconsommation,
-- classes ea_*) : même source brute, deux linéarisations. « conteneur = liste »
-- (xml_vers_dict) → PRM toujours tableau, En_Tete_Flux indexé [0].

select
    file_name,
    modification_date,
    content ->> '$.En_Tete_Flux[0].Unite_Mesure_Index' as unite,
    p.unnest                                           as prm
from {{ source('flux_raw', 'raw_r15') }},
    unnest(cast(content -> '$.PRM' as json[])) as p
