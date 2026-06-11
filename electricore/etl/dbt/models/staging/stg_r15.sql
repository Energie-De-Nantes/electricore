-- Éclatement R15 : une ligne par PRM, unité portée depuis l'en-tête de flux.
--
-- Partagé par flux_r15 (relevés, cadran unique) et flux_r15_acc (autoconsommation,
-- classes ea_*) : même source brute, deux linéarisations. « conteneur = liste »
-- (xml_vers_dict) → PRM toujours tableau, En_Tete_Flux indexé [0].

-- prm_id : clé d'occurrence stable (fichier + position du PRM) pour scoper les relevés
-- par PRM et non par PDL (un PDL revient dans de nombreux fichiers R15).
select
    file_name,
    modification_date,
    unite,
    file_name || '#' || generate_subscripts(prms, 1) as prm_id,
    unnest(prms)                                      as prm
from (
    select
        file_name,
        modification_date,
        content ->> '$.En_Tete_Flux[0].Unite_Mesure_Index' as unite,
        cast(content -> '$.PRM' as json[])               as prms
    from {{ source('flux_raw', 'raw_r15') }}
)
