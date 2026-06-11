-- Éclatement R151 : une ligne par PRM, unité portée depuis l'en-tête de flux.

-- prm_id : clé d'occurrence stable (fichier + position du PRM) pour scoper les relevés
-- par PRM et non par PDL (un PDL revient à chaque période dans les fichiers R151).
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
    from {{ source('flux_raw', 'raw_r151') }}
)
