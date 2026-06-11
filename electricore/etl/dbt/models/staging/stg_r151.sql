-- Éclatement R151 au grain du RELEVÉ : une ligne par Donnees_Releve.
--
-- 397 PRM observés avec plusieurs relevés par fichier (jusqu'à 20) : le grain PRM
-- fabriquait une chimère et perdait les relevés suivants (cf. stg_r15).

with prms as (
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
)

select
    file_name,
    modification_date,
    unite,
    pdl,
    prm_id || '#' || generate_subscripts(releves, 1) as releve_id,
    unnest(releves)                                   as releve
from (
    select
        file_name,
        modification_date,
        unite,
        prm_id,
        prm ->> '$.Id_PRM'                              as pdl,
        cast(prm -> '$.Donnees_Releve' as json[])       as releves
    from prms
)
