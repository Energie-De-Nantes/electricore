-- Éclatement R15 au grain du RELEVÉ : une ligne par Donnees_Releve.
--
-- Un PRM peut porter plusieurs relevés à des dates différentes dans un même fichier
-- (jusqu'à 5 observés sur le réel) : le grain PRM fabriquait une chimère (champs du
-- 1er relevé, index mélangés). releve_id = fichier + position PRM + position relevé,
-- porté à l'aval pour scoper agrégations et jointures. Partagé par flux_r15 et
-- flux_r15_acc.

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
        from {{ source('flux_raw', 'raw_r15') }}
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
