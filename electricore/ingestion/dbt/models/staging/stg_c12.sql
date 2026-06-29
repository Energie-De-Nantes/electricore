-- Éclatement du document C12 : une ligne par PRM.
--
-- C12 (SGE GUI 0129, XSD 0130) : description contractuelle C2-C4 (>36 kVA).
-- Même politique que stg_c15 (accès JSON tolérant ->>/->, pas de cast struct géant).
-- `xml_vers_dict` applique « conteneur = liste » → PRM est sous Contrat[0], toujours
-- tableau même unique → unnest direct avec generate_subscripts.
--
-- Différence clé vs C15 : PRM est dans content -> '$.Contrat[0].PRM' (pas à la racine).

-- prm_id : clé d'occurrence stable (fichier + position du PRM), portée à l'aval pour
-- l'unnest des classes temporelles. generate_subscripts s'aligne sur unnest.
select
    file_name,
    modification_date,
    file_name || '#' || generate_subscripts(prms, 1) as prm_id,
    unnest(prms)                                      as prm
from (
    select file_name, modification_date, cast(content -> '$.Contrat[0].PRM' as json[]) as prms
    from {{ source('flux_raw', 'raw_c12') }}
)
