-- Éclatement du document C15 : une ligne par PRM.
--
-- Contrairement à R64 (struct typé), C15 est un document profond (Adresse,
-- Alimentation, Dispositif_De_Comptage… des dizaines de feuilles) : un cast struct
-- exhaustif serait énorme et casserait au moindre champ Enedis inattendu (le cast
-- DuckDB est strict sur les clés inconnues). On reste donc en accès JSON tolérant
-- (`->>`/`->`), et `flux_c15` extrait sélectivement ce dont core/ a besoin.
--
-- `xml_vers_dict` applique « conteneur = liste » : PRM est toujours un tableau,
-- même unique → unnest direct.

-- prm_id : clé d'occurrence stable (fichier + position du PRM), portée à l'aval pour
-- scoper l'agrégation des relevés par PRM et non par PDL (un PDL revient dans
-- plusieurs fichiers). generate_subscripts s'aligne sur unnest.
select
    file_name,
    modification_date,
    file_name || '#' || generate_subscripts(prms, 1) as prm_id,
    unnest(prms)                                      as prm
from (
    select file_name, modification_date, cast(content -> '$.PRM' as json[]) as prms
    from {{ source('flux_raw', 'raw_c15') }}
)
