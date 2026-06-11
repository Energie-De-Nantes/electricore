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

select
    file_name,
    modification_date,
    p.unnest as prm
from {{ source('flux_raw', 'raw_c15') }},
    unnest(cast(content -> '$.PRM' as json[])) as p
