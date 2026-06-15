-- Éclatement du document X12/X13 : une ligne par affaire.
--
-- X12 (affaires initiées par l'instance) et X13 (affaires reçues d'autres acteurs)
-- partagent le même schéma (racine <affaires>) et la même table brute `raw_affaires`
-- (glob *_X1[23]_*) : l'`origine` se dérive du nom de fichier. Document profond (blocs
-- par prestation) → accès JSON tolérant (`->>`/`->`) côté `flux_affaires`, pas de cast
-- struct géant.
--
-- `xml_vers_dict` applique « conteneur = liste » (affaire toujours un tableau, même
-- unique → unnest direct) et expose les attributs sous des clés `@` (id d'affaire,
-- codes statut/objet/état). Un flux « sans affaires » (jour sans mouvement) n'a pas
-- de clé `affaire` → cast NULL → unnest → zéro ligne.

-- affaire_occ : clé d'occurrence stable (fichier + position de l'affaire), portée à
-- l'aval pour scoper l'unnest des jalons. generate_subscripts s'aligne sur unnest.
select
    file_name,
    modification_date,
    case
        when file_name like '%X12%' then 'initiee'
        when file_name like '%X13%' then 'recue'
    end                                                  as origine,
    file_name || '#' || generate_subscripts(affaires, 1) as affaire_occ,
    unnest(affaires)                                     as affaire
from (
    select file_name, modification_date, cast(content -> '$.affaire' as json[]) as affaires
    from {{ source('flux_raw', 'raw_affaires') }}
)
