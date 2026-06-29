-- Garde-fou ADR-0051 (#344) : tout Libelle de Classe_Temporelle_TURPE_Souscrite dans
-- flux_c12 doit appartenir à l'enum XSD complet.
--
-- Les classes HTA (Pointe, HPDemiSaison, HCDemiSaison, JA, PM) sont dans l'enum et
-- passent ici (leur pivot produit NULL, mais elles ne sont pas « inconnues »).
-- Un Libelle hors enum = Enedis a étendu le schéma → il faut mettre à jour le pivot.
-- Le test échoue s'il renvoie des lignes.

with classes as (
    select
        prm_id,
        t.classe ->> '$.Libelle' as libelle
    from {{ ref('stg_c12') }},
        unnest(
            cast(
                prm -> '$.Type_Evenement[0].Situation_Contractuelle[0].Option_Tarifaire_TURPE_Souscrite[0].Classe_Temporelle_TURPE_Souscrite'
                as json[]
            )
        ) as t(classe)
)

select libelle
from classes
where libelle not in (
    'Pointe', 'HPH', 'HPDemiSaison', 'HCH', 'HCDemiSaison',
    'HPE', 'HCE', 'JA', 'Base', 'HP', 'HC', 'PM'
)
