-- Linéarisation R151 : une ligne par RELEVÉ, index par cadran en colonnes.
--
-- Le staging porte le grain relevé (releve_id). Agrégation conditionnelle sur le
-- DOMAINE FERMÉ des 7 cadrans (core/models/cadrans.py) plutôt qu'un PIVOT : le
-- contrat de colonnes des loaders core est stable quel que soit le corpus (un
-- PIVOT ne crée que les colonnes rencontrées → binder error aval sur les absentes).
-- Les classes hors domaine (ex. INCONNU) sont ignorées — non consommées par l'aval.

-- Identité (ADR-0028, #232) : releve_id = CLÉ MÉTIER déterministe
-- (source|pdl|date|discriminant), stable sur re-livraison. R151 (télérelevé
-- périodique) n'a pas d'Id_Releve natif → id_releve NULL, et pas de Nature_Index
-- → nature_index 'réel' par défaut. L'id d'occurrence fichier du staging
-- (fichier#position, instable, rejeté comme clé) est exposé comme provenance
-- forensique `occurrence_id`.
with flat as (
    select
        releve_id as occurrence_id,
        pdl,
        cast(releve ->> '$.Date_Releve' as date)       as date_releve,
        releve ->> '$.Id_Calendrier_Fournisseur'       as id_calendrier_fournisseur,
        releve ->> '$.Id_Calendrier_Distributeur'      as id_calendrier_distributeur,
        releve ->> '$.Id_Affaire'                      as id_affaire,
        -- Unité normalisée en kWh au boundary (ADR-0034) : Enedis livre en Wh.
        case when unite = 'Wh' then 'kWh' else unite end as unite
    from {{ ref('stg_r151') }}
),

classes as (
    select
        releve_id as occurrence_id,
        lower(c.classe ->> '$.Id_Classe_Temporelle')   as cadran,
        -- Wh → kWh entier (floor par index, ADR-0034). // = division entière DuckDB ;
        -- les index sont des cumuls non négatifs → floor exact. L'erreur de troncature
        -- télescope (< 1 kWh sur la vie du registre).
        case when unite = 'Wh'
             then cast(c.classe ->> '$.Valeur' as bigint) // 1000
             else cast(c.classe ->> '$.Valeur' as bigint)
        end                                            as valeur
    from {{ ref('stg_r151') }},
        unnest(cast(releve -> '$.Classe_Temporelle_Distributeur' as json[])) as c(classe)
),

cadrans as (
    select
        occurrence_id,
        {{ pivot_cadrans() }}
    from classes
    group by occurrence_id
)

select
    {{ mint_releve_id("'flux_R151'", "pdl", "date_releve", "false") }} as releve_id,
    cast(null as varchar)                                              as id_releve,
    cast('réel' as varchar)                                           as nature_index,
    flat.* exclude (occurrence_id),
    occurrence_id,
    cadrans.* exclude (occurrence_id)
from flat left join cadrans using (occurrence_id)
