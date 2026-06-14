-- Linéarisation R15 : une ligne par RELEVÉ, index par cadran en colonnes.
--
-- Index distributeur uniquement : Classe_Mesure = 1 (index cumulé) et Sens_Mesure = 0
-- (soutirage) — même condition que flux.yaml. Agrégation conditionnelle sur le
-- DOMAINE FERMÉ des 7 cadrans (cf. flux_r151) : contrat de colonnes stable pour
-- les loaders core. Extraction en colonnes nommées AVANT le filtre (anti-pushdown).

-- Identité (ADR-0028, #232) : releve_id = CLÉ MÉTIER déterministe
-- (source|pdl|date|discriminant), stable sur re-livraison. R15 porte un Id_Releve
-- natif (provenance → id_releve) et un Nature_Index (REEL/ESTIME → nature canonique).
-- L'id d'occurrence fichier du staging (instable) reste exposé en provenance
-- forensique `occurrence_id`.
with flat as (
    select
        releve_id as occurrence_id,
        pdl,
        cast(releve ->> '$.Date_Releve' as timestamptz) as date_releve,
        releve ->> '$.Id_Releve'                       as id_releve,
        releve ->> '$.Nature_Index'                    as nature_index_source,
        releve ->> '$.Id_Calendrier'                   as id_calendrier,
        releve ->> '$.Ref_Situation_Contractuelle'     as ref_situation_contractuelle,
        releve ->> '$.Type_Compteur'                   as type_compteur,
        releve ->> '$.Motif_Releve'                    as motif_releve,
        releve ->> '$.Ref_Demandeur'                   as ref_demandeur,
        releve ->> '$.Id_Affaire'                      as id_affaire,
        unite
    from {{ ref('stg_r15') }}
),

classes as (
    select releve_id as occurrence_id, c.classe
    from {{ ref('stg_r15') }},
        unnest(cast(releve -> '$.Classe_Temporelle_Distributeur' as json[])) as c(classe)
),

extrait as (
    select
        occurrence_id,
        classe ->> '$.Classe_Mesure'               as classe_mesure,
        classe ->> '$.Sens_Mesure'                 as sens_mesure,
        lower(classe ->> '$.Id_Classe_Temporelle') as cadran,
        cast(classe ->> '$.Valeur' as bigint)      as valeur
    from classes
),

filtre as (
    select occurrence_id, cadran, valeur
    from extrait
    where classe_mesure = '1' and sens_mesure = '0'
),

cadrans as (
    select
        occurrence_id,
        max(case when cadran = 'base' then valeur end) as index_base_kwh,
        max(case when cadran = 'hp' then valeur end) as index_hp_kwh,
        max(case when cadran = 'hc' then valeur end) as index_hc_kwh,
        max(case when cadran = 'hph' then valeur end) as index_hph_kwh,
        max(case when cadran = 'hpb' then valeur end) as index_hpb_kwh,
        max(case when cadran = 'hch' then valeur end) as index_hch_kwh,
        max(case when cadran = 'hcb' then valeur end) as index_hcb_kwh
    from filtre
    group by occurrence_id
)

select
    {{ mint_releve_id("'flux_R15'", "pdl", "date_releve", "false") }} as releve_id,
    id_releve,
    {{ nature_depuis_nature_index("nature_index_source") }}          as nature_index,
    flat.* exclude (occurrence_id, id_releve, nature_index_source),
    occurrence_id,
    cadrans.* exclude (occurrence_id)
from flat left join cadrans using (occurrence_id)
