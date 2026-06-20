-- Adapter de source C15 → relevés (ADR-0029, #304).
--
-- `flux_c15` reste FIDÈLE : grain événement, avant/après en colonnes (fidélité de
-- l'événement + substrat d'`impacte_energie` au cœur). Ce modèle en dérive le grain
-- *relevé* — 1 événement → 2 lignes (avant/après) — consommé par le modèle canonique
-- `releves`. Adapter ÉTROIT : n'émet QUE ce que C15 porte. Pas d'`occurrence_id` (C15
-- n'en a pas → null-fill par le conformer au seam `releves`). Discriminant : `ordre_index`
-- (False = avant, True = après). `releve_id` minté au seam dbt comme les autres sources
-- (ADR-0028), discriminé par `ordre_index`. RSC/FTA portées NATIVEMENT par l'événement.

with avant as (
    select
        {{ mint_releve_id("'flux_C15'", "pdl", "(date_evenement at time zone 'Europe/Paris')", "false") }} as releve_id,
        pdl,
        date_evenement                                          as date_releve,
        false                                                   as ordre_index,
        {{ nature_depuis_nature_index("avant_nature_index") }}  as nature_index,
        ref_situation_contractuelle,
        formule_tarifaire_acheminement,
        niveau_ouverture_services,
        evenement_declencheur,
        avant_id_calendrier_distributeur                        as id_calendrier_distributeur,
        {{ cadrans_index_rename('avant_') }}
    from {{ ref('flux_c15') }}
    where coalesce({{ cadrans_index_cols('avant_') }}) is not null
),

apres as (
    select
        {{ mint_releve_id("'flux_C15'", "pdl", "(date_evenement at time zone 'Europe/Paris')", "true") }} as releve_id,
        pdl,
        date_evenement                                          as date_releve,
        true                                                    as ordre_index,
        {{ nature_depuis_nature_index("apres_nature_index") }}  as nature_index,
        ref_situation_contractuelle,
        formule_tarifaire_acheminement,
        niveau_ouverture_services,
        evenement_declencheur,
        apres_id_calendrier_distributeur                        as id_calendrier_distributeur,
        {{ cadrans_index_rename('apres_') }}
    from {{ ref('flux_c15') }}
    where coalesce({{ cadrans_index_cols('apres_') }}) is not null
)

select * from avant
union all
select * from apres
