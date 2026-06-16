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
        avant_id_calendrier_distributeur                        as id_calendrier_distributeur,
        avant_index_base_kwh as index_base_kwh, avant_index_hp_kwh as index_hp_kwh, avant_index_hc_kwh as index_hc_kwh,
        avant_index_hph_kwh as index_hph_kwh, avant_index_hpb_kwh as index_hpb_kwh,
        avant_index_hch_kwh as index_hch_kwh, avant_index_hcb_kwh as index_hcb_kwh
    from {{ ref('flux_c15') }}
    where coalesce(
        avant_index_base_kwh, avant_index_hp_kwh, avant_index_hc_kwh, avant_index_hph_kwh,
        avant_index_hpb_kwh, avant_index_hch_kwh, avant_index_hcb_kwh
    ) is not null
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
        apres_id_calendrier_distributeur                        as id_calendrier_distributeur,
        apres_index_base_kwh as index_base_kwh, apres_index_hp_kwh as index_hp_kwh, apres_index_hc_kwh as index_hc_kwh,
        apres_index_hph_kwh as index_hph_kwh, apres_index_hpb_kwh as index_hpb_kwh,
        apres_index_hch_kwh as index_hch_kwh, apres_index_hcb_kwh as index_hcb_kwh
    from {{ ref('flux_c15') }}
    where coalesce(
        apres_index_base_kwh, apres_index_hp_kwh, apres_index_hc_kwh, apres_index_hph_kwh,
        apres_index_hpb_kwh, apres_index_hch_kwh, apres_index_hcb_kwh
    ) is not null
)

select * from avant
union all
select * from apres
