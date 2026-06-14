-- Modèle de relevés canonique (ADR-0029) : la ligne de temps des relevés consommée
-- par l'aval. Union des sources (1 ligne = 1 relevé) : périodiques télérelevées
-- (R151, R64) + relevés contractuels C15 avant/après dépivotés. Dates harmonisées
-- « début de journée » Europe/Paris (ADR-0003 : R151 J → J+1). Mint uniforme de
-- releve_id (clé métier ADR-0028) pour TOUTES les sources, C15 comprise (l'exception
-- « en core pour c15 » d'ADR-0028 disparaît). Dedup même-source par releve_id (on
-- garde la livraison la plus récente).
--
-- À VENIR : enrichissement contractuel RSC/FTA des lignes périodiques, piloté par C15
-- (#243). L'arbitrage de priorité inter-sources (C15 > R64 > R151) et la sélection des
-- bornes de facturation restent au cœur (#244, ADR-0029). flux_c15 reste fidèle :
-- avant/après y sont conservés (fidélité de l'événement + substrat d'impacte_energie).

with unifies as (
    -- R151 : télérelevés périodiques. Harmonisation J → J+1 (ADR-0003) puis ancrage
    -- heure-mur Paris. releve_id (clé métier) est minté sur la date BRUTE en amont
    -- (flux_r151) : l'harmonisation d'affichage ne change pas l'identité.
    select
        releve_id,
        'flux_R151'                                                                 as source,
        pdl,
        timezone('Europe/Paris', cast(date_releve as timestamp) + interval '1 day') as date_releve,
        false                                                                       as ordre_index,
        nature_index,
        id_releve,
        occurrence_id,
        cast(null as varchar)                                                       as ref_situation_contractuelle,
        cast(null as varchar)                                                       as formule_tarifaire_acheminement,
        id_calendrier_distributeur,
        index_base_kwh, index_hp_kwh, index_hc_kwh,
        index_hph_kwh, index_hpb_kwh, index_hch_kwh, index_hcb_kwh
    from {{ ref('flux_r151') }}

    union all

    -- R64 : relevés JSON timeseries (déjà dédoublonnés intra-source dans flux_r64).
    -- date_releve naïve → ancrage heure-mur Paris (convention début de journée native,
    -- pas de décalage).
    select
        releve_id,
        'flux_R64'                                                                  as source,
        pdl,
        timezone('Europe/Paris', date_releve)                                       as date_releve,
        false                                                                       as ordre_index,
        nature_index,
        id_releve,
        occurrence_id,
        cast(null as varchar)                                                       as ref_situation_contractuelle,
        cast(null as varchar)                                                       as formule_tarifaire_acheminement,
        cast(null as varchar)                                                       as id_calendrier_distributeur,
        index_base_kwh, index_hp_kwh, index_hc_kwh,
        index_hph_kwh, index_hpb_kwh, index_hch_kwh, index_hcb_kwh
    from {{ ref('flux_r64') }}

    union all

    -- C15 « avant » : index relevé immédiatement AVANT l'événement contractuel
    -- (ordre_index = false). date_releve = date_evenement (instant). RSC/FTA sont
    -- portées NATIVEMENT par l'événement (C15 = source contractuelle). releve_id minté
    -- en dbt comme les autres sources (discriminant = ordre_index).
    select
        {{ mint_releve_id("'flux_C15'", "pdl", "date_evenement", "false") }}        as releve_id,
        'flux_C15'                                                                  as source,
        pdl,
        date_evenement                                                              as date_releve,
        false                                                                       as ordre_index,
        {{ nature_depuis_nature_index("avant_nature_index") }}                      as nature_index,
        cast(null as varchar)                                                       as id_releve,
        cast(null as varchar)                                                       as occurrence_id,
        ref_situation_contractuelle,
        formule_tarifaire_acheminement,
        avant_id_calendrier_distributeur                                            as id_calendrier_distributeur,
        avant_index_base_kwh as index_base_kwh, avant_index_hp_kwh as index_hp_kwh, avant_index_hc_kwh as index_hc_kwh,
        avant_index_hph_kwh as index_hph_kwh, avant_index_hpb_kwh as index_hpb_kwh,
        avant_index_hch_kwh as index_hch_kwh, avant_index_hcb_kwh as index_hcb_kwh
    from {{ ref('flux_c15') }}
    where coalesce(
        avant_index_base_kwh, avant_index_hp_kwh, avant_index_hc_kwh, avant_index_hph_kwh,
        avant_index_hpb_kwh, avant_index_hch_kwh, avant_index_hcb_kwh
    ) is not null

    union all

    -- C15 « après » : index relevé immédiatement APRÈS l'événement (ordre_index = true).
    select
        {{ mint_releve_id("'flux_C15'", "pdl", "date_evenement", "true") }}         as releve_id,
        'flux_C15'                                                                  as source,
        pdl,
        date_evenement                                                              as date_releve,
        true                                                                        as ordre_index,
        {{ nature_depuis_nature_index("apres_nature_index") }}                      as nature_index,
        cast(null as varchar)                                                       as id_releve,
        cast(null as varchar)                                                       as occurrence_id,
        ref_situation_contractuelle,
        formule_tarifaire_acheminement,
        apres_id_calendrier_distributeur                                            as id_calendrier_distributeur,
        apres_index_base_kwh as index_base_kwh, apres_index_hp_kwh as index_hp_kwh, apres_index_hc_kwh as index_hc_kwh,
        apres_index_hph_kwh as index_hph_kwh, apres_index_hpb_kwh as index_hpb_kwh,
        apres_index_hch_kwh as index_hch_kwh, apres_index_hcb_kwh as index_hcb_kwh
    from {{ ref('flux_c15') }}
    where coalesce(
        apres_index_base_kwh, apres_index_hp_kwh, apres_index_hc_kwh, apres_index_hph_kwh,
        apres_index_hpb_kwh, apres_index_hch_kwh, apres_index_hcb_kwh
    ) is not null
),

-- Dedup même-source (re-livraison) : 1 ligne par relevé logique = 1 par releve_id
-- (clé métier ADR-0028, encode source|pdl|date|discriminant). On garde la livraison
-- la plus récente ; occurrence_id (fichier#position / mesure_id) départage de façon
-- déterministe. R64 est déjà unique en amont (qualify dans flux_r64).
dedup as (
    select *
    from unifies
    qualify row_number() over (partition by releve_id order by occurrence_id desc) = 1
)

-- Enrichissement contractuel piloté par C15 (#243, ADR-0029) : les lignes périodiques
-- (R151/R64) ne portent pas de RSC/FTA ; on les propage (forward-fill) depuis les
-- relevés C15 — la source contractuelle — par PDL, le long du temps. Remplace le
-- join_asof incident sur les événements FACTURATION par une attribution déterministe
-- et découplée de la facturation. Tie-break à date égale : C15 d'abord (sa RSC est vue
-- par un relevé périodique du même jour), puis « après » avant « avant » non — on
-- ordonne ordre_index croissant pour que la situation APRÈS l'événement (nouvelle RSC
-- d'une entrée) prime. Multi-source préservé : pas de dedup inter-sources ici,
-- l'arbitrage de priorité reste au cœur (#244).
select * replace (
    coalesce(
        ref_situation_contractuelle,
        last_value(ref_situation_contractuelle ignore nulls) over w
    ) as ref_situation_contractuelle,
    coalesce(
        formule_tarifaire_acheminement,
        last_value(formule_tarifaire_acheminement ignore nulls) over w
    ) as formule_tarifaire_acheminement
)
from dedup
window w as (
    partition by pdl
    order by date_releve,
             case source when 'flux_C15' then 0 when 'flux_R64' then 1 else 2 end,
             ordre_index
    rows between unbounded preceding and current row
)
