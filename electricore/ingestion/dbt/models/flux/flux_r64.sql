-- Linéarisation R64 : une ligne par (mesure, date de relevé), cadrans en colonnes.
--
-- Sémantique = le parser legacy (etl/parsing/r64.py), validée par golden :
-- - métadonnées (étape, contexte, grandeur, unité) du PREMIER couple
--   contexte/grandeur valide (grandeurMetier=CONS, grandeurPhysique=EA) ;
-- - points collectés sur TOUS les couples valides, calendriers distributeur
--   uniquement, classes SANS idClasseTemporelle ignorées, points iv=0 ;
-- - pivot par (mesure, date) — pas par PDL (grain occurrence, cf. C15/R15).
--
-- Accès JSON tolérant + extraction en colonnes nommées AVANT tout filtre
-- (anti-pushdown DuckDB, cf. flux_c15).

with contextes as (
    select
        mesure_id,
        mesure ->> '$.idPrm' as pdl,
        generate_subscripts(ctxs, 1) as ctx_i,
        unnest(ctxs)                 as ctx
    from (
        select mesure_id, mesure, cast(mesure -> '$.contexte' as json[]) as ctxs
        from {{ ref('stg_r64') }}
    )
),

grandeurs as (
    select
        mesure_id,
        pdl,
        ctx_i,
        ctx,
        generate_subscripts(gs, 1) as g_i,
        unnest(gs)                 as g
    from (
        select mesure_id, pdl, ctx_i, ctx, cast(ctx -> '$.grandeur' as json[]) as gs
        from contextes
    )
),

grandeurs_extraites as (
    select
        mesure_id,
        pdl,
        ctx_i,
        g_i,
        ctx ->> '$.etapeMetier'      as etape_metier,
        ctx ->> '$.contexteReleve'   as contexte_releve,
        ctx ->> '$.typeReleve'       as type_releve,
        g ->> '$.grandeurMetier'     as grandeur_metier,
        g ->> '$.grandeurPhysique'   as grandeur_physique,
        g ->> '$.unite'              as unite,
        g
    from grandeurs
),

-- Métadonnées : premier couple contexte/grandeur CONS+EA de chaque mesure.
meta as (
    select *
    from (
        select
            mesure_id, pdl, etape_metier, contexte_releve, type_releve,
            grandeur_metier, grandeur_physique, unite,
            row_number() over (partition by mesure_id order by ctx_i, g_i) as rang
        from grandeurs_extraites
        where grandeur_metier = 'CONS' and grandeur_physique = 'EA'
    )
    where rang = 1
),

calendriers as (
    select
        mesure_id,
        grandeur_metier,
        grandeur_physique,
        unnest(cals) as cal
    from (
        select mesure_id, grandeur_metier, grandeur_physique, cast(g -> '$.calendrier' as json[]) as cals
        from grandeurs_extraites
    )
),

classes as (
    select
        mesure_id,
        grandeur_metier,
        grandeur_physique,
        cal ->> '$.idCalendrier'                  as id_calendrier,
        lower(cal ->> '$.libelleCalendrier')      as libelle_calendrier,
        unnest(cast(cal -> '$.classeTemporelle' as json[])) as classe
    from calendriers
),

points as (
    select
        mesure_id,
        grandeur_metier,
        grandeur_physique,
        id_calendrier,
        libelle_calendrier,
        id_classe,
        p ->> '$.d'              as date_point,
        p ->> '$.v'              as valeur_point,
        p ->> '$.iv'             as iv
    from (
        select
            mesure_id, grandeur_metier, grandeur_physique, id_calendrier, libelle_calendrier,
            classe ->> '$.idClasseTemporelle' as id_classe,
            cast(classe -> '$.valeur' as json[]) as pts
        from classes
    ), unnest(pts) as t(p)
),

-- Filtres legacy, appliqués sur colonnes nommées (anti-pushdown) :
-- grandeur CONS/EA, calendrier distributeur, classe identifiée, point valide (iv=0).
points_valides as (
    select
        mesure_id,
        lower(id_classe)                       as cadran,
        cast(date_point as timestamp)          as date_releve,
        cast(valeur_point as bigint)           as valeur
    from points
    where grandeur_metier = 'CONS'
      and grandeur_physique = 'EA'
      and (
          id_calendrier in ('DI000001', 'DI000002', 'DI000003')
          or libelle_calendrier like '%distributeur%'
      )
      and id_classe is not null
      and iv = '0'
      and date_point is not null
      and valeur_point is not null
),

-- Agrégation conditionnelle sur le DOMAINE FERMÉ des 7 cadrans (comme flux_r151).
-- Contrat de colonnes stable quel que soit le corpus, là où un PIVOT ne crée que les
-- cadrans rencontrés → binder error en aval sur les absents (ex. modèle `releves`).
pivot_cadrans as (
    select
        mesure_id,
        date_releve,
        max(case when cadran = 'base' then valeur end) as index_base_kwh,
        max(case when cadran = 'hp'   then valeur end) as index_hp_kwh,
        max(case when cadran = 'hc'   then valeur end) as index_hc_kwh,
        max(case when cadran = 'hph'  then valeur end) as index_hph_kwh,
        max(case when cadran = 'hpb'  then valeur end) as index_hpb_kwh,
        max(case when cadran = 'hch'  then valeur end) as index_hch_kwh,
        max(case when cadran = 'hcb'  then valeur end) as index_hcb_kwh
    from points_valides
    group by mesure_id, date_releve
),

en_tete as (
    select distinct mesure_id, id_demande, si_demandeur, code_flux, format, modification_date
    from {{ ref('stg_r64') }}
)

-- Identité (ADR-0028, #232) : releve_id = CLÉ MÉTIER déterministe avec discriminant
-- type_releve (R64 distingue ses relevés par type, pas par avant/après). R64 n'a pas
-- d'Id_Releve natif → id_releve NULL ; nature canonique dérivée d'etapeMetier
-- (BRUT/VALID→réel, CORR→corrigé). occurrence_id = mesure_id (id d'occurrence
-- fichier, provenance forensique). La clé métier ne dépend pas de la livraison
-- gagnante du qualify : stable malgré les fenêtres chevauchantes.
select
    {{ mint_releve_id("'flux_R64'", "meta.pdl", "pivot_cadrans.date_releve", "meta.type_releve") }} as releve_id,
    cast(null as varchar)   as id_releve,
    {{ nature_depuis_etape_metier("meta.etape_metier") }} as nature_index,
    meta.pdl,
    meta.etape_metier,
    meta.contexte_releve,
    meta.type_releve,
    meta.grandeur_physique,
    meta.grandeur_metier,
    -- Unité normalisée en kWh au boundary (ADR-0034) : Enedis livre en Wh.
    case when meta.unite = 'Wh' then 'kWh' else meta.unite end as unite,
    en_tete.id_demande,
    en_tete.si_demandeur,
    en_tete.code_flux,
    en_tete.format,
    mesure_id as occurrence_id,
    -- Wh → kWh entier (floor par index, ADR-0034). meta.unite est l'unité déclarée
    -- (1er couple CONS/EA) ; // = division entière DuckDB, NULL-safe, exacte sur des
    -- cumuls non négatifs. L'erreur de troncature télescope (< 1 kWh / vie du registre).
    pivot_cadrans.* exclude (mesure_id) replace (
        case when meta.unite = 'Wh' then index_base_kwh // 1000 else index_base_kwh end as index_base_kwh,
        case when meta.unite = 'Wh' then index_hp_kwh   // 1000 else index_hp_kwh   end as index_hp_kwh,
        case when meta.unite = 'Wh' then index_hc_kwh   // 1000 else index_hc_kwh   end as index_hc_kwh,
        case when meta.unite = 'Wh' then index_hph_kwh  // 1000 else index_hph_kwh  end as index_hph_kwh,
        case when meta.unite = 'Wh' then index_hpb_kwh  // 1000 else index_hpb_kwh  end as index_hpb_kwh,
        case when meta.unite = 'Wh' then index_hch_kwh  // 1000 else index_hch_kwh  end as index_hch_kwh,
        case when meta.unite = 'Wh' then index_hcb_kwh  // 1000 else index_hcb_kwh  end as index_hcb_kwh
    )
from pivot_cadrans
join meta using (mesure_id)
join en_tete using (mesure_id)
-- Les fenêtres R64 se chevauchent : le même relevé (pdl, type, date) arrive dans
-- plusieurs fichiers. Même contrat que le merge legacy (primary_key pdl/type_releve/
-- date_releve) : on garde la livraison la plus récente.
qualify row_number() over (
    partition by meta.pdl, meta.type_releve, pivot_cadrans.date_releve
    order by en_tete.modification_date desc, mesure_id desc
) = 1
