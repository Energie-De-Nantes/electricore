-- Modèle de relevés canonique (ADR-0029) : la ligne de temps des relevés consommée
-- par l'aval. Union des sources (1 ligne = 1 relevé) : périodiques télérelevées
-- (R151, R64) + relevés contractuels C15 avant/après dépivotés (via int_releves__c15).
--
-- ASSEMBLAGE PAR ADAPTERS CONFORMÉS (#304) : chaque source est un adapter ÉTROIT
-- (CTE pour R151/R64, modèle int_releves__c15 pour le dépivot C15) n'émettant que ce
-- qu'elle porte vraiment, avec sa logique propre (harmonisation J→J+1 ADR-0003 pour R151,
-- ancrage heure-mur Paris). Le macro `conformer_au_contrat_releve()` (cf.
-- macros/conformer_releve.sql) estampille `source` et remplit en NULL typé ce que la
-- source n'a pas — le contrat de colonnes vit en UN endroit (`contrat_releve()`), plus
-- une transcription `cast(null)` répétée par branche. « Ce que la source n'a pas » est
-- lisible au point d'appel (l'argument `fournit`).
--
-- Mint uniforme de releve_id (clé métier ADR-0028) pour TOUTES les sources, C15 comprise.
-- Dedup même-source par releve_id (on garde la livraison la plus récente). L'arbitrage de
-- priorité inter-sources (C15 > R64 > R151) et la sélection des bornes de facturation
-- restent au cœur (#244, ADR-0029). flux_c15 reste fidèle (avant/après conservés).

with
-- R151 : adapter de source. Télérelevés périodiques. Harmonisation J → J+1 (ADR-0003)
-- puis ancrage heure-mur Paris. releve_id (clé métier) est minté sur la date BRUTE en
-- amont (flux_r151) : l'harmonisation d'affichage ne change pas l'identité. Ne porte pas
-- de RSC/FTA (null-fill au conformer).
r151_raw as (
    select
        releve_id,
        pdl,
        timezone('Europe/Paris', cast(date_releve as timestamp) + interval '1 day') as date_releve,
        false                                                                       as ordre_index,
        nature_index,
        occurrence_id,
        id_calendrier_distributeur,
        {{ cadrans_index_cols() }}
    from {{ ref('flux_r151') }}
),

-- R64 : adapter de source. Relevés JSON timeseries (déjà dédoublonnés intra-source dans
-- flux_r64). date_releve naïve → ancrage heure-mur Paris (convention début de journée
-- native). Porte son calendrier distributeur (#304). Ne porte pas de RSC/FTA.
r64_raw as (
    select
        releve_id,
        pdl,
        timezone('Europe/Paris', date_releve)                                       as date_releve,
        false                                                                       as ordre_index,
        nature_index,
        occurrence_id,
        id_calendrier_distributeur,
        {{ cadrans_index_cols() }}
    from {{ ref('flux_r64') }}
),

unifies as (
    {{ conformer_au_contrat_releve('r151_raw', 'flux_R151', fournit=[
        'releve_id', 'pdl', 'date_releve', 'ordre_index', 'nature_index', 'occurrence_id',
        'id_calendrier_distributeur',
    ] + cadrans_index_noms()) }}

    union all

    {{ conformer_au_contrat_releve('r64_raw', 'flux_R64', fournit=[
        'releve_id', 'pdl', 'date_releve', 'ordre_index', 'nature_index', 'occurrence_id',
        'id_calendrier_distributeur',
    ] + cadrans_index_noms()) }}

    union all

    {{ conformer_au_contrat_releve(ref('int_releves__c15'), 'flux_C15', fournit=[
        'releve_id', 'pdl', 'date_releve', 'ordre_index', 'nature_index',
        'ref_situation_contractuelle', 'formule_tarifaire_acheminement', 'id_calendrier_distributeur',
    ] + cadrans_index_noms()) }}
),

-- Dedup même-source (re-livraison) : 1 ligne par relevé logique = 1 par releve_id
-- (clé métier ADR-0028). On garde la livraison la plus récente ; occurrence_id départage
-- de façon déterministe. R64 est déjà unique en amont (qualify dans flux_r64).
dedup as (
    select *
    from unifies
    qualify row_number() over (partition by releve_id order by occurrence_id desc) = 1
)

-- Enrichissement contractuel piloté par C15 (#243, ADR-0029) : les lignes périodiques
-- (R151/R64) ne portent pas de RSC/FTA ; on les propage (forward-fill) depuis les
-- relevés C15 — la source contractuelle — par PDL, le long du temps. Tie-break à date
-- égale : C15 d'abord, puis R64, puis R151 ; ordre_index croissant pour que la situation
-- APRÈS l'événement prime. Multi-source préservé : pas de dedup inter-sources ici,
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
