{{ config(materialized='view') }}

-- Audit de séquences Enedis (#645, PRD #644) : une ligne par anomalie détectée sur les
-- tables brutes du landing. Deux volets :
-- 1. Nomenclature (macro `audit_sequences`, brique commune #645/#646, SEUL lieu où
--    vivent les regexp de nom de zip) : trou de séquence, queue invérifiable, nom non
--    reconnu — sur l'union des tables brutes portant le numéro de séquence.
-- 2. Contrôle intra-zip rétrospectif (spécifique ingestion, PAS dans la macro) : chaque
--    zip atterri a-t-il un XML en base pour chaque rang 1..Y de son compteur
--    `_XXXXX_YYYYY` ? Attrape un échec de linéarisation isolé que l'escalade d'échec de
--    chaîne (ADR-0037 étendu) tolère (documents > 0 au flux, mais un fichier manque).
--
-- Publications ponctuelles R64/R67 EXCLUES (séquence toujours figée à 00001, hors champ
-- — cf. macro). C15/R15/C12/X12/X13/F12(détail) portent un compteur intra-zip (au sein
-- de CHAQUE zip) ; R151 fait exception : son compteur est INTER-zips (un XML par zip,
-- Y = nombre de zips de la même séquence) — CTE dédiée `r151_*` ci-dessous. X12/X13
-- partagent `raw_affaires` : l'origine se dérive du nom de zip (même convention que
-- `stg_affaires`, qui la dérive du file_name).

with source_union as (
    select 'C15' as flux, _source_zip as nom_zip, file_name from {{ source('flux_raw', 'raw_c15') }}
    union all
    select 'R15', _source_zip, file_name from {{ source('flux_raw', 'raw_r15') }}
    union all
    select 'F15', _source_zip, file_name from {{ source('flux_raw', 'raw_f15') }}
    union all
    select 'F12', _source_zip, file_name from {{ source('flux_raw', 'raw_f12') }}
    union all
    select 'R151', _source_zip, file_name from {{ source('flux_raw', 'raw_r151') }}
    union all
    select 'C12', _source_zip, file_name from {{ source('flux_raw', 'raw_c12') }}
    union all
    select
        case when _source_zip like '%X12%' then 'X12' else 'X13' end,
        _source_zip,
        file_name
    from {{ source('flux_raw', 'raw_affaires') }}
),

anomalies_sequences as (
    {{ audit_sequences('source_union', 'nom_zip') }}
),

-- Intra-zip (C15/R15/C12/X12/X13/F12) : rang/total = les deux derniers groupes à 5
-- chiffres du `file_name` (convention `..._XXXXX_YYYYY.xml`, F12 détail `..._FL_XXXXX_YYYYY`).
intra_zip_brut as (
    select
        flux,
        nom_zip,
        try_cast(regexp_extract(file_name, '_([0-9]{5})_([0-9]{5})[^0-9]*$', 1) as integer) as rang,
        try_cast(regexp_extract(file_name, '_([0-9]{5})_([0-9]{5})[^0-9]*$', 2) as integer) as total
    from source_union
    where flux in ('C15', 'R15', 'C12', 'X12', 'X13', 'F12')
),

intra_zip_grp as (
    select
        flux,
        nom_zip,
        max(total)              as total,
        count(distinct rang)    as rangs_distincts,
        min(rang)                as rang_min,
        max(rang)                as rang_max
    from intra_zip_brut
    where rang is not null and total is not null
    group by flux, nom_zip
),

intra_zip_anomalies as (
    select
        flux,
        cast(null as varchar)                                              as cle_sequence,
        'intra_zip_incomplet'                                              as type_anomalie,
        nom_zip || ' (' || rangs_distincts || '/' || total || ')'          as seq_ou_plage
    from intra_zip_grp
    where rangs_distincts <> total or rang_min <> 1 or rang_max <> total
),

-- R151 : compteur INTER-zips. rang/total viennent du nom du ZIP lui-même (un seul XML
-- par zip), l'enveloppe est (abonnement, seq) — même clé de séquence que la macro.
r151_brut as (
    select
        regexp_extract(regexp_replace(nom_zip, '\.zip$', ''),
            '^[^_]+_R151_[^_]+_[^_]+_([^_]+)_([0-9]{5})_[^_]+_[0-9]{5}_[0-9]{5}_[0-9]+$', 1)
            || '|' ||
        regexp_extract(regexp_replace(nom_zip, '\.zip$', ''),
            '^[^_]+_R151_[^_]+_[^_]+_([^_]+)_([0-9]{5})_[^_]+_[0-9]{5}_[0-9]{5}_[0-9]+$', 2)
                                                                                            as enveloppe,
        try_cast(regexp_extract(regexp_replace(nom_zip, '\.zip$', ''),
            '^[^_]+_R151_[^_]+_[^_]+_[^_]+_[0-9]{5}_[^_]+_([0-9]{5})_([0-9]{5})_[0-9]+$', 1) as integer) as rang,
        try_cast(regexp_extract(regexp_replace(nom_zip, '\.zip$', ''),
            '^[^_]+_R151_[^_]+_[^_]+_[^_]+_[0-9]{5}_[^_]+_([0-9]{5})_([0-9]{5})_[0-9]+$', 2) as integer) as total
    from source_union
    where flux = 'R151'
),

r151_grp as (
    select
        enveloppe,
        max(total)              as total,
        count(distinct rang)    as rangs_distincts,
        min(rang)                as rang_min,
        max(rang)                as rang_max
    from r151_brut
    where enveloppe is not null and rang is not null and total is not null
    group by enveloppe
),

r151_anomalies as (
    select
        'R151'                                                    as flux,
        cast(null as varchar)                                     as cle_sequence,
        'intra_zip_incomplet'                                     as type_anomalie,
        enveloppe || ' (' || rangs_distincts || '/' || total || ')' as seq_ou_plage
    from r151_grp
    where rangs_distincts <> total or rang_min <> 1 or rang_max <> total
)

select * from anomalies_sequences
union all
select * from intra_zip_anomalies
union all
select * from r151_anomalies
