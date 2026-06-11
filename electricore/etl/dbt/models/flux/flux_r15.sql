-- Linéarisation R15 : une ligne par RELEVÉ, index par cadran en colonnes.
--
-- Le staging porte le grain relevé (releve_id). date_releve R15 est un horodatage
-- avec offset (instant-correct → TIMESTAMPTZ).

with flat as (
    select
        releve_id,
        pdl,
        cast(releve ->> '$.Date_Releve' as timestamptz) as date_releve,
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
    select releve_id, c.classe
    from {{ ref('stg_r15') }},
        unnest(cast(releve -> '$.Classe_Temporelle_Distributeur' as json[])) as c(classe)
),

-- Extraction en colonnes nommées AVANT le filtre (anti-pushdown, cf. flux_c15).
extrait as (
    select
        releve_id,
        classe ->> '$.Classe_Mesure'                                as classe_mesure,
        classe ->> '$.Sens_Mesure'                                  as sens_mesure,
        'index_' || lower(classe ->> '$.Id_Classe_Temporelle') || '_kwh' as cadran_col,
        cast(classe ->> '$.Valeur' as bigint)                      as valeur
    from classes
),

-- Index distributeur uniquement : Classe_Mesure = 1 (index cumulé) et Sens_Mesure = 0
-- (soutirage) — même condition que flux.yaml (corrigée après comparaison réelle).
filtre as (
    select releve_id, cadran_col, valeur
    from extrait
    where classe_mesure = '1' and sens_mesure = '0'
),

pivot_cadrans as (
    pivot filtre on cadran_col using first(valeur) group by releve_id
)

select * exclude (releve_id) from flat left join pivot_cadrans using (releve_id)
