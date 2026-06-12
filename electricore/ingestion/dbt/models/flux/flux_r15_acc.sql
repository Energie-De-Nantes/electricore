-- Linéarisation R15 autoconsommation : une ligne par RELEVÉ, énergies ea_* en colonnes.
--
-- Même source que flux_r15 (staging au grain relevé), mais on extrait les classes
-- d'autoconsommation (Classe_Mesure 3 à 6, préfixées ea_autoproduite_/ea_alloproduite_/
-- ea_autoconsommee_/ea_surplus_) au lieu de l'index distributeur. Extraction en
-- colonnes nommées avant le WHERE (anti-pushdown). Quand aucune donnée ACC : le PIVOT
-- ne produit aucune colonne ea_*, la ligne plate est préservée par le left join.

with flat as (
    select
        releve_id,
        pdl,
        cast(releve ->> '$.Date_Releve' as timestamptz) as date_releve,
        releve ->> '$.Id_Calendrier'                   as id_calendrier,
        releve ->> '$.Id_Calendrier_Distributeur'      as id_calendrier_distributeur,
        releve ->> '$.Ref_Situation_Contractuelle'     as ref_situation_contractuelle,
        releve ->> '$.Type_Compteur'                   as type_compteur,
        releve ->> '$.Motif_Releve'                    as motif_releve,
        releve ->> '$.Id_Affaire'                      as id_affaire,
        releve ->> '$.Nature_Index'                    as nature_index,
        releve ->> '$.Statut_Releve'                   as statut_releve,
        releve ->> '$.Autoconsommation_Collective'     as autoconsommation_collective,
        unite
    from {{ ref('stg_r15') }}
),

classes as (
    select releve_id, c.classe
    from {{ ref('stg_r15') }},
        unnest(cast(releve -> '$.Classe_Temporelle_Distributeur' as json[])) as c(classe)
),

extrait as (
    select
        releve_id,
        classe ->> '$.Classe_Mesure'               as classe_mesure,
        lower(classe ->> '$.Id_Classe_Temporelle') as cadran,
        classe ->> '$.Valeur'                      as valeur
    from classes
),

ea as (
    select
        releve_id,
        case classe_mesure
            when '3' then 'ea_autoproduite_'
            when '4' then 'ea_alloproduite_'
            when '5' then 'ea_autoconsommee_'
            when '6' then 'ea_surplus_'
        end || 'index_' || cadran || '_kwh' as ea_col,
        cast(valeur as bigint)              as valeur
    from extrait
    where classe_mesure in ('3', '4', '5', '6')
),

pivot_ea as (
    pivot ea on ea_col using first(valeur) group by releve_id
)

select * exclude (releve_id) from flat left join pivot_ea using (releve_id)
