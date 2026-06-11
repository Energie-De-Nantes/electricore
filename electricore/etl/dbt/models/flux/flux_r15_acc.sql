-- Linéarisation R15 autoconsommation : une ligne par PRM, énergies ea_* en colonnes.
--
-- Même source brute que flux_r15, mais on extrait les classes d'autoconsommation
-- (Classe_Mesure 3 à 6, préfixées ea_autoproduite_/ea_alloproduite_/ea_autoconsommee_/
-- ea_surplus_) au lieu de l'index distributeur. Comme C15, le filtre sur la classe
-- impose d'extraire en colonnes nommées AVANT le WHERE (sinon pushdown DuckDB sous
-- l'unnest). Cas vide aujourd'hui (aucune donnée ACC locale) : le PIVOT ne produit
-- alors aucune colonne ea_*, la ligne plate est préservée par le left join.

with flat as (
    select
        prm ->> '$.Id_PRM'                                        as pdl,
        cast(prm ->> '$.Donnees_Releve[0].Date_Releve' as timestamptz) as date_releve,
        prm ->> '$.Donnees_Releve[0].Id_Calendrier'              as id_calendrier,
        prm ->> '$.Donnees_Releve[0].Id_Calendrier_Distributeur' as id_calendrier_distributeur,
        prm ->> '$.Donnees_Releve[0].Ref_Situation_Contractuelle' as ref_situation_contractuelle,
        prm ->> '$.Donnees_Releve[0].Type_Compteur'             as type_compteur,
        prm ->> '$.Donnees_Releve[0].Motif_Releve'             as motif_releve,
        prm ->> '$.Donnees_Releve[0].Id_Affaire'              as id_affaire,
        prm ->> '$.Donnees_Releve[0].Nature_Index'           as nature_index,
        prm ->> '$.Donnees_Releve[0].Statut_Releve'          as statut_releve,
        prm ->> '$.Donnees_Releve[0].Autoconsommation_Collective' as autoconsommation_collective,
        unite
    from {{ ref('stg_r15') }}
),

classes as (
    select prm ->> '$.Id_PRM' as pdl, c.classe
    from {{ ref('stg_r15') }},
        unnest(cast(prm -> '$.Donnees_Releve[0].Classe_Temporelle_Distributeur' as json[])) as c(classe)
),

extrait as (
    select
        pdl,
        classe ->> '$.Classe_Mesure'               as classe_mesure,
        lower(classe ->> '$.Id_Classe_Temporelle') as cadran,
        classe ->> '$.Valeur'                      as valeur
    from classes
),

ea as (
    select
        pdl,
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
    pivot ea on ea_col using first(valeur) group by pdl
)

select * from flat left join pivot_ea using (pdl)
