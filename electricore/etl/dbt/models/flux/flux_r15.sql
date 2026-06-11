-- Linéarisation R15 : une ligne par PRM, index par cadran en colonnes.
--
-- Même motif que R151 (Donnees_Releve unique, un seul unnest → PIVOT). date_releve
-- R15 porte un horodatage avec offset (instant-correct → TIMESTAMPTZ).

with flat as (
    select
        prm ->> '$.Id_PRM'                                        as pdl,
        cast(prm ->> '$.Donnees_Releve[0].Date_Releve' as timestamptz) as date_releve,
        prm ->> '$.Donnees_Releve[0].Id_Calendrier'              as id_calendrier,
        prm ->> '$.Donnees_Releve[0].Ref_Situation_Contractuelle' as ref_situation_contractuelle,
        prm ->> '$.Donnees_Releve[0].Type_Compteur'             as type_compteur,
        prm ->> '$.Donnees_Releve[0].Motif_Releve'             as motif_releve,
        prm ->> '$.Donnees_Releve[0].Ref_Demandeur'           as ref_demandeur,
        prm ->> '$.Donnees_Releve[0].Id_Affaire'              as id_affaire,
        unite
    from {{ ref('stg_r15') }}
),

classes as (
    select
        prm ->> '$.Id_PRM'                                                  as pdl,
        'index_' || lower(c.classe ->> '$.Id_Classe_Temporelle') || '_kwh'  as cadran_col,
        cast(c.classe ->> '$.Valeur' as bigint)                            as valeur
    from {{ ref('stg_r15') }},
        unnest(cast(prm -> '$.Donnees_Releve[0].Classe_Temporelle_Distributeur' as json[])) as c(classe)
),

pivot_cadrans as (
    pivot classes on cadran_col using first(valeur) group by pdl
)

select * from flat left join pivot_cadrans using (pdl)
