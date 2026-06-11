-- Linéarisation R151 : une ligne par PRM, index par cadran en colonnes.
--
-- Donnees_Releve unique (indexé [0]), un seul niveau d'unnest (les classes
-- temporelles) → PIVOT natif possible (contrairement à C15, double unnest + WHERE).
-- Le pivot découvre les cadrans présents (4 saisonniers + INCONNU éventuel) sans
-- domaine codé en dur. date_releve R151 est une date nue (pas d'horodatage).

with flat as (
    select
        prm ->> '$.Id_PRM'                                              as pdl,
        cast(prm ->> '$.Donnees_Releve[0].Date_Releve' as date)         as date_releve,
        prm ->> '$.Donnees_Releve[0].Id_Calendrier_Fournisseur'         as id_calendrier_fournisseur,
        prm ->> '$.Donnees_Releve[0].Id_Calendrier_Distributeur'        as id_calendrier_distributeur,
        prm ->> '$.Donnees_Releve[0].Id_Affaire'                        as id_affaire,
        unite
    from {{ ref('stg_r151') }}
),

classes as (
    select
        prm ->> '$.Id_PRM'                                                  as pdl,
        'index_' || lower(c.classe ->> '$.Id_Classe_Temporelle') || '_kwh'  as cadran_col,
        cast(c.classe ->> '$.Valeur' as bigint)                            as valeur
    from {{ ref('stg_r151') }},
        unnest(cast(prm -> '$.Donnees_Releve[0].Classe_Temporelle_Distributeur' as json[])) as c(classe)
),

pivot_cadrans as (
    pivot classes on cadran_col using first(valeur) group by pdl
)

select * from flat left join pivot_cadrans using (pdl)
