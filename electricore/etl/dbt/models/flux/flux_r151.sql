-- Linéarisation R151 : une ligne par RELEVÉ, index par cadran en colonnes.
--
-- Le staging porte déjà le grain relevé (releve_id) ; ici on aplatit les champs du
-- relevé et on pivote ses classes temporelles. date_releve R151 est une date nue.

with flat as (
    select
        releve_id,
        pdl,
        cast(releve ->> '$.Date_Releve' as date)       as date_releve,
        releve ->> '$.Id_Calendrier_Fournisseur'       as id_calendrier_fournisseur,
        releve ->> '$.Id_Calendrier_Distributeur'      as id_calendrier_distributeur,
        releve ->> '$.Id_Affaire'                      as id_affaire,
        unite
    from {{ ref('stg_r151') }}
),

classes as (
    select
        releve_id,
        'index_' || lower(c.classe ->> '$.Id_Classe_Temporelle') || '_kwh'  as cadran_col,
        cast(c.classe ->> '$.Valeur' as bigint)                            as valeur
    from {{ ref('stg_r151') }},
        unnest(cast(releve -> '$.Classe_Temporelle_Distributeur' as json[])) as c(classe)
),

pivot_cadrans as (
    pivot classes on cadran_col using first(valeur) group by releve_id
)

select * exclude (releve_id) from flat left join pivot_cadrans using (releve_id)
