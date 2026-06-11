-- Linéarisation R15 : une ligne par PRM, index par cadran en colonnes.
--
-- Même motif que R151 (Donnees_Releve unique, un seul unnest → PIVOT). date_releve
-- R15 porte un horodatage avec offset (instant-correct → TIMESTAMPTZ).

with flat as (
    select
        prm_id,
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
    select prm_id, c.classe
    from {{ ref('stg_r15') }},
        unnest(cast(prm -> '$.Donnees_Releve[0].Classe_Temporelle_Distributeur' as json[])) as c(classe)
),

-- Extraction en colonnes nommées AVANT le filtre (anti-pushdown, cf. flux_c15).
extrait as (
    select
        prm_id,
        classe ->> '$.Classe_Mesure'                                as classe_mesure,
        classe ->> '$.Sens_Mesure'                                  as sens_mesure,
        'index_' || lower(classe ->> '$.Id_Classe_Temporelle') || '_kwh' as cadran_col,
        cast(classe ->> '$.Valeur' as bigint)                      as valeur
    from classes
),

-- Index distributeur uniquement : Classe_Mesure = 1 (énergie) et Sens_Mesure = 0
-- (soutirage). Le config legacy R15 n'avait PAS cette condition et tombait sur le
-- bon cadran par accident d'ordre (dernier-écrit) ; on filtre explicitement — le
-- comportement correct (décision : corriger plutôt que reproduire le legacy bancal).
filtre as (
    select prm_id, cadran_col, valeur
    from extrait
    where classe_mesure = '1' and sens_mesure = '0'
),

pivot_cadrans as (
    pivot filtre on cadran_col using first(valeur) group by prm_id
)

select * exclude (prm_id) from flat left join pivot_cadrans using (prm_id)
