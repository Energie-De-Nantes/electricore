-- Chronologie des relevés (ADR-0041) : projection ÉNERGIE de la spine, assemblée
-- entièrement en dbt. Une ligne par relevé qui borne une période d'énergie :
--   • relevés contractuels C15 aux événements qui IMPACTENT l'énergie (valeur native) ;
--   • bornes FACTURATION mensuelles (spine) appariées aux relevés périodiques (R151/R64)
--     au grain JOUR — equi-join `(pdl, jour)` qui REMPLACE l'asof « nearest 4h »
--     (`TOLERANCE_APPARIEMENT_RELEVES`) du `_assembler_chronologie` du cœur (ADR-0041 §7).
-- Dédoublonnage inter-sources par priorité explicite C15 > R64 > R151 (ADR-0028) via QUALIFY.
--
-- L'`impacte_energie` (rupture énergie) DESCEND ici : c'est la projection énergie de la
-- spine, et l'énergie ne garde que son découpage (#377). Calculé sur flux_c15 (avant/après),
-- miroir SQL de `expr_impacte_energie` du cœur (parité vérifiée, test_chronologie_releves).
--
-- L'horizon reste un FILTRE côté cœur ; ce mart est horizon-indépendant (bornes générées
-- par la spine jusqu'à une borne généreuse).

{% set cadrans = var('cadrans_releve') %}

with spine as (
    select * from {{ ref('spine_contrat') }}
),

releves as (
    select * from {{ ref('releves') }}
),

-- Rupture énergie par événement C15 (cf. `expr_impacte_energie` : structurant | changement
-- calendrier | changement index (avant↔après) | changement FTA (vs événement précédent)).
-- Comparaisons « entre deux valeurs non-nulles » seulement, comme le cœur.
evts_energie as (
    select
        pdl,
        date_evenement,
        (
            evenement_declencheur in ('CFNE', 'MES', 'PMES', 'CFNS', 'RES')
            or (
                avant_id_calendrier_distributeur is not null
                and apres_id_calendrier_distributeur is not null
                and avant_id_calendrier_distributeur <> apres_id_calendrier_distributeur
            )
            {% for c in cadrans %}
            or (
                avant_index_{{ c }}_kwh is not null and apres_index_{{ c }}_kwh is not null
                and avant_index_{{ c }}_kwh <> apres_index_{{ c }}_kwh
            )
            {% endfor %}
            or (
                lag(formule_tarifaire_acheminement) over (
                    partition by ref_situation_contractuelle order by date_evenement
                ) is not null
                and formule_tarifaire_acheminement is not null
                and lag(formule_tarifaire_acheminement) over (
                    partition by ref_situation_contractuelle order by date_evenement
                ) <> formule_tarifaire_acheminement
            )
        ) as impacte_energie
    from {{ ref('flux_c15') }}
    where ref_situation_contractuelle is not null
),

evts_impacte as (
    select distinct pdl, date_evenement from evts_energie where impacte_energie
),

-- Partie C15 : relevés contractuels (valeur de situation NATIVE) aux événements énergie.
c15_part as (
    select
        r.pdl,
        r.ref_situation_contractuelle,
        r.formule_tarifaire_acheminement,
        r.niveau_ouverture_services,
        r.date_releve,
        r.ordre_index,
        r.source,
        r.releve_id,
        r.nature_index,
        r.evenement_declencheur,
        r.id_calendrier_distributeur,
        {% for c in cadrans %}r.index_{{ c }}_kwh,{% endfor %}
        cast(null as boolean) as releve_manquant
    from releves r
    join evts_impacte e on r.pdl = e.pdl and r.date_releve = e.date_evenement
    where r.source = 'flux_C15'
),

-- Bornes FACTURATION (spine) appariées aux périodiques au grain JOUR (equi-join).
facturation as (
    select pdl, ref_situation_contractuelle, formule_tarifaire_acheminement, niveau_ouverture_services, date_evenement
    from spine
    where type_fait = 'facturation'
),

periodiques as (
    select * from releves where source <> 'flux_C15'
),

fact_part as (
    select
        f.pdl,
        f.ref_situation_contractuelle,
        f.formule_tarifaire_acheminement,
        f.niveau_ouverture_services,
        f.date_evenement as date_releve,
        false            as ordre_index,
        p.source,
        p.releve_id,
        p.nature_index,
        cast(null as varchar) as evenement_declencheur,
        p.id_calendrier_distributeur,
        {% for c in cadrans %}p.index_{{ c }}_kwh,{% endfor %}
        (p.releve_id is null) as releve_manquant
    from facturation f
    left join periodiques p
        on f.pdl = p.pdl
        and cast(f.date_evenement at time zone 'Europe/Paris' as date)
            = cast(p.date_releve at time zone 'Europe/Paris' as date)
),

combined as (
    select * from c15_part
    union all by name
    select * from fact_part
)

-- Priorité de source explicite (C15 > R64 > R151, ADR-0028) ; un manquant (source null)
-- ne gagne que s'il est seul sur (rsc, date_releve, ordre_index).
select *
from combined
qualify row_number() over (
    partition by ref_situation_contractuelle, date_releve, ordre_index
    order by case source
        when 'flux_C15' then 0
        when 'flux_R64' then 1
        when 'flux_R151' then 2
        else 99
    end
) = 1
