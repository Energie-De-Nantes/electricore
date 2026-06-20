-- Spine de la *Chronologie du contrat* (ADR-0041) : substrat relationnel (Class-Table
-- Inheritance) assemblé en dbt. Une ligne par fait d'une situation contractuelle (RSC) —
-- événements C15 ∪ grille FACTURATION (1ᵉʳ de chaque mois) — sur l'épine commune
-- (pdl, ref_situation_contractuelle, date_evenement, source, type_fait), augmentée des
-- attributs de SITUATION forward-fillés en SQL sur la timeline d'événements complète.
--
-- L'horizon reste un FILTRE côté cœur : la grille FACTURATION est pré-générée jusqu'à une
-- borne généreuse (var borne_facturation_genereuse), le cœur filtre `date_evenement <= horizon`
-- (pureté #179 préservée).
--
-- Tout le calcul calendaire se fait en wall-clock Europe/Paris (`AT TIME ZONE`) pour être
-- DÉTERMINISTE quel que soit le fuseau de session (poste local vs CI/VPS en UTC).

{% set borne_var = var('borne_facturation_genereuse', none) %}
{% if borne_var %}
{% set borne_tstz = "(timestamp '" ~ borne_var ~ "' at time zone 'Europe/Paris')" %}
{% else %}
{% set borne_tstz = "(date_trunc('month', now()) + interval '2 months')" %}
{% endif %}

with evenements as (
    select
        date_evenement,
        pdl,
        ref_situation_contractuelle,
        'flux_C15'  as source,
        'evenement' as type_fait,
        evenement_declencheur,
        type_evenement,
        segment_clientele,
        etat_contractuel,
        puissance_souscrite_kva,
        formule_tarifaire_acheminement,
        type_compteur,
        num_compteur,
        categorie,
        ref_demandeur,
        id_affaire,
        niveau_ouverture_services,
        date_changement_niveau_ouverture_services
    from {{ ref('flux_c15') }}
    where ref_situation_contractuelle is not null
),

-- Bornes calendaires par RSC : entrée (1ᵉʳ événement structurant d'entrée) et résiliation
-- (dernier événement de sortie, sinon borne généreuse). Grille = 1ᵉʳ de chaque mois de
-- l'entrée+1mois à la résiliation incluse (vide si l'intervalle est négatif).
bornes as (
    select
        ref_situation_contractuelle,
        any_value(pdl) as pdl,
        min(case when evenement_declencheur in ('CFNE', 'MES', 'PMES') then date_evenement end) as date_entree,
        max(case when evenement_declencheur in ('RES', 'CFNS') then date_evenement end)         as date_resiliation
    from evenements
    group by ref_situation_contractuelle
),

grille as (
    select
        ref_situation_contractuelle,
        pdl,
        unnest(generate_series(
            date_trunc('month', (date_entree at time zone 'Europe/Paris')) + interval '1 month',
            date_trunc('month', (coalesce(date_resiliation, {{ borne_tstz }}) at time zone 'Europe/Paris')),
            interval '1 month'
        )) as mois_naif
    from bornes
    where date_entree is not null
),

facturation as (
    select
        (mois_naif at time zone 'Europe/Paris') as date_evenement,
        pdl,
        ref_situation_contractuelle,
        'synthese_mensuelle' as source,
        'facturation'        as type_fait,
        'FACTURATION'        as evenement_declencheur,
        'artificiel'         as type_evenement,
        cast(null as varchar) as segment_clientele,
        cast(null as varchar) as etat_contractuel,
        cast(null as double)  as puissance_souscrite_kva,
        cast(null as varchar) as formule_tarifaire_acheminement,
        cast(null as varchar) as type_compteur,
        cast(null as varchar) as num_compteur,
        cast(null as varchar) as categorie,
        cast(null as varchar) as ref_demandeur,
        cast(null as varchar) as id_affaire,
        cast(null as varchar) as niveau_ouverture_services,
        cast(null as date)    as date_changement_niveau_ouverture_services
    from grille
),

faits as (
    select * from evenements
    union all by name
    select * from facturation
)

-- Forward-fill SQL des attributs de SITUATION sur la timeline d'événements complète : la
-- valeur portée est celle du dernier fait NON-NULL au-or-before (ROWS, comme le
-- `forward_fill` Polars). Départage des ex-aequo de timestamp : événement AVANT facturation
-- (stable-sort de l'ex-`pipeline_historique` : concat [événements, FACTURATION]). Le
-- garde-fou #374 garantit qu'aucun attribut de situation ne collisionne au même instant.
select
    date_evenement,
    pdl,
    ref_situation_contractuelle,
    source,
    type_fait,
    evenement_declencheur,
    type_evenement,
    {% set situation = [
        'segment_clientele', 'etat_contractuel', 'puissance_souscrite_kva',
        'formule_tarifaire_acheminement', 'type_compteur', 'num_compteur', 'categorie',
        'ref_demandeur', 'id_affaire', 'niveau_ouverture_services',
        'date_changement_niveau_ouverture_services'
    ] -%}
    {% for col in situation -%}
    last_value({{ col }} ignore nulls) over w as {{ col }}{{ "," if not loop.last }}
    {% endfor %}
from faits
window w as (
    partition by ref_situation_contractuelle
    order by date_evenement, (case when type_fait = 'facturation' then 1 else 0 end)
    rows between unbounded preceding and current row
)
