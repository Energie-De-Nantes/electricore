-- Garde-fou ADR-0041 §7 (#374) : sur la Chronologie du contrat, le timestamp
-- `date_evenement` ordonne le forward-fill de situation
-- (`last_value(<attr> IGNORE NULLS) OVER (PARTITION BY ref_situation_contractuelle
-- ORDER BY date_evenement)`). Pour chaque attribut de situation, aucun couple
-- d'événements de la MÊME RSC ne doit porter DEUX valeurs non-nulles DIFFÉRENTES au
-- MÊME instant — sinon l'ordre du forward-fill devient indéterminé. Le test échoue
-- (lignes renvoyées) dès qu'une collision apparaît. Vérifié 0 au spike prod (20/06/2026).
--
-- Tag `garde_donnees_reelles` : invariant d'INTÉGRITÉ des données Enedis réelles, à
-- exclure des harnais golden synthétiques (les fixtures XSD « instance maximale » cyclent
-- les énumérations → collisions artificielles). Tourne en prod (`construire_dbt` →
-- `+flux_c15`) et dans son propre harnais (test_dbt_spine_garde_collision.py).
{{ config(tags=['garde_donnees_reelles']) }}

with attributs as (
    select
        ref_situation_contractuelle,
        date_evenement,
        'formule_tarifaire_acheminement' as attribut,
        cast(formule_tarifaire_acheminement as varchar) as valeur
    from {{ ref('flux_c15') }}
    where ref_situation_contractuelle is not null
      and formule_tarifaire_acheminement is not null

    union all
    select
        ref_situation_contractuelle,
        date_evenement,
        'puissance_souscrite_kva',
        cast(puissance_souscrite_kva as varchar)
    from {{ ref('flux_c15') }}
    where ref_situation_contractuelle is not null
      and puissance_souscrite_kva is not null

    union all
    select
        ref_situation_contractuelle,
        date_evenement,
        'niveau_ouverture_services',
        cast(niveau_ouverture_services as varchar)
    from {{ ref('flux_c15') }}
    where ref_situation_contractuelle is not null
      and niveau_ouverture_services is not null

    union all
    select
        ref_situation_contractuelle,
        date_evenement,
        'segment_clientele',
        cast(segment_clientele as varchar)
    from {{ ref('flux_c15') }}
    where ref_situation_contractuelle is not null
      and segment_clientele is not null
)

select
    ref_situation_contractuelle,
    date_evenement,
    attribut,
    count(distinct valeur) as n_valeurs
from attributs
group by ref_situation_contractuelle, date_evenement, attribut
having count(distinct valeur) > 1
