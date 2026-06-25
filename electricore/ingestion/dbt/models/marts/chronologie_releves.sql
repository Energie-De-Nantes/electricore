-- Chronologie des relevés — VUE (ADR-0045, #431) : ré-attache le payload d'index au mart
-- MINCE `chronologie_releves_situation` par **star-join** sur `releve_id`, faisant de
-- `releves` la **source de vérité unique** des valeurs d'index (ADR-0029/0038). C'est le
-- modèle consommé par le cœur (loader `chronologie()` / `pipeline_energie`) ; son contrat
-- `ChronologieReleves` est inchangé (mêmes colonnes qu'avant la normalisation).
--
-- Matérialisée en VUE : un join de plus à la lecture (~10 ms à 442k lignes, immatériel — le
-- spike d'ADR-0045 a montré que surrogate/FK ne se justifient pas). Le mart mince reste une
-- table (l'appariement equi-join jour + priorité QUALIFY n'est pas trivial).
--
-- `releve_id` est UNIQUE dans `releves` (clé métier ADR-0028, testé) ⇒ le left join est 1:1,
-- le grain `(rsc, date_releve, ordre_index)` du mart est préservé. Une borne sans relevé
-- apparié (`releve_manquant`, `releve_id` nul) ne matche aucune ligne ⇒ payload NUL — comme
-- la recopie d'avant. Le payload migré (fonction 1:1 de `releve_id`) : `nature_index`,
-- `id_calendrier_distributeur`, `index_*_kwh`. La situation (FTA/niveau/RSC) et
-- `evenement_declencheur` restent NATIFS dans le mart mince (≠ `releves[releve_id]`).

{{ config(materialized='view') }}

{% set cadrans = var('cadrans_releve') %}

select
    s.*,
    r.nature_index,
    r.id_calendrier_distributeur,
    {% for c in cadrans %}r.index_{{ c }}_kwh{{ "," if not loop.last }}{% endfor %}
from {{ ref('chronologie_releves_situation') }} s
left join {{ ref('releves') }} r on s.releve_id = r.releve_id
