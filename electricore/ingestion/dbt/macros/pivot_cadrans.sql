-- Fan-out des cadrans temporels (ADR-0035 §1, #292).
--
-- Émet le bloc d'agrégation conditionnelle par cadran — un `max(case when ...)` par
-- `index_*_kwh` — à partir de la var `cadrans_releve` (SOURCE UNIQUE = core/models/cadrans.py,
-- injectée dans dbt_project.yml). Remplace les `CASE WHEN` recopiés à la main dans chaque
-- modèle de linéarisation (flux_r151, flux_r64, flux_r15, flux_c15) et le contrat du mart.
-- La liste des 7 cadrans cesse d'être recopiée ~7 fois : une dérive est attrapée par
-- tests/ingestion/test_cadrans_source_unique.py (parité var dbt ↔ CADRANS).
--
-- Args :
--   valeur            : expression SQL agrégée (défaut 'valeur').
--   cadran_col        : colonne portant le code de cadran (défaut 'cadran').
--   condition_prefixe : fragment SQL préfixant le `case when` (ex. "prefixe = 'avant_' and "),
--                       pour le dépivot avant/après de flux_c15 ; vide par défaut.
--   alias_prefixe     : préfixe d'alias des colonnes (ex. 'avant_') ; vide par défaut.
--   grandeur          : grandeur portée par les colonnes ('index' par défaut → index cumulé
--                       des relevés R151/R64/R15/C15 ; 'energie' pour les énergies déjà
--                       différenciées par le distributeur du flux R67, ADR-0047). Le défaut
--                       'index' laisse R64/R151/R15/C15 strictement inchangés.
{% macro pivot_cadrans(valeur='valeur', cadran_col='cadran', condition_prefixe='', alias_prefixe='', grandeur='index') %}
    {%- for cadran in var('cadrans_releve') %}
    max(case when {{ condition_prefixe }}{{ cadran_col }} = '{{ cadran }}' then {{ valeur }} end) as {{ alias_prefixe }}{{ grandeur }}_{{ cadran }}_kwh{{ "," if not loop.last }}
    {%- endfor %}
{% endmacro %}


-- Noms des colonnes d'index (un par cadran), dérivés de la var `cadrans_releve`. Forme
-- LISTE Jinja — pour les arguments `fournit` du conformer (cf. conformer_releve.sql) et la
-- construction du contrat. `prefixe` ajoute un préfixe d'alias (ex. 'avant_').
{% macro cadrans_index_noms(prefixe='') %}
    {%- set noms = [] -%}
    {%- for cadran in var('cadrans_releve') -%}
        {%- do noms.append(prefixe ~ 'index_' ~ cadran ~ '_kwh') -%}
    {%- endfor -%}
    {{ return(noms) }}
{% endmacro %}


-- Idem, forme SQL : colonnes d'index séparées par des virgules, pour un SELECT.
{% macro cadrans_index_cols(prefixe='') %}
    {{- cadrans_index_noms(prefixe) | join(', ') -}}
{% endmacro %}


-- Renomme un bloc d'index préfixé vers le nom canonique : `<prefixe>index_X_kwh as index_X_kwh`
-- pour chaque cadran. Pour le dépivot avant/après de C15 (int_releves__c15).
{% macro cadrans_index_rename(prefixe) %}
    {%- for cadran in var('cadrans_releve') -%}
        {{ prefixe }}index_{{ cadran }}_kwh as index_{{ cadran }}_kwh{{ ", " if not loop.last }}
    {%- endfor -%}
{% endmacro %}
