-- Contrat de colonnes du modèle de relevés canonique `releves` (ADR-0029).
--
-- SOURCE UNIQUE DE VÉRITÉ de l'ordre et du type de chaque colonne du contrat. Ajouter
-- une colonne au modèle canonique = l'ajouter ICI, une fois, et non dans chaque branche
-- de l'union (fin de la transcription bouchée `cast(null)` répétée par source).
--
-- `source` n'est PAS dans le contrat : il est estampillé par le conformer (constante de
-- branche). `id_releve` natif Enedis n'y est PAS non plus (#304) : toujours NULL pour les
-- trois sources vivantes (R151/R64 n'en ont pas ; C15 le nullait), aucun consommateur ne
-- le lit. La traçabilité repose sur `releve_id` (clé métier ADR-0028) + `occurrence_id`
-- (provenance fichier). Réintroduit dans le journal des relevés utilisés, pas ici, le jour
-- où la régularisation aura besoin de l'id officiel (cf. #305).
{% macro contrat_releve() %}
  {%- set colonnes = [
    ('releve_id', 'varchar'),
    ('pdl', 'varchar'),
    ('date_releve', 'timestamptz'),
    ('ordre_index', 'boolean'),
    ('nature_index', 'varchar'),
    ('occurrence_id', 'varchar'),
    ('ref_situation_contractuelle', 'varchar'),
    ('formule_tarifaire_acheminement', 'varchar'),
    ('niveau_ouverture_services', 'varchar'),
    ('id_calendrier_distributeur', 'varchar'),
    ('evenement_declencheur', 'varchar')
  ] -%}
  {#- Colonnes d'index DÉRIVÉES de cadrans.py (var `cadrans_releve`, ADR-0035 §1) : le nom
      vient du cadran (source unique), le type bigint est porté par dbt (kWh entiers, ADR-0034). -#}
  {%- for nom in cadrans_index_noms() -%}
    {%- do colonnes.append((nom, 'bigint')) -%}
  {%- endfor -%}
  {{ return(colonnes) }}
{% endmacro %}


-- Projette une source de relevés (CTE ou modèle) sur le contrat canonique (ADR-0029).
--
-- PATTERN « adapter de source » : chaque source est une CTE / un modèle ÉTROIT qui n'émet
-- que les colonnes qu'elle porte vraiment, déjà nommées et typées canoniquement, avec sa
-- logique propre (harmonisation de date R151 +1j, ancrage Paris R64, dépivot C15…). Ce
-- macro fait le reste, une fois : estampille `source`, et remplit en NULL typé tout ce que
-- la source n'a pas. `fournit` = la liste des colonnes du contrat que la source porte → le
-- complément (« ce que la source N'A PAS ») est LISIBLE au point d'appel, au lieu d'être
-- noyé en `cast(null)` dans une union large.
--
-- Ajouter une source = écrire son adapter étroit + un appel ici. Ajouter une colonne =
-- l'ajouter à `contrat_releve()` (+ aux `fournit` des sources qui la portent).
{% macro conformer_au_contrat_releve(relation, source, fournit) %}
    select
        '{{ source }}' as source
        {%- for col, type in contrat_releve() %},
        {% if col in fournit %}{{ col }}{% else %}cast(null as {{ type }}){% endif %} as {{ col }}
        {%- endfor %}
    from {{ relation }}
{% endmacro %}
