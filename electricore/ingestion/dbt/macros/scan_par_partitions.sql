-- Scan d'une table brute en partitions disjointes (UNION ALL sur hash(file_name)).
--
-- DuckDB évalue chaque fonction JSON sur un vecteur entier (≤ 2 048 lignes) : les arbres
-- yyjson de tous les documents du vecteur vivent en même temps, ~10 × le texte chacun.
-- Besoin ≈ (documents par vecteur) × (taille d'un document) × 10, quels que soient
-- threads/memory_limit. OOM prod : flux_f15_detail (Enargia, 02/09/2026), flux_r151
-- (edn, 04/10/2026). Filtrer AU SCAN de la source brute réduit les documents par vecteur
-- d'un facteur ~`partitions` ; filtrer à travers une vue staging ne descend pas sous ses
-- extractions. Partition exhaustive et disjointe : même résultat, à l'ordre près.
--
-- Condition aval : les unnest doivent être en SELECT. Un unnest latéral en FROM produit
-- une LEFT_DELIM_JOIN qui reforme des vecteurs pleins au-dessus des partitions
-- (cf. flux_f15_detail). Pièges DuckDB : docs/ingestion.md.
--
-- Mesures locales, plafond 1 GiB, documents distincts : R151 réel (319 Mo) et F15
-- synthétique (1 000 × 256 Ko) passent de OOM à OK.
--
-- Args :
--   relation   : source brute à scanner ; doit porter une colonne `file_name`.
--   partitions : nombre de partitions (défaut 32).
--
-- ponytail: contournement à plafond connu (#731) — chaque partition relit toute la
-- colonne content ; sortie structurelle = éclater les documents à l'atterrissage.
{% macro scan_par_partitions(relation, partitions=32) %}
    (
    {%- for k in range(partitions) %}
        select * from {{ relation }} where hash(file_name) % {{ partitions }} = {{ k }}
        {{- " union all" if not loop.last }}
    {%- endfor %}
    )
{% endmacro %}
