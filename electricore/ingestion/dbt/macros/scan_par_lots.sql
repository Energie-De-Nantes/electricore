-- Scan d'une table brute en `lots` partitions disjointes (UNION ALL sur hash(file_name)).
--
-- DuckDB évalue chaque fonction JSON sur un vecteur entier (≤ 2 048 lignes) : les arbres
-- yyjson de tous les documents du vecteur vivent en même temps, ~10 × le texte chacun.
-- Besoin ≈ (documents par vecteur) × (taille d'un document) × 10, quels que soient
-- threads/memory_limit ou l'écriture des unnest. OOM prod : flux_f15_detail (Enargia,
-- 02/09/2026), flux_r151 (edn, 04/10/2026). Filtrer AU SCAN de la source brute réduit
-- les documents par vecteur d'un facteur ~`lots` (filtrer à travers une vue staging ne
-- descend pas sous ses extractions). Partition exhaustive et disjointe : même résultat.
-- Mesures locales, plafond 1 GiB : R151 réel 319 Mo OOM → OK ; F15 synthétique 1 500 ×
-- 256 Ko OOM → OK.
-- ponytail: contournement à plafond connu (#731) — chaque branche relit toute la colonne
-- content (32 lectures) ; sortie structurelle = éclater les documents à l'atterrissage.
{% macro scan_par_lots(relation, lots=32) %}
    (
    {%- for k in range(lots) %}
        select * from {{ relation }} where hash(file_name) % {{ lots }} = {{ k }}
        {{- " union all" if not loop.last }}
    {%- endfor %}
    )
{% endmacro %}
