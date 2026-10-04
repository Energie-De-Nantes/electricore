-- Scan d'une table brute en `lots` partitions disjointes (UNION ALL sur hash(file_name)).
--
-- Le parse JSON DuckDB alloue son arène PAR CHUNK (jusqu'à 2048 lignes) : sur les gros
-- documents (R151 ~0,4 Mo/fichier, R64 jusqu'à ~25 Mo), un chunk plein de fichiers
-- dépasse la RAM (OOM prod 6,2 GiB sur flux_r151, threads=1 n'y change rien). Filtrer
-- le scan en lots réduit le nombre de lignes par chunk, donc le pic mémoire, d'un
-- facteur ~`lots` (local : > 3 GiB → < 512 MiB). Partition exhaustive et disjointe :
-- même résultat, ordre des lignes sans importance (preserve_insertion_order: false).
-- ponytail: lots fixe ; à monter si le volume par fichier explose encore.
{% macro scan_par_lots(relation, lots=32) %}
    (
    {%- for k in range(lots) %}
        select * from {{ relation }} where hash(file_name) % {{ lots }} = {{ k }}
        {{- " union all" if not loop.last }}
    {%- endfor %}
    )
{% endmacro %}
