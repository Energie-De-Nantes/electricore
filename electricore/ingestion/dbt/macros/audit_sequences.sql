-- Brique commune d'audit de séquences Enedis (#645, PRD #644 ; réutilisée telle quelle
-- par le relais de flux #646 via import dbt package local).
--
-- SEUL lieu où vivent les règles de nomenclature Enedis : extraction du *numéro de
-- séquence* et de la *clé de séquence* depuis le nom du zip (glossaire, `CONTEXT.md`
-- « Complétude des flux »), propres à chaque type de flux (guides SGE) :
--   C15/R15 = (contrat, DIR) ; F15 = destinataire×contrat×DIR×type_facture×fréquence×
--   type_client ; F12 = destinataire×contrat×DIR ; C12 = contrat ; X12/X13 =
--   destinataire×code_flux ; R151 = abonnement (compteur INTER-zips, hors macro — la
--   caller ingestion le traite dans sa propre CTE, cf. `models/marts/audit_sequences.sql`).
--
-- Paramètres :
--   relation    : relation déjà UNIONÉE (une ligne par document), portant une colonne
--                 `flux` (code flux Enedis : 'C15'/'R15'/'F15'/'F12'/'R151'/'C12'/
--                 'X12'/'X13') — convention fixe, pas un paramètre (le caller construit
--                 l'union avec ce nom de colonne).
--   colonne_zip : nom de la colonne portant le nom de l'archive (`_source_zip` côté
--                 ingestion — même colonne côté relais #646).
--
-- Publications ponctuelles (R64, R67) HORS CHAMP : séquence toujours figée à 00001 —
-- ne pas les unioner dans `relation` (pas de règle ici, un flux inconnu du dict `regles`
-- retombe sur `nom_reconnu = false`, ce qui produirait de faux `nom_non_reconnu` sur des
-- publications ponctuelles jamais nommées selon ces règles).
--
-- Sortie : une ligne par anomalie — (flux, cle_sequence, type_anomalie, seq_ou_plage) :
--   - 'trou' : numéro absent ENTRE deux numéros observés d'une même clé de séquence.
--     Découpage en SEGMENTS d'abord (un segment casse quand le numéro repart en
--     arrière — redémarrage légitime, type F15/bascule Ginko v2) : un trou n'est
--     détecté qu'AU SEIN d'un segment, jamais à la jointure de deux segments.
--   - 'queue_inverifiable' : le numéro le plus RÉCENT (chronologiquement, via
--     l'horodate du nom de zip) d'une clé — jamais déclaré sain, un trou n'est
--     constatable qu'entre deux numéros observés (le suivant n'est pas encore arrivé).
--   - 'nom_non_reconnu' : nom de zip qui ne correspond à aucune règle connue pour son
--     flux — jamais ignoré en silence.
{% macro audit_sequences(relation, colonne_zip) %}

{%- set regles = {
    'C15':  {'pattern': '^[^_]+_C15_[^_]+_([^_]+)_([^_]+)_([0-9]{5})_[0-9]+$',
             'cle': '\\1|\\2', 'seq': '\\3'},
    'R15':  {'pattern': '^[^_]+_R15_[^_]+_([^_]+)_([^_]+)_([0-9]{5})_[0-9]+$',
             'cle': '\\1|\\2', 'seq': '\\3'},
    'F15':  {'pattern': '^[^_]+_F15_([^_]+)_([^_]+)_([^_]+)_([^_]+)_([^_]+)_([^_]+)_[^_]+_([0-9]{5})_[0-9]+$',
             'cle': '\\1|\\2|\\3|\\4|\\5|\\6', 'seq': '\\7'},
    'F12':  {'pattern': '^[^_]+_F12_([^_]+)_([^_]+)_([^_]+)_([0-9]{5})_[^_]+_[^_]+_[0-9]+$',
             'cle': '\\1|\\2|\\3', 'seq': '\\4'},
    'R151': {'pattern': '^[^_]+_R151_[^_]+_[^_]+_([^_]+)_([0-9]{5})_[^_]+_[0-9]{5}_[0-9]{5}_[0-9]+$',
             'cle': '\\1', 'seq': '\\2'},
    'C12':  {'pattern': '^[^_]+_C12_[^_]+_([^_]+)_([0-9]{5})_[0-9]+$',
             'cle': '\\1', 'seq': '\\2'},
    'X12':  {'pattern': '^[^_]+_X12_([^_]+)_([0-9]{5})_[0-9]+$',
             'cle': '\\1', 'seq': '\\2'},
    'X13':  {'pattern': '^[^_]+_X13_([^_]+)_([0-9]{5})_[0-9]+$',
             'cle': '\\1', 'seq': '\\2'},
} -%}

with brut as (
    select
        flux,
        {{ colonne_zip }}                               as nom_zip,
        regexp_replace({{ colonne_zip }}, '\.zip$', '')  as base
    from {{ relation }}
),

-- Extraction par flux — reconnaissance, clé de séquence (préfixée du flux : distingue
-- nativement X12/X13, même clé destinataire) et numéro de séquence.
extraits as (
    select
        flux,
        nom_zip,
        case flux
        {%- for f, r in regles.items() %}
            when '{{ f }}' then regexp_matches(base, '{{ r.pattern }}')
        {%- endfor %}
            else false
        end as nom_reconnu,
        case flux
        {%- for f, r in regles.items() %}
            when '{{ f }}' then case when regexp_matches(base, '{{ r.pattern }}')
                then flux || '|' || regexp_replace(base, '{{ r.pattern }}', '{{ r.cle }}') end
        {%- endfor %}
        end as cle_sequence,
        case flux
        {%- for f, r in regles.items() %}
            when '{{ f }}' then case when regexp_matches(base, '{{ r.pattern }}')
                then try_cast(regexp_replace(base, '{{ r.pattern }}', '{{ r.seq }}') as integer) end
        {%- endfor %}
        end as numero_sequence,
        -- Horodate générique (dernier champ `_YYYYMMDDHHMMSS` avant `.zip`, convention
        -- Enedis stable tous flux confondus) : ordre CHRONOLOGIQUE d'arrivée, seul repère
        -- fiable pour distinguer un redémarrage (numéro qui repart en arrière dans le
        -- temps) d'un simple tri numérique (qui confondrait redémarrage et trou).
        regexp_extract(base, '_([0-9]{14})$', 1)         as horodate
    from brut
),

-- Occurrences reconnues, dédoublonnées par (flux, clé, numéro) : une re-livraison sous
-- un nom de zip différent pour le même numéro ne doit pas se compter deux fois.
reconnus as (
    select distinct flux, cle_sequence, numero_sequence, horodate
    from extraits
    where nom_reconnu and cle_sequence is not null and numero_sequence is not null
),

ordonnes as (
    select
        *,
        lag(numero_sequence) over (partition by flux, cle_sequence order by horodate, numero_sequence) as seq_precedent
    from reconnus
),

-- Segments : un nouveau segment démarre dès que le numéro repart en arrière par rapport
-- au précédent (chronologiquement) — redémarrage légitime (F15/Ginko v2), pas un trou.
segments as (
    select
        *,
        sum(case when seq_precedent is not null and numero_sequence < seq_precedent then 1 else 0 end)
            over (partition by flux, cle_sequence order by horodate, numero_sequence rows unbounded preceding) as segment_id
    from ordonnes
),

trous as (
    select
        flux,
        cle_sequence,
        'trou' as type_anomalie,
        lpad(cast(precedent + 1 as varchar), 5, '0')
            || case when numero_sequence - precedent > 2
                    then '-' || lpad(cast(numero_sequence - 1 as varchar), 5, '0')
                    else '' end                                                       as seq_ou_plage
    from (
        select
            flux, cle_sequence, numero_sequence,
            lag(numero_sequence) over (partition by flux, cle_sequence, segment_id order by numero_sequence) as precedent
        from segments
    )
    where precedent is not null and numero_sequence - precedent > 1
),

-- Queue : le numéro le plus RÉCENT (horodate desc) de chaque clé — jamais numériquement
-- le plus grand (un redémarrage a un numéro plus petit mais plus récent : c'est LUI la
-- vraie queue invérifiable, pas le dernier numéro de l'ancien segment).
queues as (
    select flux, cle_sequence, 'queue_inverifiable' as type_anomalie, lpad(cast(numero_sequence as varchar), 5, '0') as seq_ou_plage
    from (
        select flux, cle_sequence, numero_sequence,
               row_number() over (partition by flux, cle_sequence order by horodate desc, numero_sequence desc) as rn
        from reconnus
    )
    where rn = 1
),

non_reconnus as (
    select flux, cast(null as varchar) as cle_sequence, 'nom_non_reconnu' as type_anomalie, nom_zip as seq_ou_plage
    from extraits
    where not nom_reconnu
)

select * from trous
union all
select * from queues
union all
select * from non_reconnus

{% endmacro %}


-- Data test générique (severity: warn côté schema.yml) : 0 anomalie attendue. Le mart
-- audit_sequences PRODUIT une ligne par anomalie — appliquer ce test au modèle entier
-- revient donc à vérifier qu'il est vide, sans dupliquer une requête `select * from`.
{% test aucune_anomalie(model) %}
select * from {{ model }}
{% endtest %}
