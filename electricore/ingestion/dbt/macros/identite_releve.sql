-- Mint de l'identité métier d'un relevé (ADR-0028, issue #232).
--
-- `releve_id` est une CLÉ MÉTIER déterministe, dérivée de la lecture *logique* :
-- `(source, pdl, date_releve, discriminant)`. Elle existe pour le cas dominant
-- (R151/R64 périodiques, sans id Enedis natif) et survit aux re-livraisons /
-- corrections : ré-ingérer le même relevé, ou recevoir une fenêtre R64 chevauchante
-- plus récente, ne change pas la clé (elle ne dépend ni de l'id d'occurrence fichier
-- `fichier#position`, ni de `modification_date`).
--
-- Discriminant par source : `ordre_index` (avant/après C15, périodique = avant) ou
-- `type_releve` (R64). À NE PAS confondre avec l'id d'occurrence fichier (instable,
-- rejeté comme clé par l'ADR-0028) ni avec l'`Id_Releve` natif (provenance seule).
--
-- Format : HASH COURT déterministe `substr(md5(source|pdl|date_iso|discriminant), 1, 16)`
-- (ADR-0038, #359). On ne change que l'ENCODAGE : les composantes d'identité (source,
-- pdl, date, discriminant) sont strictement inchangées, donc la stabilité (re-livraison,
-- `CORR ≡ BRUT`, dédup même-source, reprise #191) est préservée. La chaîne canonique
-- `source|pdl|date_iso|discriminant` reste l'empreinte logique — elle n'est plus stockée
-- telle quelle (verbeuse, timestamp+tz pénible à afficher/stocker côté ERP), mais hachée.
--
-- ⚠️ DÉTERMINISME / FUSEAU : `cast(timestamptz as varchar)` rend la date dans le fuseau
-- de SESSION (Paris en local, UTC en CI) → empreinte instable entre environnements. Les
-- appelants dont la date est un `timestamptz` (R15, C15) DOIVENT la normaliser :
-- `(date at time zone 'Europe/Paris')` (→ timestamp naïf, rendu stable). R151 passe une
-- `date` nue et R64 un `timestamp` naïf : déjà stables, pas de normalisation requise. Le
-- hash propage le déterminisme de la chaîne : même lecture logique → même empreinte → même hash.
{% macro mint_releve_id(source, pdl, date_releve, discriminant) %}
    substr(md5(
        {{ source }} || '|' || {{ pdl }} || '|' || cast({{ date_releve }} as varchar) || '|' || coalesce(cast({{ discriminant }} as varchar), '')
    ), 1, 16)
{% endmacro %}


-- Nature canonique d'un relevé (ADR-0028) : `réel` / `estimé` / `corrigé`.
--
-- Chaque source porte sa propre nomenclature ; on la projette sur le vocabulaire
-- canonique unique consommé par l'aval (RelevéIndex / ChronologieReleves) :
-- - R15/C15 `Nature_Index` : REEL → réel, ESTIME → estimé (autres ET absent/NULL →
--   estimé par défaut prudent, jamais réel par erreur — sur données réelles certains
--   relevés R15 n'ont pas de Nature_Index) ;
-- - R64 `etapeMetier` : BRUT/VALID → réel, CORR → corrigé ;
-- - R151 (télérelevé périodique) : pas de Nature_Index → réel par défaut.
{% macro nature_depuis_nature_index(nature_index) %}
    case
        when upper({{ nature_index }}) = 'REEL' then 'réel'
        when upper({{ nature_index }}) = 'ESTIME' then 'estimé'
        else 'estimé'
    end
{% endmacro %}


{% macro nature_depuis_etape_metier(etape_metier) %}
    case
        when {{ etape_metier }} = 'CORR' then 'corrigé'
        when {{ etape_metier }} in ('BRUT', 'VALID') then 'réel'
        when {{ etape_metier }} is null then 'réel'
        else 'réel'
    end
{% endmacro %}
