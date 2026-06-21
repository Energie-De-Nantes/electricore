-- Linéarisation R15 : une ligne par RELEVÉ, index par cadran en colonnes.
--
-- Index distributeur uniquement : Classe_Mesure = 1 (index cumulé) et Sens_Mesure = 0
-- (soutirage) — même condition que flux.yaml. Agrégation conditionnelle sur le
-- DOMAINE FERMÉ des 7 cadrans (cf. flux_r151) : contrat de colonnes stable pour
-- les loaders core. Extraction en colonnes nommées AVANT le filtre (anti-pushdown).

-- Identité (ADR-0028, #232) : releve_id = CLÉ MÉTIER déterministe
-- (source|pdl|date|discriminant), stable sur re-livraison. R15 porte un Id_Releve
-- natif (provenance → id_releve) et un Nature_Index (REEL/ESTIME → nature canonique).
-- L'id d'occurrence fichier du staging (instable) reste exposé en provenance
-- forensique `occurrence_id`.
with flat as (
    select
        releve_id as occurrence_id,
        pdl,
        cast(releve ->> '$.Date_Releve' as timestamptz) as date_releve,
        releve ->> '$.Id_Releve'                       as id_releve,
        releve ->> '$.Nature_Index'                    as nature_index_source,
        releve ->> '$.Id_Calendrier'                   as id_calendrier,
        releve ->> '$.Ref_Situation_Contractuelle'     as ref_situation_contractuelle,
        releve ->> '$.Type_Compteur'                   as type_compteur,
        releve ->> '$.Motif_Releve'                    as motif_releve,
        releve ->> '$.Ref_Demandeur'                   as ref_demandeur,
        releve ->> '$.Id_Affaire'                      as id_affaire,
        unite
    from {{ ref('stg_r15') }}
),

classes as (
    select releve_id as occurrence_id, c.classe
    from {{ ref('stg_r15') }},
        unnest(cast(releve -> '$.Classe_Temporelle_Distributeur' as json[])) as c(classe)
),

extrait as (
    select
        occurrence_id,
        classe ->> '$.Classe_Mesure'               as classe_mesure,
        classe ->> '$.Sens_Mesure'                 as sens_mesure,
        lower(classe ->> '$.Id_Classe_Temporelle') as cadran,
        cast(classe ->> '$.Valeur' as bigint)      as valeur
    from classes
),

filtre as (
    select occurrence_id, cadran, valeur
    from extrait
    where classe_mesure = '1' and sens_mesure = '0'
),

cadrans as (
    select
        occurrence_id,
        {{ pivot_cadrans() }}
    from filtre
    group by occurrence_id
)

select
    {{ mint_releve_id("'flux_R15'", "pdl", "(date_releve at time zone 'Europe/Paris')", "false") }} as releve_id,
    id_releve,
    {{ nature_depuis_nature_index("nature_index_source") }}          as nature_index,
    -- Forme résiduelle descendue du loader (ADR-0042, #397) : `r15()` devient un SELECT *.
    -- Rename id_calendrier → id_calendrier_distributeur, placeholders null (FTA, calendrier
    -- fournisseur), littéraux source/ordre_index/unite/precision. date_releve est déjà un
    -- instant TIMESTAMPTZ (offset), rien à ré-ancrer.
    'flux_R15'              as source,
    false                  as ordre_index,
    'kWh'                  as unite,
    'kWh'                  as precision,
    cast(null as varchar)  as formule_tarifaire_acheminement,
    cast(null as varchar)  as id_calendrier_fournisseur,
    id_calendrier          as id_calendrier_distributeur,
    flat.* exclude (occurrence_id, id_releve, nature_index_source, unite, id_calendrier),
    occurrence_id,
    cadrans.* exclude (occurrence_id)
from flat left join cadrans using (occurrence_id)
