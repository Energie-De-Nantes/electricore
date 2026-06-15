-- Linéarisation X12/X13 : une ligne par (affaire, jalon d'avancement).
--
-- Grain : un *jalon*. Une affaire accumule ses jalons (DMTR → DMREC → INPL → CPRE pour
-- un cas nominal). Les flux quotidiens sont des **snapshots cumulatifs** : chaque jour
-- reprend toute la liste de jalons d'une affaire qui a bougé → on déduplique sur la clé
-- logique `(affaire_id, jalon_num)` en gardant la livraison la plus récente
-- (max modification_date), exactement comme l'identité de relevé (ADR-0028).
--
-- On ne linéarise que le sous-ensemble essentiel (affaire, prestation, statut, point,
-- jalon, état) ; le reste du document profond dort dans le brut (`flux_raw.raw_affaires`),
-- re-matérialisable par `rebuild`. Accès JSON tolérant (pas de cast struct strict).

-- Champs d'affaire extraits en colonnes nommées AVANT l'unnest des jalons (piège DuckDB
-- du pushdown sous unnest, cf. flux_c15) ; `jalons` reste en JSON pour l'unnest aval.
with affaires as (
    select
        affaire_occ,
        origine,
        modification_date,
        affaire ->> '$."@id"'                                            as affaire_id,
        affaire ->> '$.donneesGenerales[0].statut[0]."@code"'            as statut,
        affaire ->> '$.donneesGenerales[0].donneesPoint[0].id'           as pdl,
        affaire ->> '$.donneesGenerales[0].segment'                      as segment,
        affaire ->> '$.demande[0].donneesGenerales[0].objet[0]."@code"'  as prestation,
        affaire ->> '$.demande[0].donneesGenerales[0].objet[0].libelle'  as prestation_libelle,
        affaire -> '$.donneesGenerales[0].jalons[0].jalon'               as jalons
    from {{ ref('stg_affaires') }}
),

jalons as (
    select
        a.affaire_id,
        a.origine,
        a.statut,
        a.pdl,
        a.segment,
        a.prestation,
        a.prestation_libelle,
        a.modification_date,
        j.jalon
    from affaires a,
        unnest(cast(a.jalons as json[])) as j(jalon)
)

select
    affaire_id,
    origine,
    prestation,
    prestation_libelle,
    statut,
    pdl,
    segment,
    cast(jalon ->> '$.num' as integer)                          as jalon_num,
    affaire_id || '#' || (jalon ->> '$.num')                    as affaire_jalon_id,
    cast(jalon ->> '$.dateHeure' as timestamptz)                as jalon_date_heure,
    cast(jalon ->> '$.affaireDateEffet' as date)                as affaire_date_effet,
    jalon ->> '$.affaireEtat[0]."@code"'                        as affaire_etat,
    jalon ->> '$.affaireEtat[0].libelle'                        as affaire_etat_libelle
from jalons
-- Dédup cross-livraisons : une ligne par (affaire, jalon), la plus récente gagne.
qualify row_number() over (
    partition by affaire_id, jalon_num order by modification_date desc
) = 1
