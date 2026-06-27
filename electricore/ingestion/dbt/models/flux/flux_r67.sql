-- Linéarisation R67 (« mesures facturantes », ADR-0047) : une ligne par
-- (pdl, debut, fin), énergie de consommation par cadran en colonnes.
--
-- R67 ≠ R64 : sa feuille JSON porte une ÉNERGIE consommée sur une PÉRIODE
-- `[debut, fin)` (quantite/dbtMesure/finMesure/codeNature), déjà différenciée par le
-- distributeur — pas un index cumulé à un instant. C'est un ASSET PARALLÈLE, jamais
-- unioné dans `releves`/`chronologie_releves` (les 5 ruptures, ADR-0047 §Raison).
--
-- Sémantique (ADR-0047) :
-- - UNION de tous les `contexte[]` (les motifs CYCL/CFNS/MCT/AUTRE partitionnent le
--   temps sans recouvrement → l'union tuile sans double-compte) ; `id_motif_releve`
--   gardé en annotation (1:1 par intervalle) ;
-- - COALESCE 1 grille par période, priorité `D/DI000003 ≻ D/DI000001 ≻ F` (repli F
--   forcé quand le distributeur manque — non-Linky — ou est dégénéré ; la bascule
--   DI000001→DI000003 en cours de mesure est gérée car le rang se calcule PAR
--   intervalle) ; `code_grille`/`code_calendrier` gardés en provenance ;
-- - PIVOT wide `energie_<cadran>_kwh` via `pivot_cadrans(grandeur='energie')` ;
-- - Wh → kWh par `floor` uniforme (`// 1000`, ADR-0034) ; NÉGATIFS PRÉSERVÉS (la régul
--   `codeNature=C` est une régularisation physique, pas un signe comptable) ;
-- - bornes `debut`/`fin` en JOUR CIVIL `DATE`, intervalle demi-ouvert (ADR-0042 :
--   ce sont des bornes de période, pas des instants — aucun +1j) ; la `periode`
--   (fenêtre d'éligibilité M023) est gardée À PART, jamais une borne d'énergie ;
-- - `nature` depuis `codeNature` (pas `etapeMetier`, toujours FACT) ; `code_statut`
--   en passthrough.
--
-- Accès JSON tolérant + extraction en colonnes nommées AVANT tout filtre
-- (anti-pushdown DuckDB, cf. flux_r64/flux_c15).

with contextes as (
    select
        mesure_id,
        mesure ->> '$.idPrm'                  as pdl,
        mesure ->> '$.periode.dateDebut'      as periode_debut,
        mesure ->> '$.periode.dateFin'        as periode_fin,
        unnest(ctxs)                          as ctx
    from (
        select mesure_id, mesure, cast(mesure -> '$.contexte' as json[]) as ctxs
        from {{ ref('stg_r67') }}
    )
),

grandeurs as (
    select
        mesure_id,
        pdl,
        periode_debut,
        periode_fin,
        ctx ->> '$.etapeMetier'      as etape_metier,
        ctx ->> '$.idMotifReleve'    as id_motif_releve,
        unnest(gs)                   as g
    from (
        select
            mesure_id, pdl, periode_debut, periode_fin,
            ctx,
            cast(ctx -> '$.grandeur' as json[]) as gs
        from contextes
    )
),

grandeurs_extraites as (
    select
        mesure_id,
        pdl,
        periode_debut,
        periode_fin,
        etape_metier,
        id_motif_releve,
        g ->> '$.grandeurMetier'     as grandeur_metier,
        g ->> '$.grandeurPhysique'   as grandeur_physique,
        g ->> '$.unite'              as unite,
        g
    from grandeurs
),

calendriers as (
    select
        mesure_id,
        pdl,
        periode_debut,
        periode_fin,
        etape_metier,
        id_motif_releve,
        grandeur_metier,
        grandeur_physique,
        unite,
        cal ->> '$.codeGrille'       as code_grille,
        cal ->> '$.codeCalendrier'   as code_calendrier,
        cal
    from (
        select
            mesure_id, pdl, periode_debut, periode_fin, etape_metier, id_motif_releve,
            grandeur_metier, grandeur_physique, unite,
            unnest(cast(g -> '$.calendrier' as json[])) as cal
        from grandeurs_extraites
    )
),

classes as (
    select
        mesure_id,
        pdl,
        periode_debut,
        periode_fin,
        etape_metier,
        id_motif_releve,
        grandeur_metier,
        grandeur_physique,
        unite,
        code_grille,
        code_calendrier,
        classe ->> '$.idClasseTemporelle'  as id_classe,
        cast(classe -> '$.quantite' as json[]) as quantites_brutes
    from (
        select
            mesure_id, pdl, periode_debut, periode_fin, etape_metier, id_motif_releve,
            grandeur_metier, grandeur_physique, unite, code_grille, code_calendrier,
            unnest(cast(cal -> '$.classeTemporelle' as json[])) as classe
        from calendriers
    )
),

-- Quantités à plat (anti-pushdown : unnest dans le FROM en table function, comme le CTE
-- `points` de flux_r64 — extraire les champs nommés AVANT tout filtre). Une ligne par
-- (intervalle, cadran, grille). Filtres CONS/EA appliqués ici.
quantites_extraites as (
    select
        mesure_id,
        pdl,
        periode_debut,
        periode_fin,
        id_motif_releve,
        unite,
        code_grille,
        code_calendrier,
        lower(id_classe)             as cadran,
        q ->> '$.dbtMesure'          as dbt_mesure,
        q ->> '$.finMesure'          as fin_mesure,
        q ->> '$.codeNature'         as code_nature,
        q ->> '$.codeStatut'         as code_statut,
        q ->> '$.dateCreation'       as date_creation,
        q ->> '$.quantite'           as quantite_brute
    from (
        select
            mesure_id, pdl, periode_debut, periode_fin, id_motif_releve, unite,
            code_grille, code_calendrier, id_classe, quantites_brutes,
            grandeur_metier, grandeur_physique
        from classes
        where grandeur_metier = 'CONS'
          and grandeur_physique = 'EA'
          and id_classe is not null
    ), unnest(quantites_brutes) as t(q)
),

quantites as (
    select
        mesure_id,
        pdl,
        cast(periode_debut as timestamp)  as periode_debut,
        cast(periode_fin as timestamp)    as periode_fin,
        id_motif_releve,
        unite,
        code_grille,
        code_calendrier,
        cadran,
        cast(dbt_mesure as date)          as debut,
        cast(fin_mesure as date)          as fin,
        code_nature,
        code_statut,
        cast(date_creation as timestamp)  as date_creation,
        cast(quantite_brute as bigint)    as quantite
    from quantites_extraites
    where dbt_mesure is not null
      and fin_mesure is not null
),

-- Rang de priorité de grille (ADR-0047) : D/DI000003 ≻ D/DI000001 ≻ (autre D) ≻ F.
-- Calculé PAR intervalle `(pdl, debut, fin)` → la bascule DI000001→DI000003 en cours de
-- mesure (M0AIMFND) tombe juste : l'entrée garde DI000001 (seule grille D dispo), la
-- suite prend DI000003. Repli F forcé quand aucune grille D n'existe (non-Linky M0AIMB1Z).
quantites_rang as (
    select
        *,
        case
            when code_grille = 'D' and code_calendrier = 'DI000003' then 0
            when code_grille = 'D' and code_calendrier = 'DI000001' then 1
            when code_grille = 'D'                                  then 2
            else 3
        end as rang_grille
    from quantites
),

-- Coalesce : pour chaque intervalle, ne garder que les quantités de la grille gagnante
-- (rang minimal présent). DI000003 étant un raffinement strict du fournisseur, on ne
-- perd rien (le total fournisseur est re-dérivable par agrégation, ADR-0047).
quantites_coalescees as (
    select *
    from quantites_rang
    qualify rang_grille = min(rang_grille) over (partition by pdl, debut, fin)
),

-- Pivot wide + Wh→kWh floor. Une ligne par (pdl, debut, fin) : la grille gagnante est
-- unique par intervalle (coalesce ci-dessus), le motif est 1:1, nature/statut sont
-- constants sur les cadrans de l'intervalle (vérifié) → max() déterministe.
pivote as (
    select
        mesure_id,
        pdl,
        debut,
        fin,
        max(periode_debut)     as periode_debut,
        max(periode_fin)       as periode_fin,
        max(id_motif_releve)   as id_motif_releve,
        max(code_grille)       as code_grille,
        max(code_calendrier)   as code_calendrier,
        max(code_nature)       as code_nature,
        max(code_statut)       as code_statut,
        max(date_creation)     as date_creation,
        max(unite)             as unite,
        {{ pivot_cadrans(valeur='quantite', grandeur='energie') }}
    from quantites_coalescees
    group by mesure_id, pdl, debut, fin
),

en_tete as (
    select distinct mesure_id, id_demande, si_demandeur, code_flux, format, modification_date
    from {{ ref('stg_r67') }}
)

-- Clé propre `periode_id` (ADR-0047) : md5(flux_R67|pdl|debut|fin)[:16], minté sur dates
-- NAÏVES (stabilité tz, ADR-0028), dans un espace de nommage DISTINCT de `releve_id`
-- (R67 hors espace des relevés index, ADR-0028 amendé). `debut`/`fin` sont des DATE nues.
select
    {{ mint_releve_id("'flux_R67'", "pivote.pdl", "pivote.debut", "pivote.fin") }} as periode_id,
    pivote.pdl,
    pivote.debut,
    pivote.fin,
    -- La `periode` (fenêtre d'éligibilité M023, plus large que la couverture data) gardée
    -- À PART : jamais une borne d'énergie (ADR-0047). Instant Paris pour un dtype stable.
    timezone('Europe/Paris', pivote.periode_debut) as periode_debut,
    timezone('Europe/Paris', pivote.periode_fin)   as periode_fin,
    pivote.id_motif_releve,
    {{ nature_depuis_code_nature("pivote.code_nature") }} as nature,
    pivote.code_nature,
    pivote.code_statut,
    pivote.code_grille,
    pivote.code_calendrier,
    -- Unité normalisée en kWh au boundary (ADR-0034) : Enedis livre en Wh.
    case when pivote.unite = 'Wh' then 'kWh' else pivote.unite end as unite,
    en_tete.id_demande,
    en_tete.si_demandeur,
    en_tete.code_flux,
    en_tete.format,
    mesure_id as occurrence_id,
    'flux_R67' as source,
    -- Wh → kWh par floor (`// 1000`, ADR-0034). NÉGATIFS PRÉSERVÉS (régul physique
    -- codeNature=C) : pas de `ge >= 0`. // = division entière DuckDB, NULL-safe, et
    -- floor sur entiers négatifs en DuckDB suit la division Python (vers −∞).
    case when pivote.unite = 'Wh' then energie_base_kwh // 1000 else energie_base_kwh end as energie_base_kwh,
    case when pivote.unite = 'Wh' then energie_hp_kwh   // 1000 else energie_hp_kwh   end as energie_hp_kwh,
    case when pivote.unite = 'Wh' then energie_hc_kwh   // 1000 else energie_hc_kwh   end as energie_hc_kwh,
    case when pivote.unite = 'Wh' then energie_hph_kwh  // 1000 else energie_hph_kwh  end as energie_hph_kwh,
    case when pivote.unite = 'Wh' then energie_hpb_kwh  // 1000 else energie_hpb_kwh  end as energie_hpb_kwh,
    case when pivote.unite = 'Wh' then energie_hch_kwh  // 1000 else energie_hch_kwh  end as energie_hch_kwh,
    case when pivote.unite = 'Wh' then energie_hcb_kwh  // 1000 else energie_hcb_kwh  end as energie_hcb_kwh
from pivote
join en_tete using (mesure_id)
-- Dédup re-livraison (ADR-0047) : une re-publication restatée du même (pdl, debut, fin,
-- grille) → on garde la plus récente (modification_date SFTP, puis dateCreation Enedis).
-- L'échantillon est 100 % `Initial`, la clé est posée par précaution (cf. ADR-0047 §Limite).
qualify row_number() over (
    partition by pivote.pdl, pivote.debut, pivote.fin, pivote.code_grille
    order by en_tete.modification_date desc, pivote.date_creation desc
) = 1
