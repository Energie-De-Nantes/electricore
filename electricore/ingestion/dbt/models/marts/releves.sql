-- Modèle de relevés canonique (ADR-0029) : la ligne de temps des relevés consommée
-- par l'aval. Union des sources périodiques télérelevées (1 ligne = 1 relevé), dates
-- harmonisées « début de journée » Europe/Paris (ADR-0003 : R151 J → J+1), dedup
-- même-source par releve_id (clé métier ADR-0028 — on garde la livraison la plus
-- récente). Reproduit le périmètre de l'ex-loader `releves_harmonises` (R151 + R64),
-- l'entrée actuelle du pipeline énergie.
--
-- À VENIR : relevés C15 avant/après dépivotés (#242), enrichissement contractuel
-- RSC/FTA piloté par C15 (#243). L'arbitrage de priorité inter-sources (C15 > R64 >
-- R151) et la sélection des bornes de facturation restent au cœur (#244, ADR-0029).

with unifies as (
    -- R151 : télérelevés périodiques. Harmonisation J → J+1 (ADR-0003) puis ancrage
    -- heure-mur Paris. releve_id (clé métier) est minté sur la date BRUTE en amont
    -- (flux_r151) : l'harmonisation d'affichage ne change pas l'identité.
    select
        releve_id,
        'flux_R151'                                                                 as source,
        pdl,
        timezone('Europe/Paris', cast(date_releve as timestamp) + interval '1 day') as date_releve,
        false                                                                       as ordre_index,
        nature_index,
        id_releve,
        occurrence_id,
        cast(null as varchar)                                                       as ref_situation_contractuelle,
        cast(null as varchar)                                                       as formule_tarifaire_acheminement,
        id_calendrier_distributeur,
        index_base_kwh, index_hp_kwh, index_hc_kwh,
        index_hph_kwh, index_hpb_kwh, index_hch_kwh, index_hcb_kwh
    from {{ ref('flux_r151') }}

    union all

    -- R64 : relevés JSON timeseries (déjà dédoublonnés intra-source dans flux_r64).
    -- date_releve naïve → ancrage heure-mur Paris (convention début de journée native,
    -- pas de décalage).
    select
        releve_id,
        'flux_R64'                                                                  as source,
        pdl,
        timezone('Europe/Paris', date_releve)                                       as date_releve,
        false                                                                       as ordre_index,
        nature_index,
        id_releve,
        occurrence_id,
        cast(null as varchar)                                                       as ref_situation_contractuelle,
        cast(null as varchar)                                                       as formule_tarifaire_acheminement,
        cast(null as varchar)                                                       as id_calendrier_distributeur,
        index_base_kwh, index_hp_kwh, index_hc_kwh,
        index_hph_kwh, index_hpb_kwh, index_hch_kwh, index_hcb_kwh
    from {{ ref('flux_r64') }}
)

select *
from unifies
-- Dedup même-source (re-livraison) : 1 ligne par relevé logique = 1 par releve_id
-- (clé métier ADR-0028, encode source|pdl|date|discriminant). On garde la livraison
-- la plus récente ; occurrence_id (fichier#position / mesure_id) départage de façon
-- déterministe. R64 est déjà unique en amont (qualify dans flux_r64).
qualify row_number() over (partition by releve_id order by occurrence_id desc) = 1
