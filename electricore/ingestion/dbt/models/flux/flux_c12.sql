-- Linéarisation C12 : une ligne par PRM, spine contractuelle C4 avec puissances
-- TURPE pivotées en colonnes (ADR-0051, issue #344).
--
-- Grain : une ligne par PRM (prm_id = fichier + position). Un fichier C12 porte en
-- général plusieurs PRM ; on prend Type_Evenement[0].Situation_Contractuelle[0] pour
-- chaque PRM (grain « dernière situation contractuelle du dernier événement »).
--
-- Pas de Segment_Clientele natif dans le XSD C12 : le segment (C2/C3/C4) est inféré
-- en aval depuis domaine_de_tension et option_tarifaire_turpe.
--
-- Pivot puissances (à confirmer sur golden réel prod, #344) :
--   HPE→hpb (Heures Pleines Été = Basse saison), HCE→hcb (Heures Creuses Été = Basse saison)
--   Classes HTA (Pointe, HPDemiSaison, HCDemiSaison, JA, PM) → NULL (non mappées, gardées)

with flat as (
    select
        prm_id,
        prm ->> '$.Identifiant'                                                                       as pdl,
        cast(prm ->> '$.Type_Evenement[0].Date_Evenement' as date)                                   as date_evenement,
        cast(prm ->> '$.Type_Evenement[0].Situation_Contractuelle[0].Date_Debut' as date)             as date_debut,
        cast(prm ->> '$.Type_Evenement[0].Situation_Contractuelle[0].Date_Fin' as date)               as date_fin,
        cast(
            prm ->> '$.Type_Evenement[0].Situation_Contractuelle[0].Synthese_Contractuelle[0].Date_De_Premiere_Mise_En_Service'
            as date
        )                                                                                              as date_premiere_mise_en_service,
        -- Événement : nature (liste) → première valeur (pattern C15-compatible)
        prm ->> '$.Type_Evenement[0].Situation_Contractuelle[0].Evenement[0].Nature_Evenement[0]'    as nature_evenement,
        prm ->> '$.Type_Evenement[0].Situation_Contractuelle[0].Evenement[0].Numero_Affaire'         as numero_affaire,
        -- Option tarifaire TURPE
        prm ->> '$.Type_Evenement[0].Situation_Contractuelle[0].Option_Tarifaire_TURPE_Souscrite[0].Option_Tarifaire_TURPE[0].Libelle'
                                                                                                      as option_tarifaire_turpe,
        -- Profil
        prm ->> '$.Type_Evenement[0].Situation_Contractuelle[0].Profil'                              as profil,
        -- Domaine de tension (depuis Alimentation_Principale, optionnel)
        prm ->> '$.Type_Evenement[0].Alimentation[0].Alimentation_Principale[0].Domaine_De_Tension'  as domaine_de_tension,
        -- Utilisateur réseau (Personne_Morale — proxy nature économique pour l'accise #226)
        prm ->> '$.Type_Evenement[0].Situation_Contractuelle[0].Utilisateur_Reseau[0].Personne_Morale[0].Raison_Sociale'
                                                                                                      as raison_sociale,
        prm ->> '$.Type_Evenement[0].Situation_Contractuelle[0].Utilisateur_Reseau[0].Personne_Morale[0].Code_APE'
                                                                                                      as code_ape,
        -- Classes temporelles TURPE souscrites (liste pour le pivot aval)
        cast(
            prm -> '$.Type_Evenement[0].Situation_Contractuelle[0].Option_Tarifaire_TURPE_Souscrite[0].Classe_Temporelle_TURPE_Souscrite'
            as json[]
        )                                                                                              as classes
    from {{ ref('stg_c12') }}
),

-- Unnest des classes temporelles : une ligne par (prm_id, classe) pour le pivot.
-- CTE isolé avant le WHERE (discipline anti-pushdown-sous-unnest, comme flux_c15).
classes as (
    select
        prm_id,
        t.classe ->> '$.Libelle'             as libelle,
        cast(t.classe ->> '$.Puissance_Souscrite' as bigint) as puissance_kva
    from flat,
        unnest(classes) as t(classe)
),

-- Pivot des puissances par libelle TURPE → colonnes standard cadrans.
-- HPE→hpb / HCE→hcb : convention Basse saison (ADR-0035 §1).
-- Classes HTA (Pointe, HPDemiSaison, HCDemiSaison, JA, PM) : NULL délibéré, gardées.
puissances as (
    select
        prm_id,
        max(case when libelle = 'Base' then puissance_kva end) as puissance_souscrite_kva,
        max(case when libelle = 'HP'   then puissance_kva end) as puissance_souscrite_hp_kva,
        max(case when libelle = 'HC'   then puissance_kva end) as puissance_souscrite_hc_kva,
        max(case when libelle = 'HPH'  then puissance_kva end) as puissance_souscrite_hph_kva,
        max(case when libelle = 'HCH'  then puissance_kva end) as puissance_souscrite_hch_kva,
        max(case when libelle = 'HPE'  then puissance_kva end) as puissance_souscrite_hpb_kva,
        max(case when libelle = 'HCE'  then puissance_kva end) as puissance_souscrite_hcb_kva
    from classes
    group by prm_id
)

select
    * exclude (prm_id, classes),
    'flux_C12' as source
from flat left join puissances using (prm_id)
