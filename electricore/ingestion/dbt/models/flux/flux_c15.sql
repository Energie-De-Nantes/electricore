-- Linéarisation C15 : une ligne par PRM, situation contractuelle + relevés
-- avant/après pivotés en colonnes (ADR-0020, issue #124 — le flux le plus dur).
--
-- Remplace le moteur Python `parser_flux_xml` piloté par `flux.yaml` : la sélection
-- (data_fields) devient des chemins JSON, l'axe parent XPath `../Code_Qualification`
-- du DSL disparaît (après unnest, les champs du Donnees_Releve parent sont en scope,
-- la condition devient un WHERE), le pivot des cadrans devient une agrégation
-- conditionnelle sur le domaine fermé des cadrans (core/models/cadrans.py).
--
-- Accès JSON tolérant (pas de cast struct géant) car le document C15 est profond.
-- `xml_vers_dict` rend tout conteneur tableau (« conteneur = liste ») → chaque
-- élément unique est indexé `[0]`.

-- Grain : une ligne par *occurrence de PRM* (prm_id = fichier + position), pas par
-- PDL. Un même PDL revient dans plusieurs fichiers C15 (un par événement) ; agréger
-- les relevés par PDL collerait le relevé le plus récent à tous ses événements. On
-- scope donc l'agrégation des relevés sur prm_id, porté par le staging.

with flat as (
    select
        prm_id,
        prm ->> '$.Id_PRM'                                                    as pdl,
        prm ->> '$.Segment_Clientele'                                         as segment_clientele,
        prm ->> '$.Num_Depannage'                                             as num_depannage,
        prm ->> '$.Situation_Contractuelle[0].Titulaire_Contrat[0].Categorie' as categorie,
        prm ->> '$.Situation_Contractuelle[0].Etat_Contractuel'              as etat_contractuel,
        prm ->> '$.Situation_Contractuelle[0].Ref_Situation_Contractuelle'   as ref_situation_contractuelle,
        cast(prm ->> '$.Situation_Contractuelle[0].Structure_Tarifaire[0].Puissance_Souscrite' as double) as puissance_souscrite_kva,
        prm ->> '$.Situation_Contractuelle[0].Structure_Tarifaire[0].Formule_Tarifaire_Acheminement' as formule_tarifaire_acheminement,
        prm ->> '$.Dispositif_De_Comptage[0].Compteur[0].Type'              as type_compteur,
        prm ->> '$.Dispositif_De_Comptage[0].Compteur[0].Num_Serie'         as num_compteur,
        cast(prm ->> '$.Date_Derniere_Modification_FTA' as date)            as date_derniere_modification_fta,
        prm ->> '$.Evenement_Declencheur[0].Nature_Evenement'              as evenement_declencheur,
        prm ->> '$.Evenement_Declencheur[0].Type_Evenement'                as type_evenement,
        cast(prm ->> '$.Evenement_Declencheur[0].Date_Evenement' as timestamptz) as date_evenement,
        prm ->> '$.Evenement_Declencheur[0].Ref_Demandeur'                 as ref_demandeur,
        prm ->> '$.Evenement_Declencheur[0].Id_Affaire'                    as id_affaire
    from {{ ref('stg_c15') }}
),

-- Unnest des relevés en deux étages, chacun dans son CTE : isole le double unnest
-- latéral du WHERE aval. Sans cette césure, l'optimiseur DuckDB pousse le prédicat
-- JSON sous l'unnest et le caste contre la mauvaise valeur (objet pré-unnest).
donnees as (
    select
        prm_id,
        t1.donnee
    from {{ ref('stg_c15') }},
        unnest(cast(prm -> '$.Evenement_Declencheur[0].Releves[0].Donnees_Releve' as json[])) as t1(donnee)
),

classes as (
    select
        prm_id,
        donnee,
        t2.classe
    from donnees,
        unnest(cast(donnee -> '$.Classe_Temporelle_Distributeur' as json[])) as t2(classe)
),

-- Extraction en colonnes VARCHAR nommées AVANT tout filtre/cast : le prédicat ne
-- peut plus se faufiler dans l'unnest.
extrait as (
    select
        prm_id,
        donnee ->> '$.Code_Qualification'          as code_qualif,
        classe ->> '$.Classe_Mesure'               as classe_mesure,
        classe ->> '$.Sens_Mesure'                 as sens_mesure,
        lower(classe ->> '$.Id_Classe_Temporelle') as cadran,
        classe ->> '$.Valeur'                      as valeur,
        donnee ->> '$.Date_Releve'                 as date_releve,
        donnee ->> '$.Nature_Index'                as nature_index,
        donnee ->> '$.Id_Calendrier_Distributeur'  as id_calendrier_distributeur,
        donnee ->> '$.Id_Calendrier'               as id_calendrier_fournisseur
    from classes
),

releves as (
    select
        prm_id,
        case when code_qualif = '1' then 'avant_' else 'apres_' end as prefixe,
        cadran,
        cast(valeur as bigint)           as valeur,
        cast(date_releve as timestamptz) as date_releve,
        nature_index,
        id_calendrier_distributeur,
        id_calendrier_fournisseur
    -- Code_Qualification : 1 = relevé avant événement, 2 = après. Classe_Mesure = 1
    -- (énergie) et Sens_Mesure = 0 (soutirage) écartent injection et autres mesures.
    from extrait
    where code_qualif in ('1', '2')
      and classe_mesure = '1'
      and sens_mesure = '0'
),

releves_agg as (
    select
        prm_id,
        {{ pivot_cadrans(condition_prefixe="prefixe = 'avant_' and ", alias_prefixe="avant_") }},
        {{ pivot_cadrans(condition_prefixe="prefixe = 'apres_' and ", alias_prefixe="apres_") }},
        max(case when prefixe = 'avant_' then date_releve end)                as avant_date_releve,
        max(case when prefixe = 'avant_' then nature_index end)               as avant_nature_index,
        max(case when prefixe = 'avant_' then id_calendrier_distributeur end) as avant_id_calendrier_distributeur,
        max(case when prefixe = 'avant_' then id_calendrier_fournisseur end)  as avant_id_calendrier_fournisseur,
        max(case when prefixe = 'apres_' then date_releve end)                as apres_date_releve,
        max(case when prefixe = 'apres_' then nature_index end)               as apres_nature_index,
        max(case when prefixe = 'apres_' then id_calendrier_distributeur end) as apres_id_calendrier_distributeur,
        max(case when prefixe = 'apres_' then id_calendrier_fournisseur end)  as apres_id_calendrier_fournisseur
    from releves
    group by prm_id
)

select * exclude (prm_id) from flat left join releves_agg using (prm_id)
