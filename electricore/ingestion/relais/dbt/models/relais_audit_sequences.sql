{{ config(materialized='view') }}

-- Vue d'audit du journal du relais (#646, PRD #644) : étage TRANSFORM du relais (ELT).
-- Réutilise SANS AUCUNE règle dupliquée la macro `audit_sequences` du projet dbt
-- principal (#645), importée en PACKAGE LOCAL (packages.yml -> local: ../../dbt) — le seul
-- lieu qui connaît la nomenclature des noms de zip reste `electricore_flux/macros/audit_sequences.sql`.
--
-- Le journal du relais n'a PAS de table brute par flux (contrairement à l'ingestion, dont
-- le mart union raw_c15/raw_r15/…) : une seule table `relais_livraisons`, tous flux
-- mélangés. Le code flux est donc dérivé du nom de zip lui-même (2e segment `_`-délimité,
-- convention Enedis stable `<emetteur>_<FLUX>_...`) plutôt que porté par la table source —
-- ce qui a un bénéfice : un flux non encore couvert côté ingestion (R17) apparaît quand
-- même dans cette vue dès qu'il transite par le relais (au pire sous `nom_non_reconnu` si
-- sa nomenclature n'est pas encore dans la macro).
--
-- TOUS les statuts sont inclus (vu/pousse/amorce/echec, pas de filtre) : un zip REÇU
-- (même en échec de push, même seulement vu) prouve qu'Enedis a bien émis ce numéro de
-- séquence — l'exclure créerait de faux trous. `zips_non_relayes()` (côté Python), lui,
-- filtre sur pousse/amorce : question différente (relayé ou pas), pas celle-ci (reçu ou pas).
--
-- Vue PASSIVE (#646) : aucun data test `aucune_anomalie` ici — la supervision du relais
-- reste `StatsRelais`/exit code (#643) ; cette vue sert la consultation à la demande
-- (rapprochement Haulogy).

with journal as (
    select
        split_part("zip", '_', 2) as flux,
        "zip"                     as nom_zip
    from {{ source('journal', 'relais_livraisons') }}
),

-- La macro s'expanse elle-même en un bloc `with ... select ...` complet (cf.
-- macros/audit_sequences.sql du projet principal) : elle doit être le CORPS d'une CTE,
-- pas enchaînée après une autre (deux `with` de suite = SQL invalide).
audit as (
    {{ electricore_flux.audit_sequences('journal', 'nom_zip') }}
)

select * from audit
