-- Éclatement R67 (« mesures facturantes », ADR-0047) : une ligne par mesure (PDL),
-- métadonnées d'en-tête à plat. Même plomberie de landing que R64 (stg_r64), mais la
-- feuille JSON porte de l'ÉNERGIE par période (quantite/dbtMesure/finMesure), pas un
-- index cumulé à un instant.
--
-- Accès JSON tolérant (->>), PAS de cast struct (cf. stg_r64) : on n'impose aucune forme
-- aux sous-objets, on cast en json[] uniquement les tableaux qu'on déplie en aval.
-- mesure_id (fichier + position) scope l'agrégation aval.

select
    file_name,
    modification_date,
    id_demande,
    si_demandeur,
    code_flux,
    format,
    file_name || '#' || generate_subscripts(mesures, 1) as mesure_id,
    unnest(mesures)                                      as mesure
from (
    select
        file_name,
        modification_date,
        content ->> '$.header.idDemande'     as id_demande,
        content ->> '$.header.siDemandeur'   as si_demandeur,
        content ->> '$.header.codeFlux'      as code_flux,
        content ->> '$.header.format'        as format,
        cast(content -> '$.mesures' as json[]) as mesures
    from {{ source('flux_raw', 'raw_r67') }}
)
