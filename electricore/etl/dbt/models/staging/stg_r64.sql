-- Éclatement R64 : une ligne par mesure (PDL), métadonnées d'en-tête à plat.
--
-- Accès JSON tolérant (->>), PAS de cast struct : le corpus réel porte 4 formes
-- différentes de classeTemporelle (avec/sans codeCadran, avec/sans idClasseTemporelle)
-- et le cast struct strict de DuckDB casse sur les clés absentes — c'est la limite
-- « inférence sur échantillon » annoncée par l'ADR-0020, rencontrée au premier run
-- complet réel. mesure_id (fichier + position) scope l'agrégation aval.

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
    from {{ source('flux_raw', 'raw_r64') }}
)
