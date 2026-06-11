-- Cast du document JSON brut en structs typés.
--
-- Récupère l'ergonomie de read_json (accès `m.unnest.idPrm`) sur une *colonne*
-- JSON landée par dlt — un seul cast par flux, le modèle flux_ aval reste propre
-- (cf. ADR-0020, fork α). Le type est le schéma du flux R64 ; il est dérivable
-- du XSD R6X (Documents/guides_flux/Enedis.SGE.GUI.0503.Flux.R6X) — ici capturé
-- par inférence DuckDB sur un échantillon réel.

select
    file_name,
    modification_date,
    cast(content -> '$.header' as struct(
        "idDemande" varchar, "siDemandeur" varchar, "typeDestinataire" varchar,
        "idDestinataire" varchar, "codeFlux" varchar, "modePublication" varchar,
        "idCanalContact" varchar, format varchar, "publicationCrp" varchar
    )) as header,
    cast(content -> '$.mesures' as struct(
        "idPrm" varchar,
        periode struct("dateDebut" timestamp, "dateFin" timestamp),
        contexte struct(
            "etapeMetier" varchar, "contexteReleve" varchar, "typeReleve" varchar,
            grandeur struct(
                "grandeurMetier" varchar, "grandeurPhysique" varchar, unite varchar,
                calendrier struct(
                    "idCalendrier" varchar, "libelleCalendrier" varchar, "libelleGrille" varchar,
                    "classeTemporelle" struct(
                        "idClasseTemporelle" varchar, "libelleClasseTemporelle" varchar,
                        "codeCadran" varchar,
                        valeur struct(d timestamp, v bigint, iv bigint)[]
                    )[]
                )[],
                "cadranTotalisateur" struct(
                    "codeCadran" varchar, valeur struct(d timestamp, v bigint, iv bigint)[]
                )
            )[]
        )[]
    )[]) as mesures
from {{ source('flux_raw', 'raw_r64') }}
