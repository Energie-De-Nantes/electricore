-- Linéarisation F15 : une ligne par Element_Valorise (détail de facture valorisé).
--
-- Pas de pivot : éclatement à trois niveaux Donnees_Valorisation → Groupe_Valorise →
-- Element_Valorise, chaque niveau projetant ses scalaires avant de descendre.
-- taux_tva reste VARCHAR (vaut « NS » = non soumis, pas un nombre) ; les montants
-- signés sont typés.
--
-- Mémoire : unnest en SELECT, pas en FROM — un unnest latéral sur content produit une
-- LEFT_DELIM_JOIN qui annule les partitions de stg_f15 (docs/ingestion.md, piège n° 4).

with dv as (
    select
        flux, num_facture, date_facture,
        unnest(cast(content -> '$.Donnees_Valorisation' as json[])) as dv
    from {{ ref('stg_f15') }}
),

dv_scal as (
    select
        flux, num_facture, date_facture,
        dv ->> '$.Type_Facturation'                           as type_facturation,
        dv ->> '$.Donnees_PRM[0].Id_PRM'                      as pdl,
        dv ->> '$.Donnees_PRM[0].Ref_Situation_Contractuelle' as ref_situation_contractuelle,
        dv ->> '$.Donnees_PRM[0].Type_Compteur'               as type_compteur,
        unnest(cast(dv -> '$.Groupe_Valorise' as json[]))     as gv
    from dv
),

gv_scal as (
    select
        flux, num_facture, date_facture,
        type_facturation, pdl, ref_situation_contractuelle, type_compteur,
        gv ->> '$.Nature_EV'                                  as nature_ev,
        unnest(cast(gv -> '$.Element_Valorise' as json[]))    as ev
    from dv_scal
)

select
    type_facturation,
    pdl,
    ref_situation_contractuelle,
    type_compteur,
    ev ->> '$.Id_EV'                                  as id_ev,
    nature_ev,
    ev ->> '$.Taux_TVA_Applicable'                    as taux_tva_applicable,
    ev ->> '$.Formule_Tarifaire_Acheminement'        as formule_tarifaire_acheminement,
    ev ->> '$.Unite_Quantite'                        as unite,
    cast(ev ->> '$.Prix_Unitaire' as double)         as prix_unitaire,
    cast(ev ->> '$.Quantite' as double)              as quantite,
    cast(ev ->> '$.Montant_HT' as double)            as montant_ht,
    cast(ev ->> '$.Date_Debut' as date)              as date_debut,
    cast(ev ->> '$.Date_Fin' as date)                as date_fin,
    ev ->> '$.Libelle_EV'                            as libelle_ev,
    flux,
    num_facture,
    -- date_facture / date_debut / date_fin sont des JOURS CIVILS (DATE), pas d'ancrage en
    -- instant (ADR-0042, #396) : le loader f15() devient un SELECT * et les sert en DATE.
    date_facture,
    -- Source résiduelle descendue du loader (ADR-0042, #396) : `f15()` devient un SELECT *.
    'flux_F15' as source
from gv_scal
