-- Linéarisation F15 : une ligne par Element_Valorise (détail de facture valorisé).
--
-- Pas de pivot : éclatement à trois niveaux Donnees_Valorisation → Groupe_Valorise →
-- Element_Valorise. Les champs des parents (PDL, type de facturation, nature EV) sont
-- en scope après unnest — l'axe parent ../../ du DSL legacy disparaît. taux_tva reste
-- VARCHAR (vaut « NS » = non soumis, pas un nombre) ; les montants signés sont typés.

with dv as (
    select flux, num_facture, date_facture, d.v as dv
    from {{ ref('stg_f15') }},
        unnest(cast(content -> '$.Donnees_Valorisation' as json[])) as d(v)
),

gv as (
    select flux, num_facture, date_facture, dv, g.v as gv
    from dv,
        unnest(cast(dv -> '$.Groupe_Valorise' as json[])) as g(v)
),

ev as (
    select flux, num_facture, date_facture, dv, gv, e.v as ev
    from gv,
        unnest(cast(gv -> '$.Element_Valorise' as json[])) as e(v)
)

select
    dv ->> '$.Type_Facturation'                       as type_facturation,
    dv ->> '$.Donnees_PRM[0].Id_PRM'                  as pdl,
    dv ->> '$.Donnees_PRM[0].Ref_Situation_Contractuelle' as ref_situation_contractuelle,
    dv ->> '$.Donnees_PRM[0].Type_Compteur'          as type_compteur,
    ev ->> '$.Id_EV'                                  as id_ev,
    gv ->> '$.Nature_EV'                              as nature_ev,
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
    date_facture
from ev
