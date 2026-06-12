-- Linéarisation F12 : une ligne par Element_Valorise (détail valorisé distributeur).
--
-- Éclatement Sous_Lot → Element_Valorise. Les champs du Sous_Lot parent (PDL, tarif,
-- puissance pondérée…) sont en scope après unnest. Les valeurs d'acheminement vivent
-- sous Element_Valorise.Acheminement[0]. taux_tva reste VARCHAR (« 20.000 », code) ;
-- les quantités/prix/montants sont typés.

with sl as (
    select flux, num_facture, date_facture, s.v as sl
    from {{ ref('stg_f12') }},
        unnest(cast(content -> '$.Sous_Lot' as json[])) as s(v)
),

ev as (
    select flux, num_facture, date_facture, sl, e.v as ev
    from sl,
        unnest(cast(sl -> '$.Element_Valorise' as json[])) as e(v)
)

select
    sl ->> '$.Num_Sous_Lot'                     as num_sous_lot,
    sl ->> '$.Type_Facturation'                 as type_facturation,
    sl ->> '$.Motif_Rectif'                     as motif_rectif,
    sl ->> '$.Id_PRM'                           as pdl,
    sl ->> '$.Type_PRM'                         as type_prm,
    sl ->> '$.Code_Segmentation_ERDF'           as code_segmentation_erdf,
    cast(sl ->> '$.Puissance_Ponderee' as double) as puissance_ponderee_kva,
    sl ->> '$.Autoproducteur'                   as autoproducteur,
    sl ->> '$.Tarif_Souscrit'                   as tarif_souscrit,
    ev ->> '$.Id_EV'                            as id_ev,
    ev ->> '$.Libelle_EV'                       as libelle_ev,
    ev ->> '$.Code_Recapitulatif'               as code_recapitulatif,
    ev ->> '$.CSPE_Applicable'                  as cspe_applicable,
    ev ->> '$.Taux_TVA_Applicable'              as taux_tva_applicable,
    ev ->> '$.Code_Debit_Credit'                as code_debit_credit,
    cast(ev ->> '$.Acheminement[0].Date_Debut' as date) as date_debut,
    cast(ev ->> '$.Acheminement[0].Date_Fin' as date)   as date_fin,
    ev ->> '$.Acheminement[0].Unite_Quantite'  as unite,
    cast(ev ->> '$.Acheminement[0].Quantite' as double)      as quantite,
    cast(ev ->> '$.Acheminement[0].Prix_Unitaire' as double) as prix_unitaire,
    cast(ev ->> '$.Acheminement[0].Montant_HT' as double)    as montant_ht,
    flux,
    num_facture,
    date_facture
from ev
