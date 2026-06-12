-- Staging F12 : métadonnées de facture (niveau document) + contenu brut.
--
-- L'éclatement Sous_Lot → Element_Valorise vit dans flux_f12_detail (une ligne par
-- Element_Valorise). « conteneur = liste » → en-têtes indexés [0].

select
    file_name,
    modification_date,
    content ->> '$.En_Tete_Flux[0].Identifiant_Flux'    as flux,
    content ->> '$.Rappel_En_Tete[0].Num_Facture'       as num_facture,
    cast(content ->> '$.Rappel_En_Tete[0].Date_Facture' as date) as date_facture,
    content                                             as content
from {{ source('flux_raw', 'raw_f12') }}
