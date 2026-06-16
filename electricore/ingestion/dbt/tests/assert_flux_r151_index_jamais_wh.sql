-- Garde-fou ADR-0034 (#285) : flux_r151 ne doit JAMAIS émettre d'index labellisé Wh.
-- La régression qui a corrompu les relevés (Wh stockés dans des colonnes index_*_kwh)
-- repasserait silencieusement si la normalisation Wh→kWh au boundary disparaissait ;
-- ce test la rend bruyante. La valeur synthétique 'TEXTE' (fixture XSD, Unite_Mesure_Index
-- sans énumération) est tolérée — seul 'Wh' est interdit. Le test échoue s'il renvoie des lignes.
select unite
from {{ ref('flux_r151') }}
where unite = 'Wh'
