-- Garde-fou ADR-0034 (#285) : flux_r64 ne doit JAMAIS émettre d'index labellisé Wh.
-- Pendant R64 de assert_flux_r151_index_jamais_wh : la normalisation Wh→kWh au boundary
-- (floor par index) doit tenir. 'TEXTE' synthétique toléré, 'Wh' interdit. Échoue si lignes.
select unite
from {{ ref('flux_r64') }}
where unite = 'Wh'
