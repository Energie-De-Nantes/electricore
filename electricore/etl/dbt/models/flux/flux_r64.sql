-- Linéarisation R64 : une ligne par (PDL, date de relevé), cadrans en colonnes.
--
-- Remplace les ~250 lignes Python de `etl/parsing/r64.py` (cf. ADR-0020). La
-- sélection « calendrier distributeur uniquement » (is_valid_calendrier) devient
-- un WHERE ; le pivot wide « 1 mesure × N classes × N dates » devient un PIVOT.

with points as (
    select
        m.unnest."idPrm"                                                   as pdl,
        ctx.unnest."etapeMetier"                                           as etape_metier,
        ctx.unnest."contexteReleve"                                        as contexte_releve,
        ctx.unnest."typeReleve"                                            as type_releve,
        g.unnest."grandeurPhysique"                                        as grandeur_physique,
        g.unnest."grandeurMetier"                                          as grandeur_metier,
        g.unnest.unite                                                     as unite,
        s.header."idDemande"                                               as id_demande,
        s.header."siDemandeur"                                             as si_demandeur,
        s.header."codeFlux"                                                as code_flux,
        s.header.format                                                    as format,
        'index_' || lower(ct.unnest."idClasseTemporelle") || '_kwh'        as cadran_col,
        v.unnest.d                                                         as date_releve,
        v.unnest.v                                                         as valeur
    from {{ ref('stg_r64') }} s,
        unnest(s.mesures) as m,
        unnest(m.unnest.contexte) as ctx,
        unnest(ctx.unnest.grandeur) as g,
        unnest(g.unnest.calendrier) as cal,
        unnest(cal.unnest."classeTemporelle") as ct,
        unnest(ct.unnest.valeur) as v
    where g.unnest."grandeurMetier" = 'CONS'
      and g.unnest."grandeurPhysique" = 'EA'
      -- Seuls les calendriers distributeur entrent dans les tables (is_valid_calendrier).
      and (
          cal.unnest."idCalendrier" in ('DI000001', 'DI000002', 'DI000003')
          or lower(cal.unnest."libelleCalendrier") like '%distributeur%'
      )
      and v.unnest.iv = 0
      and v.unnest.d is not null
      and v.unnest.v is not null
)

pivot points
on cadran_col using first(valeur)
group by
    pdl, etape_metier, contexte_releve, type_releve, grandeur_physique,
    grandeur_metier, unite, id_demande, si_demandeur, code_flux, format, date_releve
