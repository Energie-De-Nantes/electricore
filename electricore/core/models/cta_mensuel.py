"""Schéma Pandera Polars pour la CTA au grain mensuel (situation contractuelle × mois).

Sortie de `pipeline_cta` (issue #116) : la facturation mensuelle (`PeriodeMeta`)
enrichie de `order_name`, `taux_cta_pct`, `cta_eur` et `trimestre`.

Grain hérité de `pipeline_facturation` : une ligne par **(RSC, mois)**, pas par
PDL — un PDL qui change de situation contractuelle en cours de mois porte deux
lignes ce mois-là (le TURPE fixe est facturé par situation, cf. CONTEXT.md,
entrée *Méta-période mensuelle*). `Config.unique` garantit ce grain à la
matérialisation et détecte au passage tout fan-out du join `pdl_mapping`.

`turpe_fixe_eur`, `taux_cta_pct` et `cta_eur` restent nullables : les
méta-périodes incomplètes (`data_complete=False`) peuvent ne pas porter de
TURPE fixe, et le montant CTA est alors null — comportement préservé.
"""

import pandera.polars as pa
import polars as pl


class CtaMensuel(pa.DataFrameModel):
    """CTA au grain mensuel : une ligne par (situation contractuelle, mois)."""

    ref_situation_contractuelle: pl.Utf8 = pa.Field(nullable=False)
    pdl: pl.Utf8 = pa.Field(nullable=False)
    mois_annee: pl.Utf8 = pa.Field(nullable=False, str_matches=r"^\d{4}-\d{2}$")  # ex: "2025-03"
    order_name: pl.Utf8 = pa.Field(nullable=False)
    turpe_fixe_eur: pl.Float64 | None = pa.Field(nullable=True, ge=0.0)
    taux_cta_pct: pl.Float64 | None = pa.Field(nullable=True, ge=0.0)
    cta_eur: pl.Float64 | None = pa.Field(nullable=True, ge=0.0)
    trimestre: pl.Utf8 = pa.Field(nullable=False)  # ex: "2025-T1"

    class Config:
        strict = False
        coerce = True
        unique = ["ref_situation_contractuelle", "mois_annee"]
