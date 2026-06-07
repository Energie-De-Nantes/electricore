"""Service de sérialisation des taxes énergétiques (CTA).

Volet Accise migré : les endpoints `/taxes/accise/*` consomment désormais
directement `feuilles_rapport_accise` + `accise_par_contrat` côté
`integrations.odoo.taxes`, sans couche service intermédiaire (issue #75).

Le volet CTA reste ici tant qu'il n'a pas été migré au même pattern.
"""

import polars as pl

from electricore.api.serializers import arrow_stream, xlsx_multi_sheet
from electricore.integrations.odoo.decorators import with_odoo
from electricore.integrations.odoo.taxes import (
    cta_par_contrat,
    rapport_cta,
)

# =============================================================================
# CTA — détail brut + rapport agrégé
# =============================================================================


@with_odoo
def calculer_cta_detail(odoo, trimestre: str | None = None) -> pl.DataFrame:
    """Binding HTTP : délègue à `cta_par_contrat` avec OdooReader auto-géré."""
    return cta_par_contrat(odoo, trimestre)


def generer_cta_detail_arrow(trimestre: str | None = None) -> bytes:
    """Sérialise le détail CTA mensuel (PDL × mois) en flux Arrow IPC."""
    return arrow_stream(calculer_cta_detail(trimestre))


def generer_cta_detail_xlsx(trimestre: str | None = None) -> bytes:
    """Sérialise le détail CTA mensuel (PDL × mois) en XLSX mono-onglet."""
    return xlsx_multi_sheet({"Détail": calculer_cta_detail(trimestre)})


@with_odoo
def _cta_rapport(odoo, trimestre: str | None = None):
    return rapport_cta(odoo, trimestre)


def generer_cta_rapport_xlsx(trimestre: str | None = None) -> bytes:
    """Livrable facturiste : 3 onglets (Résumé / Par taux / Détail) depuis `rapport_cta`.

    Le décret CRE peut faire varier `taux_cta_pct` en cours de trimestre ;
    l'onglet `Par taux` reflète cette dimensionalité, et l'onglet `Détail`
    list les taux successifs par PDL en string joined ` ; `.
    """
    r = _cta_rapport(trimestre)
    return xlsx_multi_sheet(
        {
            "Résumé": r.resume,
            "Par taux": r.par_taux,
            "Détail": r.detail,
        }
    )
