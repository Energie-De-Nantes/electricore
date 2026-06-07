"""Service de sérialisation des taxes énergétiques (Accise TICFE, CTA).

Le calcul et l'orchestration métier vivent dans
`electricore.integrations.odoo.taxes`. Ce service ne fait que :

1. Ouvrir la connexion `OdooReader` (responsabilité HTTP)
2. Déléguer à l'orchestration (`accise_par_contrat`, `rapport_accise`,
   `cta_par_contrat`, `rapport_cta`)
3. Sérialiser le résultat (XLSX multi-onglets / Arrow IPC).

La shape des livrables (`Résumé` / `Par taux` / `Détail`) vit désormais dans
`integrations.odoo.taxes.rapport_accise` et `.rapport_cta` (cf. issues #56, #63).
"""

import polars as pl

from electricore.api.config import settings
from electricore.api.serializers import arrow_stream, xlsx_multi_sheet
from electricore.integrations.odoo import OdooReader
from electricore.integrations.odoo.taxes import (
    accise_par_contrat,
    cta_par_contrat,
    rapport_accise,
    rapport_cta,
)


def calculer_accise_detail(trimestre: str | None = None) -> pl.DataFrame:
    """Binding HTTP : ouvre Odoo + délègue à `accise_par_contrat`."""
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        return accise_par_contrat(odoo, trimestre)


def generer_accise_detail_arrow(trimestre: str | None = None) -> bytes:
    """Sérialise le détail accise (PDL × mois) en flux Arrow IPC."""
    return arrow_stream(calculer_accise_detail(trimestre))


def generer_accise_detail_xlsx(trimestre: str | None = None) -> bytes:
    """Sérialise le détail accise (PDL × mois) en XLSX mono-onglet."""
    return xlsx_multi_sheet({"Détail": calculer_accise_detail(trimestre)})


def generer_accise_rapport_xlsx(trimestre: str | None = None) -> bytes:
    """Livrable facturiste : 3 onglets (Résumé / Par taux / Détail) depuis `rapport_accise`."""
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        r = rapport_accise(odoo, trimestre)
    return xlsx_multi_sheet(
        {
            "Résumé": r.resume,
            "Par taux": r.par_taux,
            "Détail": r.detail.sort(["pdl", "mois_consommation"]),
        }
    )


def calculer_cta_detail(trimestre: str | None = None) -> pl.DataFrame:
    """Binding HTTP : ouvre Odoo + délègue à `cta_par_contrat`."""
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        return cta_par_contrat(odoo, trimestre)


def generer_cta_detail_arrow(trimestre: str | None = None) -> bytes:
    """Sérialise le détail CTA mensuel (PDL × mois) en flux Arrow IPC."""
    return arrow_stream(calculer_cta_detail(trimestre))


def generer_cta_detail_xlsx(trimestre: str | None = None) -> bytes:
    """Sérialise le détail CTA mensuel (PDL × mois) en XLSX mono-onglet."""
    return xlsx_multi_sheet({"Détail": calculer_cta_detail(trimestre)})


def generer_cta_rapport_xlsx(trimestre: str | None = None) -> bytes:
    """Livrable facturiste : 3 onglets (Résumé / Par taux / Détail) depuis `rapport_cta`.

    Le décret CRE peut faire varier `taux_cta_pct` en cours de trimestre ;
    l'onglet `Par taux` reflète cette dimensionalité, et l'onglet `Détail`
    list les taux successifs par PDL en string joined ` ; `.
    """
    with OdooReader(config=settings.get_odoo_config()) as odoo:
        r = rapport_cta(odoo, trimestre)
    return xlsx_multi_sheet(
        {
            "Résumé": r.resume,
            "Par taux": r.par_taux,
            "Détail": r.detail,
        }
    )
