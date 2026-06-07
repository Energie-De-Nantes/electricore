"""Tests pour `feuilles_rapport_cta` (issue #77).

Transforme un `RapportCta` en `dict[str, DataFrame]` consommable par
`xlsx_multi_sheet`. Fonction pure — pas de dépendance Odoo.
"""

import polars as pl
from polars.testing import assert_frame_equal


def _rapport_synthetique():
    """`RapportCta` minimal pour tester la sérialisation en feuilles."""
    from electricore.integrations.odoo.taxes import RapportCta

    return RapportCta(
        resume=pl.DataFrame(
            {
                "trimestre": ["2025-T1"],
                "nb_pdl": [1],
                "turpe_fixe_total_eur": [42.50],
                "cta_total_eur": [9.18],
            }
        ),
        par_taux=pl.DataFrame(
            {
                "trimestre": ["2025-T1"],
                "taux_cta_pct": [21.61],
                "nb_pdl": [1],
                "turpe_fixe_eur": [42.50],
                "cta_eur": [9.18],
            }
        ),
        detail=pl.DataFrame(
            {
                "pdl": ["12345678901234"],
                "order_name": ["SO/2025/0001"],
                "turpe_fixe_total_eur": [42.50],
                "cta_total_eur": [9.18],
                "taux_cta_appliques": ["21.61"],
            }
        ),
    )


def test_feuilles_rapport_cta_returns_three_keyed_dict():
    """La fonction retourne un dict avec les 3 onglets standards du livrable."""
    from electricore.integrations.odoo.taxes import feuilles_rapport_cta

    rapport = _rapport_synthetique()
    feuilles = feuilles_rapport_cta(rapport)

    assert set(feuilles.keys()) == {"Résumé", "Par taux", "Détail"}


def test_feuilles_rapport_cta_maps_each_frame_to_its_sheet():
    """Chaque feuille pointe vers le DataFrame correspondant du rapport, sans transformation."""
    from electricore.integrations.odoo.taxes import feuilles_rapport_cta

    rapport = _rapport_synthetique()
    feuilles = feuilles_rapport_cta(rapport)

    assert_frame_equal(feuilles["Résumé"], rapport.resume)
    assert_frame_equal(feuilles["Par taux"], rapport.par_taux)
    assert_frame_equal(feuilles["Détail"], rapport.detail)
