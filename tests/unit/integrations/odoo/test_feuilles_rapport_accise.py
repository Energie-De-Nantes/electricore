"""Tests pour `feuilles_rapport_accise` (issue #75).

Transforme un `RapportAccise` en `dict[str, DataFrame]` consommable par
`xlsx_multi_sheet`. Fonction pure — pas de dépendance Odoo.
"""

import polars as pl
from polars.testing import assert_frame_equal


def _rapport_synthetique():
    """`RapportAccise` minimal pour tester la sérialisation en feuilles."""
    from electricore.integrations.odoo.taxes import RapportAccise

    return RapportAccise(
        resume=pl.DataFrame(
            {"trimestre": ["2025-T1"], "nb_pdl": [2], "energie_mwh_total": [6.0], "accise_eur_total": [135.0]}
        ),
        par_taux=pl.DataFrame(
            {"taux_accise_eur_mwh": [22.5], "energie_mwh": [6.0], "accise_eur": [135.0], "nb_pdl": [2]}
        ),
        detail=pl.DataFrame(
            {
                "pdl": ["A", "B"],
                "mois_consommation": ["2025-01", "2025-01"],
                "trimestre": ["2025-T1", "2025-T1"],
                "taux_accise_eur_mwh": [22.5, 22.5],
                "energie_mwh": [3.0, 3.0],
                "accise_eur": [67.5, 67.5],
            }
        ),
    )


def test_feuilles_rapport_accise_returns_three_keyed_dict():
    """La fonction retourne un dict avec les 3 onglets standards du livrable."""
    from electricore.integrations.odoo.taxes import feuilles_rapport_accise

    rapport = _rapport_synthetique()
    feuilles = feuilles_rapport_accise(rapport)

    assert set(feuilles.keys()) == {"Résumé", "Par taux", "Détail"}


def test_feuilles_rapport_accise_maps_each_frame_to_its_sheet():
    """Chaque feuille pointe vers le DataFrame correspondant du rapport, sans transformation."""
    from electricore.integrations.odoo.taxes import feuilles_rapport_accise

    rapport = _rapport_synthetique()
    feuilles = feuilles_rapport_accise(rapport)

    assert_frame_equal(feuilles["Résumé"], rapport.resume)
    assert_frame_equal(feuilles["Par taux"], rapport.par_taux)
    assert_frame_equal(feuilles["Détail"], rapport.detail)
