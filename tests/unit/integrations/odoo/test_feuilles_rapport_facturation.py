"""Tests pour `feuilles_rapport_facturation` (issue #77).

Transforme un `RapportFacturation` en `dict[str, DataFrame]` consommable par
`xlsx_multi_sheet`. Fonction pure — pas de dépendance Odoo.
"""

import polars as pl
from polars.testing import assert_frame_equal


def _rapport_synthetique():
    """`RapportFacturation` minimal pour tester la sérialisation en feuilles."""
    from electricore.integrations.odoo.facturation import RapportFacturation

    lignes = pl.DataFrame(
        {
            "pdl": ["12345678901234", "98765432109876"],
            "x_pdl": ["12345678901234", "98765432109876"],
            "memo_puissance": ["", "passage 6→9 kVA"],
            "a_facturer": [True, True],
            "a_supprimer": [False, False],
        }
    )
    return RapportFacturation(
        resume=pl.DataFrame(
            {
                "mois": ["2025-03-01"],
                "nb_pdl": [2],
                "total_a_facturer": [2],
                "total_a_supprimer": [0],
            }
        ),
        lignes=lignes,
        changements_puissance=lignes.filter(pl.col("memo_puissance") != ""),
    )


def test_feuilles_rapport_facturation_returns_three_keyed_dict():
    """La fonction retourne un dict avec les 3 onglets standards du livrable."""
    from electricore.integrations.odoo.facturation import feuilles_rapport_facturation

    rapport = _rapport_synthetique()
    feuilles = feuilles_rapport_facturation(rapport)

    assert set(feuilles.keys()) == {"Résumé", "Lignes", "Changements puissance"}


def test_feuilles_rapport_facturation_maps_each_frame_to_its_sheet():
    """Chaque feuille pointe vers le DataFrame correspondant du rapport, sans transformation."""
    from electricore.integrations.odoo.facturation import feuilles_rapport_facturation

    rapport = _rapport_synthetique()
    feuilles = feuilles_rapport_facturation(rapport)

    assert_frame_equal(feuilles["Résumé"], rapport.resume)
    assert_frame_equal(feuilles["Lignes"], rapport.lignes)
    assert_frame_equal(feuilles["Changements puissance"], rapport.changements_puissance)
