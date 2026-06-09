"""Tests pour `feuilles_rapport_taxe` côté CTA (ADR-0019, issue #108).

Symétrique de `test_feuilles_rapport_accise.py` — les deux livrent les mêmes
3 onglets FR via `feuilles_rapport_taxe`.
"""

import polars as pl
from polars.testing import assert_frame_equal


def _rapport_synthetique():
    from electricore.core.builds.rapport_taxe import RapportTaxe

    return RapportTaxe(
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


def test_feuilles_rapport_taxe_returns_three_keyed_dict():
    from electricore.core.builds.rapport_taxe import feuilles_rapport_taxe

    rapport = _rapport_synthetique()
    feuilles = feuilles_rapport_taxe(rapport)

    assert set(feuilles.keys()) == {"Résumé", "Par taux", "Détail"}


def test_feuilles_rapport_taxe_maps_each_frame_to_its_sheet():
    from electricore.core.builds.rapport_taxe import feuilles_rapport_taxe

    rapport = _rapport_synthetique()
    feuilles = feuilles_rapport_taxe(rapport)

    assert_frame_equal(feuilles["Résumé"], rapport.resume)
    assert_frame_equal(feuilles["Par taux"], rapport.par_taux)
    assert_frame_equal(feuilles["Détail"], rapport.detail)
