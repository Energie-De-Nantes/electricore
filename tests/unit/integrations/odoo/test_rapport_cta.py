"""Schémas Pandera pour le livrable `rapport_cta` — tests de contrat (ADR-0019).

Les tests comportementaux (`rapport_cta`, `cta_par_contrat`) vivent
désormais dans `tests/unit/test_rapport_taxe.py` (build pur `core/builds/`).
"""

import polars as pl


class TestRapportCtaSchemas:
    """Les 3 `DataFrameModel` Pandera valident bien des frames conformes."""

    def test_resume_schema_validates_correct_frame(self):
        from electricore.integrations.odoo.models.rapport_cta import RapportCtaResume

        df = pl.DataFrame(
            {
                "trimestre": ["2025-T1"],
                "nb_pdl": [3],
                "turpe_fixe_total_eur": [120.50],
                "cta_total_eur": [26.04],
            }
        )
        RapportCtaResume.validate(df)

    def test_par_taux_schema_validates_correct_frame(self):
        from electricore.integrations.odoo.models.rapport_cta import RapportCtaParTaux

        df = pl.DataFrame(
            {
                "trimestre": ["2025-T1"],
                "taux_cta_pct": [21.61],
                "nb_pdl": [3],
                "turpe_fixe_eur": [120.50],
                "cta_eur": [26.04],
            }
        )
        RapportCtaParTaux.validate(df)

    def test_detail_schema_validates_correct_frame(self):
        from electricore.integrations.odoo.models.rapport_cta import RapportCtaDetail

        df = pl.DataFrame(
            {
                "pdl": ["12345678901234"],
                "order_name": ["SO/2025/0001"],
                "turpe_fixe_total_eur": [42.50],
                "cta_total_eur": [9.18],
                "taux_cta_appliques": ["21.61"],
            }
        )
        RapportCtaDetail.validate(df)
