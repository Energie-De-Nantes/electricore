"""Schémas Pandera pour le livrable `rapport_accise` — tests de contrat (ADR-0019).

Les tests comportementaux (`rapport_accise`, `accise_par_contrat`) vivent
désormais dans `tests/unit/test_rapport_taxe.py` (build pur `core/builds/`).
"""

import polars as pl


class TestRapportAcciseSchemas:
    """Les 3 `DataFrameModel` Pandera valident bien des frames conformes."""

    def test_resume_schema_validates_correct_frame(self):
        from electricore.integrations.odoo.models.rapport_accise import RapportAcciseResume

        df = pl.DataFrame(
            {
                "trimestre": ["2025-T1"],
                "nb_pdl": [2],
                "energie_mwh_total": [10.0],
                "accise_eur_total": [225.0],
            }
        )
        RapportAcciseResume.validate(df)

    def test_par_taux_schema_validates_correct_frame(self):
        from electricore.integrations.odoo.models.rapport_accise import RapportAcciseParTaux

        df = pl.DataFrame(
            {
                "taux_accise_eur_mwh": [22.5],
                "energie_mwh": [10.0],
                "accise_eur": [225.0],
                "nb_pdl": [2],
            }
        )
        RapportAcciseParTaux.validate(df)

    def test_detail_schema_validates_correct_frame(self):
        from electricore.integrations.odoo.models.rapport_accise import RapportAcciseDetail

        df = pl.DataFrame(
            {
                "pdl": ["A"],
                "mois_consommation": ["2025-01"],
                "trimestre": ["2025-T1"],
                "taux_accise_eur_mwh": [22.5],
                "energie_mwh": [1.0],
                "accise_eur": [22.5],
            }
        )
        RapportAcciseDetail.validate(df)
