"""Tests de contrat des schémas taxes (`core/models/`) — issue #116.

Couvre les schémas au grain mensuel (`AcciseMensuel`, `CtaMensuel`), dont la
garantie d'unicité du grain, et les onglets du livrable (`RapportAccise*`,
`RapportCta*`, rapatriés d'`integrations/odoo/models/`).

Les tests comportementaux des builds vivent dans `tests/unit/test_rapport_taxe.py`.
"""

import pandera.errors
import polars as pl
import pytest

# ---------------------------------------------------------------------------
# AcciseMensuel — contrat de pipeline_accise, grain (pdl, mois_annee)
# ---------------------------------------------------------------------------


def _accise_mensuel_frame(mois: list[str]) -> pl.DataFrame:
    n = len(mois)
    return pl.DataFrame(
        {
            "pdl": ["A"] * n,
            "mois_annee": mois,
            "trimestre": ["2025-T1"] * n,
            "order_name": ["SO/1"] * n,
            "energie_kwh": [1000.0] * n,
            "energie_mwh": [1.0] * n,
            "taux_accise_eur_mwh": [22.5] * n,
            "accise_eur": [22.5] * n,
        }
    )


class TestAcciseMensuel:
    def test_validates_correct_frame(self):
        from electricore.core.models.accise_mensuel import AcciseMensuel

        AcciseMensuel.validate(_accise_mensuel_frame(["2025-01", "2025-02"]))

    def test_rejects_duplicate_grain(self):
        """Le grain (pdl, mois_annee) est garanti par Config.unique."""
        from electricore.core.models.accise_mensuel import AcciseMensuel

        with pytest.raises(pandera.errors.SchemaError, match="unique"):
            AcciseMensuel.validate(_accise_mensuel_frame(["2025-01", "2025-01"]))

    def test_rejects_libelle_francais(self):
        """`mois_annee` est une clé calculable YYYY-MM, pas un libellé d'affichage (issue #115)."""
        from electricore.core.models.accise_mensuel import AcciseMensuel

        with pytest.raises(pandera.errors.SchemaError):
            AcciseMensuel.validate(_accise_mensuel_frame(["janvier 2025"]))


# ---------------------------------------------------------------------------
# CtaMensuel — contrat de pipeline_cta, grain (situation contractuelle, mois)
# ---------------------------------------------------------------------------


def _cta_mensuel_frame(mois_annee: list[str]) -> pl.DataFrame:
    n = len(mois_annee)
    return pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC001"] * n,
            "pdl": ["12345678901234"] * n,
            "mois_annee": mois_annee,
            "order_name": ["SO/2025/0001"] * n,
            "turpe_fixe_eur": [42.50] * n,
            "taux_cta_pct": [21.93] * n,
            "cta_eur": [9.32] * n,
            "trimestre": ["2025-T1"] * n,
        }
    )


class TestCtaMensuel:
    def test_validates_correct_frame(self):
        from electricore.core.models.cta_mensuel import CtaMensuel

        CtaMensuel.validate(_cta_mensuel_frame(["2025-01", "2025-02"]))

    def test_rejects_duplicate_grain(self):
        """Le grain (RSC, mois_annee) est garanti — détecte aussi un fan-out du join pdl_mapping."""
        from electricore.core.models.cta_mensuel import CtaMensuel

        with pytest.raises(pandera.errors.SchemaError, match="unique"):
            CtaMensuel.validate(_cta_mensuel_frame(["2025-01", "2025-01"]))

    def test_rejects_libelle_francais(self):
        """`mois_annee` est une clé calculable YYYY-MM, pas un libellé d'affichage (issue #115)."""
        from electricore.core.models.cta_mensuel import CtaMensuel

        with pytest.raises(pandera.errors.SchemaError):
            CtaMensuel.validate(_cta_mensuel_frame(["janvier 2025"]))

    def test_montants_nullables_pour_periodes_incompletes(self):
        """Une méta-période incomplète (turpe null → cta null) reste valide."""
        from electricore.core.models.cta_mensuel import CtaMensuel

        df = _cta_mensuel_frame(["2025-01"]).with_columns(
            pl.lit(None, dtype=pl.Float64).alias("turpe_fixe_eur"),
            pl.lit(None, dtype=pl.Float64).alias("cta_eur"),
        )
        CtaMensuel.validate(df)


# ---------------------------------------------------------------------------
# Onglets du livrable RapportTaxe
# ---------------------------------------------------------------------------


class TestRapportAcciseSchemas:
    """Les onglets Résumé / Par taux du rapport accise (le Détail = AcciseMensuel)."""

    def test_resume_schema_validates_correct_frame(self):
        from electricore.core.models.rapport_taxe import RapportAcciseResume

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
        from electricore.core.models.rapport_taxe import RapportAcciseParTaux

        df = pl.DataFrame(
            {
                "taux_accise_eur_mwh": [22.5],
                "energie_mwh": [10.0],
                "accise_eur": [225.0],
                "nb_pdl": [2],
            }
        )
        RapportAcciseParTaux.validate(df)


class TestRapportCtaSchemas:
    """Les 3 onglets du rapport CTA (le Détail est un agrégat par PDL, distinct du grain mensuel)."""

    def test_resume_schema_validates_correct_frame(self):
        from electricore.core.models.rapport_taxe import RapportCtaResume

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
        from electricore.core.models.rapport_taxe import RapportCtaParTaux

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
        from electricore.core.models.rapport_taxe import RapportCtaDetail

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
