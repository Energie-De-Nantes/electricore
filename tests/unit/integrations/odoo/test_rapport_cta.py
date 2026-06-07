"""Tests pour `cta_par_contrat` et `rapport_cta` (Candidate 1.2, issue #63).

Réplique du pattern validé par #56 (accise) sur le domaine CTA. Spécificité :
le `detail` du livrable est une **agrégation par PDL** (sommes + liste de taux
sérialisée), pas la sortie brute de `cta_par_contrat` (qui reste mensuelle).
"""

import polars as pl


def test_cta_par_contrat_exists_and_is_callable():
    """`cta_par_contrat` est exporté depuis `integrations.odoo.taxes`."""
    from electricore.integrations.odoo import taxes

    assert hasattr(taxes, "cta_par_contrat")
    assert callable(taxes.cta_par_contrat)


def test_cta_du_trimestre_is_removed():
    """L'ancien nom `cta_du_trimestre` n'existe plus."""
    from electricore.integrations.odoo import taxes

    assert not hasattr(taxes, "cta_du_trimestre")


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


def _monthly_synthetique() -> pl.DataFrame:
    """`cta_par_contrat` synthétique : 3 PDL × plusieurs mois × 2 taux distincts.

    PDL "A" : T1 (taux 21.61 sur 2 mois) → 2 lignes
    PDL "B" : T1 (taux 21.61 sur 1 mois) + T2 (taux 22.10 sur 2 mois — décret CRE) → 3 lignes
    PDL "C" : T2 (taux 22.10 sur 1 mois) → 1 ligne
    """
    return pl.DataFrame(
        {
            "pdl": ["A", "A", "B", "B", "B", "C"],
            "order_name": ["SO/A", "SO/A", "SO/B", "SO/B", "SO/B", "SO/C"],
            "trimestre": ["2025-T1", "2025-T1", "2025-T1", "2025-T2", "2025-T2", "2025-T2"],
            "taux_cta_pct": [21.61, 21.61, 21.61, 22.10, 22.10, 22.10],
            "turpe_fixe_eur": [10.0, 12.0, 8.0, 14.0, 14.0, 20.0],
            "cta_eur": [2.16, 2.59, 1.73, 3.09, 3.09, 4.42],
        }
    )


class TestRapportCta:
    """`rapport_cta` produit un `RapportCta(resume, par_taux, detail)`."""

    def test_returns_namedtuple_with_three_fields(self, monkeypatch):
        from electricore.integrations.odoo import taxes

        monkeypatch.setattr(taxes, "cta_par_contrat", lambda odoo, trimestre=None: _monthly_synthetique())
        result = taxes.rapport_cta(odoo=None)

        assert hasattr(result, "resume")
        assert hasattr(result, "par_taux")
        assert hasattr(result, "detail")

    def test_par_taux_groups_by_trimestre_and_taux(self, monkeypatch):
        """`par_taux` indexé par (trimestre, taux_cta_pct), sommes + n_unique(pdl)."""
        from electricore.integrations.odoo import taxes

        monkeypatch.setattr(taxes, "cta_par_contrat", lambda odoo, trimestre=None: _monthly_synthetique())
        result = taxes.rapport_cta(odoo=None)

        # Trié sur (trimestre, taux_cta_pct) — 2 lignes : T1 @ 21.61 et T2 @ 22.10
        assert result.par_taux["trimestre"].to_list() == ["2025-T1", "2025-T2"]
        assert result.par_taux["taux_cta_pct"].to_list() == [21.61, 22.10]
        # T1 @ 21.61 : PDLs {A, B} = 2 ; turpe 10+12+8 = 30.0 ; cta 2.16+2.59+1.73 = 6.48
        # T2 @ 22.10 : PDLs {B, C} = 2 ; turpe 14+14+20 = 48.0 ; cta 3.09+3.09+4.42 = 10.60
        assert result.par_taux["nb_pdl"].to_list() == [2, 2]
        assert result.par_taux["turpe_fixe_eur"].to_list() == [30.0, 48.0]
        assert result.par_taux["cta_eur"].to_list() == [6.48, 10.60]

    def test_resume_groups_by_trimestre(self, monkeypatch):
        """`resume` groupé par trimestre avec totaux + n_unique(pdl) corrects."""
        from electricore.integrations.odoo import taxes

        monkeypatch.setattr(taxes, "cta_par_contrat", lambda odoo, trimestre=None: _monthly_synthetique())
        result = taxes.rapport_cta(odoo=None)

        assert result.resume["trimestre"].to_list() == ["2025-T1", "2025-T2"]
        # T1: PDL = {A, B} → 2 distinct ; turpe 10+12+8 = 30.0 ; cta 2.16+2.59+1.73 = 6.48
        # T2: PDL = {B, C} → 2 distinct ; turpe 14+14+20 = 48.0 ; cta 3.09+3.09+4.42 = 10.60
        assert result.resume["nb_pdl"].to_list() == [2, 2]
        assert result.resume["turpe_fixe_total_eur"].to_list() == [30.0, 48.0]
        assert result.resume["cta_total_eur"].to_list() == [6.48, 10.60]

    def test_detail_per_pdl_with_taux_appliques_joined(self, monkeypatch):
        """`detail` agrégé par PDL ; PDL avec 2 taux successifs → string joined ` ; `."""
        from electricore.integrations.odoo import taxes

        monkeypatch.setattr(taxes, "cta_par_contrat", lambda odoo, trimestre=None: _monthly_synthetique())
        result = taxes.rapport_cta(odoo=None)

        # 3 PDL distincts → 3 lignes
        assert sorted(result.detail["pdl"].to_list()) == ["A", "B", "C"]

        # PDL B subit la transition de taux (21.61 → 22.10) — string concat des deux
        # Note : Polars cast Float64 → Utf8 drop le trailing zero (`22.10` → `"22.1"`).
        # C'est le comportement historique du XLSX facturiste, on le préserve tel quel.
        pdl_b = result.detail.filter(pl.col("pdl") == "B").row(0, named=True)
        assert pdl_b["taux_cta_appliques"] == "21.61 ; 22.1"
        # PDL A ne touche qu'un taux
        pdl_a = result.detail.filter(pl.col("pdl") == "A").row(0, named=True)
        assert pdl_a["taux_cta_appliques"] == "21.61"

    def test_rapport_cta_propagates_trimestre_filter(self, monkeypatch):
        from electricore.integrations.odoo import taxes

        appels: list[str | None] = []

        def fake_cta_par_contrat(odoo, trimestre=None):
            appels.append(trimestre)
            return _monthly_synthetique()

        monkeypatch.setattr(taxes, "cta_par_contrat", fake_cta_par_contrat)
        taxes.rapport_cta(odoo=None, trimestre="2025-T1")

        assert appels == ["2025-T1"]
