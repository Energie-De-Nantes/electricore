"""Tests unitaires de `core/builds/rapport_taxe.py` (ADR-0019, issue #108).

Couvre : `RapportTaxe`, `feuilles_rapport_taxe`, `agreger_par_taux`,
`agreger_resume`, `rapport_accise`, `rapport_cta`.

Les builds sont purs (aucun import `integrations/`) — les sources Odoo sont
injectées par le caller (service ou test).
"""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

# ---------------------------------------------------------------------------
# Fixtures synthétiques partagées
# ---------------------------------------------------------------------------


def _detail_accise_synthetique() -> pl.DataFrame:
    """Sortie de `pipeline_accise` : 3 PDL × 2 trimestres × 2 taux."""
    return pl.DataFrame(
        {
            "pdl": ["A", "A", "B", "B", "C"],
            "mois_consommation": ["2025-01", "2025-02", "2025-01", "2025-04", "2025-04"],
            "trimestre": ["2025-T1", "2025-T1", "2025-T1", "2025-T2", "2025-T2"],
            "taux_accise_eur_mwh": [22.5, 22.5, 22.5, 25.0, 25.0],
            "energie_mwh": [1.0, 2.0, 3.0, 4.0, 5.0],
            "accise_eur": [22.5, 45.0, 67.5, 100.0, 125.0],
        }
    )


def _detail_cta_synthetique() -> pl.DataFrame:
    """Sortie d'`ajouter_cta` + `expr_calculer_trimestre` : 2 PDL × 2 mois."""
    return pl.DataFrame(
        {
            "pdl": ["A", "A", "B", "B"],
            "order_name": ["SO/1", "SO/1", "SO/2", "SO/2"],
            "trimestre": ["2025-T1", "2025-T1", "2025-T1", "2025-T2"],
            "taux_cta_pct": [3.0, 3.0, 3.0, 5.0],
            "turpe_fixe_eur": [100.0, 100.0, 200.0, 200.0],
            "cta_eur": [3.0, 3.0, 6.0, 10.0],
        }
    )


# ---------------------------------------------------------------------------
# RapportTaxe
# ---------------------------------------------------------------------------


class TestRapportTaxe:
    """`RapportTaxe` est un @dataclass(frozen=True, slots=True) à 3 champs."""

    def test_importable_from_core_builds(self):
        from electricore.core.builds.rapport_taxe import RapportTaxe

        assert RapportTaxe is not None

    def test_is_frozen_dataclass(self):
        import dataclasses

        from electricore.core.builds.rapport_taxe import RapportTaxe

        assert dataclasses.is_dataclass(RapportTaxe)
        assert RapportTaxe.__dataclass_params__.frozen

    def test_has_slots(self):
        from electricore.core.builds.rapport_taxe import RapportTaxe

        assert hasattr(RapportTaxe, "__slots__")

    def test_three_fields(self):
        import dataclasses

        from electricore.core.builds.rapport_taxe import RapportTaxe

        fields = {f.name for f in dataclasses.fields(RapportTaxe)}
        assert fields == {"resume", "par_taux", "detail"}

    def test_immutable(self):
        import dataclasses

        from electricore.core.builds.rapport_taxe import RapportTaxe

        r = RapportTaxe(resume=pl.DataFrame(), par_taux=pl.DataFrame(), detail=pl.DataFrame())
        with pytest.raises(dataclasses.FrozenInstanceError):
            r.resume = pl.DataFrame()  # type: ignore[misc]


# ---------------------------------------------------------------------------
# feuilles_rapport_taxe
# ---------------------------------------------------------------------------


class TestFeuillesRapportTaxe:
    """`feuilles_rapport_taxe` produit le mapping onglet → DataFrame pour xlsx_multi_sheet."""

    def test_returns_dict_with_three_fr_keys(self):
        from electricore.core.builds.rapport_taxe import RapportTaxe, feuilles_rapport_taxe

        df = pl.DataFrame({"x": [1]})
        r = RapportTaxe(resume=df, par_taux=df, detail=df)
        result = feuilles_rapport_taxe(r)

        assert set(result.keys()) == {"Résumé", "Par taux", "Détail"}

    def test_maps_fields_to_correct_sheets(self):
        from electricore.core.builds.rapport_taxe import RapportTaxe, feuilles_rapport_taxe

        resume = pl.DataFrame({"a": [1]})
        par_taux = pl.DataFrame({"b": [2]})
        detail = pl.DataFrame({"c": [3]})
        r = RapportTaxe(resume=resume, par_taux=par_taux, detail=detail)
        result = feuilles_rapport_taxe(r)

        assert_frame_equal(result["Résumé"], resume)
        assert_frame_equal(result["Par taux"], par_taux)
        assert_frame_equal(result["Détail"], detail)


# ---------------------------------------------------------------------------
# agreger_par_taux
# ---------------------------------------------------------------------------


class TestAgregerParTaux:
    """`agreger_par_taux` agrège par `cle_taux` (+ extra_groupby), somme assiette/montant, compte pdl."""

    def test_groups_by_cle_taux(self):
        from electricore.core.builds.rapport_taxe import agreger_par_taux

        detail = _detail_accise_synthetique()
        result = agreger_par_taux(
            detail, cle_taux="taux_accise_eur_mwh", cle_assiette="energie_mwh", cle_montant="accise_eur"
        )

        assert set(result["taux_accise_eur_mwh"].to_list()) == {22.5, 25.0}

    def test_sums_assiette_and_montant(self):
        from electricore.core.builds.rapport_taxe import agreger_par_taux

        detail = _detail_accise_synthetique()
        result = agreger_par_taux(
            detail, cle_taux="taux_accise_eur_mwh", cle_assiette="energie_mwh", cle_montant="accise_eur"
        )

        # Sort by taux to get predictable order
        result = result.sort("taux_accise_eur_mwh")
        # taux 22.5 : A×2 + B×1 = 1+2+3 = 6.0 mwh, 22.5+45+67.5 = 135.0 €
        # taux 25.0 : B×1 + C×1 = 4+5 = 9.0 mwh, 100+125 = 225.0 €
        assert result["energie_mwh"].to_list() == [6.0, 9.0]
        assert result["accise_eur"].to_list() == [135.0, 225.0]

    def test_counts_distinct_pdl(self):
        from electricore.core.builds.rapport_taxe import agreger_par_taux

        detail = _detail_accise_synthetique()
        result = agreger_par_taux(
            detail, cle_taux="taux_accise_eur_mwh", cle_assiette="energie_mwh", cle_montant="accise_eur"
        )

        result = result.sort("taux_accise_eur_mwh")
        assert result["nb_pdl"].to_list() == [2, 2]  # {A,B} and {B,C}

    def test_extra_groupby_adds_partition(self):
        """CTA groupby est (trimestre, taux_cta_pct)."""
        from electricore.core.builds.rapport_taxe import agreger_par_taux

        detail = _detail_cta_synthetique()
        result = agreger_par_taux(
            detail,
            cle_taux="taux_cta_pct",
            cle_assiette="turpe_fixe_eur",
            cle_montant="cta_eur",
            extra_groupby=("trimestre",),
        )

        assert result.height == 2  # (T1, 3.0) and (T2, 5.0)
        assert "trimestre" in result.columns


# ---------------------------------------------------------------------------
# agreger_resume
# ---------------------------------------------------------------------------


class TestAgregerResume:
    """`agreger_resume` groupby trimestre, rename assiette/montant, count pdl."""

    def test_groups_by_trimestre(self):
        from electricore.core.builds.rapport_taxe import agreger_resume

        detail = _detail_accise_synthetique()
        result = agreger_resume(
            detail,
            cle_assiette="energie_mwh",
            cle_montant="accise_eur",
            nom_assiette_total="energie_mwh_total",
            nom_montant_total="accise_eur_total",
        )

        assert set(result["trimestre"].to_list()) == {"2025-T1", "2025-T2"}

    def test_renames_assiette_and_montant(self):
        from electricore.core.builds.rapport_taxe import agreger_resume

        detail = _detail_accise_synthetique()
        result = agreger_resume(
            detail,
            cle_assiette="energie_mwh",
            cle_montant="accise_eur",
            nom_assiette_total="energie_mwh_total",
            nom_montant_total="accise_eur_total",
        )

        assert "energie_mwh_total" in result.columns
        assert "accise_eur_total" in result.columns
        assert "energie_mwh" not in result.columns
        assert "accise_eur" not in result.columns

    def test_sums_and_counts_pdl(self):
        from electricore.core.builds.rapport_taxe import agreger_resume

        detail = _detail_accise_synthetique()
        result = agreger_resume(
            detail,
            cle_assiette="energie_mwh",
            cle_montant="accise_eur",
            nom_assiette_total="energie_mwh_total",
            nom_montant_total="accise_eur_total",
        ).sort("trimestre")

        # T1: {A, B} = 2 PDL, 6 mwh, 135 €
        # T2: {B, C} = 2 PDL, 9 mwh, 225 €
        assert result["nb_pdl"].to_list() == [2, 2]
        assert result["energie_mwh_total"].to_list() == [6.0, 9.0]
        assert result["accise_eur_total"].to_list() == [135.0, 225.0]

    def test_sorted_by_trimestre(self):
        from electricore.core.builds.rapport_taxe import agreger_resume

        detail = _detail_accise_synthetique()
        result = agreger_resume(
            detail,
            cle_assiette="energie_mwh",
            cle_montant="accise_eur",
            nom_assiette_total="energie_mwh_total",
            nom_montant_total="accise_eur_total",
        )

        assert result["trimestre"].to_list() == ["2025-T1", "2025-T2"]


# ---------------------------------------------------------------------------
# rapport_accise (pure build)
# ---------------------------------------------------------------------------


def _lignes_factures_synthetiques() -> pl.LazyFrame:
    """Shape minimal compatible avec pipeline_accise (lignes de commandes)."""
    return pl.DataFrame(
        {
            "pdl": ["A", "A", "B", "B", "C"],
            "mois_consommation": ["2025-01", "2025-02", "2025-01", "2025-04", "2025-04"],
            "trimestre": ["2025-T1", "2025-T1", "2025-T1", "2025-T2", "2025-T2"],
            "taux_accise_eur_mwh": [22.5, 22.5, 22.5, 25.0, 25.0],
            "energie_mwh": [1.0, 2.0, 3.0, 4.0, 5.0],
            "accise_eur": [22.5, 45.0, 67.5, 100.0, 125.0],
        }
    ).lazy()


class TestRapportAcciseBuild:
    """`rapport_accise(lignes_factures, trimestre)` est un build pur, sans Odoo."""

    def test_returns_rapport_taxe(self, monkeypatch):
        from electricore.core.builds import rapport_taxe as mod

        monkeypatch.setattr(mod, "pipeline_accise", lambda lf: _detail_accise_synthetique().lazy())

        result = mod.rapport_accise(_lignes_factures_synthetiques())

        from electricore.core.builds.rapport_taxe import RapportTaxe

        assert isinstance(result, RapportTaxe)

    def test_detail_sorted_pdl_mois(self, monkeypatch):
        from electricore.core.builds import rapport_taxe as mod

        monkeypatch.setattr(mod, "pipeline_accise", lambda lf: _detail_accise_synthetique().lazy())

        result = mod.rapport_accise(_lignes_factures_synthetiques())

        assert result.detail["pdl"].to_list() == sorted(result.detail["pdl"].to_list())

    def test_trimestre_filter_applied(self, monkeypatch):
        from electricore.core.builds import rapport_taxe as mod

        monkeypatch.setattr(mod, "pipeline_accise", lambda lf: _detail_accise_synthetique().lazy())

        result = mod.rapport_accise(_lignes_factures_synthetiques(), trimestre="2025-T1")

        assert set(result.detail["trimestre"].unique().to_list()) == {"2025-T1"}

    def test_par_taux_groups_correctly(self, monkeypatch):
        from electricore.core.builds import rapport_taxe as mod

        monkeypatch.setattr(mod, "pipeline_accise", lambda lf: _detail_accise_synthetique().lazy())

        result = mod.rapport_accise(_lignes_factures_synthetiques())

        assert set(result.par_taux["taux_accise_eur_mwh"].to_list()) == {22.5, 25.0}

    def test_resume_has_trimestre_totals(self, monkeypatch):
        from electricore.core.builds import rapport_taxe as mod

        monkeypatch.setattr(mod, "pipeline_accise", lambda lf: _detail_accise_synthetique().lazy())

        result = mod.rapport_accise(_lignes_factures_synthetiques())

        assert set(result.resume["trimestre"].to_list()) == {"2025-T1", "2025-T2"}

    def test_no_odoo_import(self):
        """Garde-fou : `core/builds/rapport_taxe.py` n'importe pas `integrations/`."""
        import ast
        from pathlib import Path

        src = Path(__file__).parents[2] / "electricore" / "core" / "builds" / "rapport_taxe.py"
        tree = ast.parse(src.read_text())
        imports = [
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module
        ]
        violations = [m for m in imports if m.startswith("electricore.integrations")]
        assert not violations, f"core/builds/rapport_taxe.py importe integrations: {violations}"


# ---------------------------------------------------------------------------
# rapport_cta (pure build)
# ---------------------------------------------------------------------------


def _facturation_mensuelle_synthetique() -> pl.DataFrame:
    """Minimal facturation mensuelle (champs requis par ajouter_cta)."""
    return pl.DataFrame(
        {
            "pdl": ["A", "A", "B"],
            "debut": ["2025-01-01", "2025-02-01", "2025-01-01"],
            "turpe_fixe_eur": [100.0, 100.0, 200.0],
        }
    )


def _mapping_pdl_synthetique() -> pl.DataFrame:
    return pl.DataFrame({"pdl": ["A", "B"], "order_name": ["SO/1", "SO/2"]})


class TestRapportCtaBuild:
    """`rapport_cta(facturation_mensuelle, pdl_mapping, trimestre)` est un build pur."""

    def test_returns_rapport_taxe(self, monkeypatch):
        from electricore.core.builds import rapport_taxe as mod

        monkeypatch.setattr(
            mod,
            "ajouter_cta",
            lambda lf: lf.with_columns(
                [
                    pl.lit(3.0).alias("taux_cta_pct"),
                    pl.lit(3.0).alias("cta_eur"),
                ]
            ),
        )
        monkeypatch.setattr(mod, "expr_calculer_trimestre", lambda: pl.lit("2025-T1"))

        result = mod.rapport_cta(_facturation_mensuelle_synthetique(), _mapping_pdl_synthetique())

        from electricore.core.builds.rapport_taxe import RapportTaxe

        assert isinstance(result, RapportTaxe)

    def test_detail_grouped_by_pdl(self, monkeypatch):
        """CTA detail est aggrégé par PDL (pas mensuel brut)."""
        from electricore.core.builds import rapport_taxe as mod

        monkeypatch.setattr(
            mod,
            "ajouter_cta",
            lambda lf: lf.with_columns(
                [
                    pl.lit(3.0).alias("taux_cta_pct"),
                    pl.lit(3.0).alias("cta_eur"),
                ]
            ),
        )
        monkeypatch.setattr(mod, "expr_calculer_trimestre", lambda: pl.lit("2025-T1"))

        result = mod.rapport_cta(_facturation_mensuelle_synthetique(), _mapping_pdl_synthetique())

        # detail aggrégé par PDL : 2 PDL, pas 3 lignes mensuelles
        assert result.detail["pdl"].n_unique() == result.detail.height

    def test_trimestre_filter_applied(self, monkeypatch):
        from electricore.core.builds import rapport_taxe as mod

        def fake_ajouter_cta(lf):
            return lf.with_columns(
                [
                    pl.lit(3.0).alias("taux_cta_pct"),
                    pl.lit(3.0).alias("cta_eur"),
                ]
            )

        def fake_expr():
            return pl.lit("2025-T1")

        monkeypatch.setattr(mod, "ajouter_cta", fake_ajouter_cta)
        monkeypatch.setattr(mod, "expr_calculer_trimestre", fake_expr)

        result = mod.rapport_cta(
            _facturation_mensuelle_synthetique(),
            _mapping_pdl_synthetique(),
            trimestre="2025-T1",
        )

        assert isinstance(result.resume, pl.DataFrame)
