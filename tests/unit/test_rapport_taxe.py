"""Tests unitaires de `core/builds/rapport_taxe.py` (ADR-0019, issue #108).

Couvre : `RapportTaxe`, `feuilles_rapport_taxe`, `agreger_par_taux`,
`agreger_resume`, `rapport_accise`, `rapport_cta`.

Les builds sont purs (aucun import `integrations/`) — les sources Odoo sont
injectées par le caller (service ou test).
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from polars.testing import assert_frame_equal

TZ = ZoneInfo("Europe/Paris")

# ---------------------------------------------------------------------------
# Fixtures synthétiques partagées
# ---------------------------------------------------------------------------


def _detail_accise_synthetique() -> pl.DataFrame:
    """Sortie de `pipeline_accise` (shape `AcciseMensuel`) : 3 PDL × 2 trimestres × 2 taux."""
    return pl.DataFrame(
        {
            "pdl": ["A", "A", "B", "B", "C"],
            "mois_annee": ["2025-01", "2025-02", "2025-01", "2025-04", "2025-04"],
            "trimestre": ["2025-T1", "2025-T1", "2025-T1", "2025-T2", "2025-T2"],
            "order_name": ["SO/1", "SO/1", "SO/2", "SO/2", "SO/3"],
            "energie_kwh": [1000.0, 2000.0, 3000.0, 4000.0, 5000.0],
            "taux_accise_eur_mwh": [22.5, 22.5, 22.5, 25.0, 25.0],
            "energie_mwh": [1.0, 2.0, 3.0, 4.0, 5.0],
            "accise_eur": [22.5, 45.0, 67.5, 100.0, 125.0],
        }
    )


def _detail_cta_synthetique() -> pl.DataFrame:
    """Sortie de `pipeline_cta` (shape `CtaMensuel`) : 2 PDL × 2 mois."""
    return pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-A", "RSC-A", "RSC-B", "RSC-B"],
            "pdl": ["A", "A", "B", "B"],
            "mois_annee": ["2025-01", "2025-02", "2025-01", "2025-04"],
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
            "mois_annee": ["2025-01", "2025-02", "2025-01", "2025-04", "2025-04"],
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
    """Minimal facturation mensuelle (champs requis par pipeline_cta)."""
    return pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC-A", "RSC-A", "RSC-B"],
            "pdl": ["A", "A", "B"],
            "mois_annee": ["2025-01", "2025-02", "2025-01"],
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

        monkeypatch.setattr(mod, "pipeline_cta", lambda fact, mapping, regles=None: _detail_cta_synthetique().lazy())

        result = mod.rapport_cta(_facturation_mensuelle_synthetique(), _mapping_pdl_synthetique())

        from electricore.core.builds.rapport_taxe import RapportTaxe

        assert isinstance(result, RapportTaxe)

    def test_detail_grouped_by_pdl(self, monkeypatch):
        """CTA detail est aggrégé par PDL (pas mensuel brut)."""
        from electricore.core.builds import rapport_taxe as mod

        monkeypatch.setattr(mod, "pipeline_cta", lambda fact, mapping, regles=None: _detail_cta_synthetique().lazy())

        result = mod.rapport_cta(_facturation_mensuelle_synthetique(), _mapping_pdl_synthetique())

        # detail aggrégé par PDL : 2 PDL, pas 4 lignes mensuelles
        assert result.detail["pdl"].n_unique() == result.detail.height

    def test_trimestre_filter_applied(self, monkeypatch):
        from electricore.core.builds import rapport_taxe as mod

        monkeypatch.setattr(mod, "pipeline_cta", lambda fact, mapping, regles=None: _detail_cta_synthetique().lazy())

        result = mod.rapport_cta(
            _facturation_mensuelle_synthetique(),
            _mapping_pdl_synthetique(),
            trimestre="2025-T1",
        )

        # PDL B n'a qu'une ligne T2 sur 2 ; le filtre exclut sa ligne d'avril
        assert set(result.par_taux["trimestre"].to_list()) == {"2025-T1"}


# ---------------------------------------------------------------------------
# rapport_cta — invariants de valeur (issue #112 : migration depuis TestPipelineCta)
# ---------------------------------------------------------------------------


@pytest.fixture
def regles_cta_synthetiques_rapport() -> pl.LazyFrame:
    """Historique CTA synthétique : deux changements de taux (même que test_cta.py)."""
    return pl.LazyFrame(
        {
            "start": [
                datetime(2020, 1, 1, tzinfo=TZ),
                datetime(2021, 8, 1, tzinfo=TZ),
                datetime(2026, 2, 1, tzinfo=TZ),
            ],
            "taux_cta_pct": [27.04, 21.93, 15.00],
        }
    )


@pytest.fixture
def facturation_2026_synthetique() -> pl.DataFrame:
    """PDL A sur 2026-T1 (3 mois) + PDL B sur 2021-T3 (1 mois)."""
    rows = [
        {"rsc": "RSC-A", "mois_annee": "2026-01", "pdl": "A", "debut": datetime(2026, 1, 1, tzinfo=TZ)},
        {"rsc": "RSC-A", "mois_annee": "2026-02", "pdl": "A", "debut": datetime(2026, 2, 1, tzinfo=TZ)},
        {"rsc": "RSC-A", "mois_annee": "2026-03", "pdl": "A", "debut": datetime(2026, 3, 1, tzinfo=TZ)},
        {"rsc": "RSC-B", "mois_annee": "2021-08", "pdl": "B", "debut": datetime(2021, 8, 1, tzinfo=TZ)},
    ]
    return (
        pl.DataFrame(rows)
        .with_columns(pl.when(pl.col("pdl") == "A").then(100.0).otherwise(50.0).alias("turpe_fixe_eur"))
        .rename({"rsc": "ref_situation_contractuelle"})
    )


@pytest.fixture
def mapping_pdl_synthetique_rapport() -> pl.DataFrame:
    return pl.DataFrame({"pdl": ["A", "B"], "order_name": ["SO-A", "SO-B"]})


class TestRapportCtaValeurs:
    """Invariants de valeur de rapport_cta — agrégation par PDL et changements de taux."""

    def test_detail_colonnes(
        self,
        facturation_2026_synthetique,
        mapping_pdl_synthetique_rapport,
        regles_cta_synthetiques_rapport,
    ):
        from electricore.core.builds.rapport_taxe import rapport_cta

        result = rapport_cta(
            facturation_2026_synthetique,
            mapping_pdl_synthetique_rapport,
            regles=regles_cta_synthetiques_rapport,
        )
        assert set(result.detail.columns) == {
            "pdl",
            "order_name",
            "turpe_fixe_total_eur",
            "cta_total_eur",
            "taux_cta_appliques",
        }

    def test_agregation_changement_taux_2026_t1(
        self,
        facturation_2026_synthetique,
        mapping_pdl_synthetique_rapport,
        regles_cta_synthetiques_rapport,
    ):
        """PDL A sur 2026-T1 : 100×21.93% + 100×15% + 100×15% = 51.93 €."""
        from electricore.core.builds.rapport_taxe import rapport_cta

        result = rapport_cta(
            facturation_2026_synthetique,
            mapping_pdl_synthetique_rapport,
            trimestre="2026-T1",
            regles=regles_cta_synthetiques_rapport,
        )
        row_a = result.detail.filter(pl.col("pdl") == "A")
        assert row_a.height == 1
        assert row_a["turpe_fixe_total_eur"].item() == 300.0
        assert row_a["cta_total_eur"].item() == pytest.approx(51.93, abs=1e-9)
        assert "21.93" in row_a["taux_cta_appliques"].item()
        assert "15.0" in row_a["taux_cta_appliques"].item()

    def test_taux_unique_quand_pas_de_changement(
        self,
        facturation_2026_synthetique,
        mapping_pdl_synthetique_rapport,
        regles_cta_synthetiques_rapport,
    ):
        """PDL B en 2021-T3 : un seul taux appliqué."""
        from electricore.core.builds.rapport_taxe import rapport_cta

        result = rapport_cta(
            facturation_2026_synthetique,
            mapping_pdl_synthetique_rapport,
            trimestre="2021-T3",
            regles=regles_cta_synthetiques_rapport,
        )
        row_b = result.detail.filter(pl.col("pdl") == "B")
        assert row_b.height == 1
        assert ";" not in row_b["taux_cta_appliques"].item()

    def test_filtre_trimestre_exclusif(
        self,
        facturation_2026_synthetique,
        mapping_pdl_synthetique_rapport,
        regles_cta_synthetiques_rapport,
    ):
        """Filtrer sur 2026-T1 ne doit pas inclure PDL B (période 2021)."""
        from electricore.core.builds.rapport_taxe import rapport_cta

        result = rapport_cta(
            facturation_2026_synthetique,
            mapping_pdl_synthetique_rapport,
            trimestre="2026-T1",
            regles=regles_cta_synthetiques_rapport,
        )
        assert "B" not in result.detail["pdl"].to_list()

    def test_detail_sort_cta_descending(
        self,
        facturation_2026_synthetique,
        mapping_pdl_synthetique_rapport,
        regles_cta_synthetiques_rapport,
    ):
        from electricore.core.builds.rapport_taxe import rapport_cta

        result = rapport_cta(
            facturation_2026_synthetique,
            mapping_pdl_synthetique_rapport,
            regles=regles_cta_synthetiques_rapport,
        )
        cta_values = result.detail["cta_total_eur"].to_list()
        assert cta_values == sorted(cta_values, reverse=True)

    def test_pdl_absent_de_mapping_exclu(self, regles_cta_synthetiques_rapport):
        """PDL présent en facturation mais absent du mapping → exclu (inner join)."""
        from electricore.core.builds.rapport_taxe import rapport_cta

        df_fact = pl.DataFrame(
            [
                {
                    "ref_situation_contractuelle": "RSC-A",
                    "mois_annee": "2025-01",
                    "pdl": "A",
                    "debut": datetime(2025, 1, 1, tzinfo=TZ),
                    "turpe_fixe_eur": 100.0,
                },
                {
                    "ref_situation_contractuelle": "RSC-ORPHAN",
                    "mois_annee": "2025-01",
                    "pdl": "ORPHAN",
                    "debut": datetime(2025, 1, 1, tzinfo=TZ),
                    "turpe_fixe_eur": 100.0,
                },
            ]
        )
        df_mapping = pl.DataFrame({"pdl": ["A"], "order_name": ["SO-A"]})
        result = rapport_cta(df_fact, df_mapping, regles=regles_cta_synthetiques_rapport)
        assert set(result.detail["pdl"].to_list()) == {"A"}
