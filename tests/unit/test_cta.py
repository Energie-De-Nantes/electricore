"""
Tests unitaires pour le pipeline CTA.

Couvre le chargement de l'historique des taux, le wire-through Accise-spécifique
(formule × / 100, arrondi, préservation des colonnes), et l'agrégation par PDL
avec gestion des changements de taux intra-trimestre.

La sélection du taux en vigueur à la date est testée séparément dans
`test_taux.py` — ici on vérifie uniquement ce qui est propre à la CTA.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from electricore.core.pipelines.cta import (
    ajouter_cta,
    load_cta_rules,
    pipeline_cta,
)

TZ = ZoneInfo("Europe/Paris")


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture
def regles_cta_synthetiques() -> pl.LazyFrame:
    """
    Historique CTA synthétique avec deux changements de taux.

    Mimique la structure du CSV de prod pour isoler les tests de la valeur
    exacte du fichier. Chaque ligne = entrée en vigueur d'un nouveau taux.
    """
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


def _facturation_mensuelle(rows: list[dict]) -> pl.LazyFrame:
    """Construit un LazyFrame de facturation mensuelle avec tz Europe/Paris."""
    return pl.LazyFrame(rows).with_columns(pl.col("debut").dt.replace_time_zone("Europe/Paris"))


# =============================================================================
# CHARGEMENT DES RÈGLES
# =============================================================================


class TestChargementRegles:
    """Valide le chargement de electricore/config/cta_rules.csv."""

    def test_load_cta_rules_returns_lazyframe(self):
        regles = load_cta_rules()
        assert isinstance(regles, pl.LazyFrame)

    def test_load_cta_rules_colonnes_et_types(self):
        regles = load_cta_rules().collect()
        assert set(regles.columns) == {"start", "taux_cta_pct"}
        assert regles["taux_cta_pct"].dtype == pl.Float64
        assert regles["start"].dtype == pl.Datetime(time_unit="us", time_zone="Europe/Paris")

    def test_load_cta_rules_contient_au_moins_une_ligne(self):
        regles = load_cta_rules().collect()
        assert regles.height >= 1


# =============================================================================
# AJOUT DU TAUX ET DU MONTANT (NIVEAU MENSUEL)
# =============================================================================


class TestAjouterCta:
    """Valide la jointure asof et le calcul mensuel."""

    def test_colonnes_ajoutees(self, regles_cta_synthetiques):
        fact = _facturation_mensuelle(
            [
                {
                    "pdl": "A",
                    "debut": datetime(2025, 3, 1),
                    "turpe_fixe_eur": 100.0,
                }
            ]
        )
        result = ajouter_cta(fact, regles_cta_synthetiques).collect()
        assert "taux_cta_pct" in result.columns
        assert "cta_eur" in result.columns

    def test_calcul_cta_eur(self, regles_cta_synthetiques):
        fact = _facturation_mensuelle(
            [
                {
                    "pdl": "A",
                    "debut": datetime(2025, 3, 1),
                    "turpe_fixe_eur": 100.0,
                }
            ]
        )
        result = ajouter_cta(fact, regles_cta_synthetiques).collect()
        assert result["cta_eur"].item() == 21.93  # 100 × 21.93 / 100

    def test_arrondi_2_decimales(self, regles_cta_synthetiques):
        fact = _facturation_mensuelle(
            [
                {
                    "pdl": "A",
                    "debut": datetime(2025, 3, 1),
                    "turpe_fixe_eur": 123.456,
                }
            ]
        )
        result = ajouter_cta(fact, regles_cta_synthetiques).collect()
        # 123.456 × 21.93 / 100 = 27.0739... → arrondi 27.07
        assert result["cta_eur"].item() == pytest.approx(27.07, abs=1e-9)

    def test_colonnes_originales_preservees(self, regles_cta_synthetiques):
        fact = _facturation_mensuelle(
            [
                {
                    "pdl": "A",
                    "debut": datetime(2025, 3, 1),
                    "turpe_fixe_eur": 100.0,
                    "ref_situation_contractuelle": "REF1",  # colonne extra
                }
            ]
        )
        result = ajouter_cta(fact, regles_cta_synthetiques).collect()
        assert "ref_situation_contractuelle" in result.columns
        assert result["ref_situation_contractuelle"].item() == "REF1"

    def test_changement_taux_intra_trimestre(self, regles_cta_synthetiques):
        """
        Cas clé : trimestre 2026-T1 avec changement au 2026-02-01.
        Janvier doit recevoir 21.93 %, février et mars 15.00 %.
        """
        fact = _facturation_mensuelle(
            [
                {"pdl": "A", "debut": datetime(2026, 1, 1), "turpe_fixe_eur": 100.0},
                {"pdl": "A", "debut": datetime(2026, 2, 1), "turpe_fixe_eur": 100.0},
                {"pdl": "A", "debut": datetime(2026, 3, 1), "turpe_fixe_eur": 100.0},
            ]
        )
        result = ajouter_cta(fact, regles_cta_synthetiques).collect().sort("debut")
        assert result["taux_cta_pct"].to_list() == [21.93, 15.00, 15.00]
        assert result["cta_eur"].to_list() == [21.93, 15.00, 15.00]


# =============================================================================
# PIPELINE COMPLET (AGRÉGATION PAR PDL)
# =============================================================================


class TestPipelineCta:
    """Valide l'agrégation par PDL et la détection de changements de taux."""

    @pytest.fixture
    def df_pdl(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "pdl": ["A", "B"],
                "order_name": ["SO-A", "SO-B"],
            }
        )

    @pytest.fixture
    def df_facturation(self) -> pl.DataFrame:
        rows = [
            {"pdl": "A", "debut": datetime(2026, 1, 1), "turpe_fixe_eur": 100.0},
            {"pdl": "A", "debut": datetime(2026, 2, 1), "turpe_fixe_eur": 100.0},
            {"pdl": "A", "debut": datetime(2026, 3, 1), "turpe_fixe_eur": 100.0},
            {"pdl": "B", "debut": datetime(2021, 8, 1), "turpe_fixe_eur": 50.0},
        ]
        return pl.DataFrame(rows).with_columns(pl.col("debut").dt.replace_time_zone("Europe/Paris"))

    def test_colonnes_sortie(self, df_facturation, df_pdl, regles_cta_synthetiques):
        result = pipeline_cta(df_facturation, df_pdl, regles=regles_cta_synthetiques).collect()
        assert set(result.columns) == {
            "pdl",
            "order_name",
            "turpe_fixe_total",
            "cta",
            "taux_cta_appliques",
        }

    def test_agregation_changement_taux_2026_t1(self, df_facturation, df_pdl, regles_cta_synthetiques):
        """PDL A sur 2026-T1 : 100×21.93% + 100×15% + 100×15% = 51.93 €."""
        result = pipeline_cta(
            df_facturation,
            df_pdl,
            trimestre="2026-T1",
            regles=regles_cta_synthetiques,
        ).collect()
        row_a = result.filter(pl.col("pdl") == "A")
        assert row_a.height == 1
        assert row_a["turpe_fixe_total"].item() == 300.0
        assert row_a["cta"].item() == pytest.approx(51.93, abs=1e-9)
        assert row_a["taux_cta_appliques"].item().to_list() == [15.00, 21.93]

    def test_taux_unique_quand_pas_de_changement(self, df_facturation, df_pdl, regles_cta_synthetiques):
        """PDL B en 2021-T3 : un seul taux appliqué."""
        result = pipeline_cta(
            df_facturation,
            df_pdl,
            trimestre="2021-T3",
            regles=regles_cta_synthetiques,
        ).collect()
        row_b = result.filter(pl.col("pdl") == "B")
        assert row_b.height == 1
        assert row_b["taux_cta_appliques"].item().to_list() == [21.93]

    def test_filtre_trimestre_exclusif(self, df_facturation, df_pdl, regles_cta_synthetiques):
        """Filtrer sur 2026-T1 ne doit pas inclure PDL B (période 2021)."""
        result = pipeline_cta(
            df_facturation,
            df_pdl,
            trimestre="2026-T1",
            regles=regles_cta_synthetiques,
        ).collect()
        assert "B" not in result["pdl"].to_list()

    def test_sans_filtre_tous_trimestres(self, df_facturation, df_pdl, regles_cta_synthetiques):
        result = pipeline_cta(df_facturation, df_pdl, regles=regles_cta_synthetiques).collect()
        assert set(result["pdl"].to_list()) == {"A", "B"}

    def test_sort_cta_descending(self, df_facturation, df_pdl, regles_cta_synthetiques):
        result = pipeline_cta(df_facturation, df_pdl, regles=regles_cta_synthetiques).collect()
        cta_values = result["cta"].to_list()
        assert cta_values == sorted(cta_values, reverse=True)


# =============================================================================
# CAS LIMITES
# =============================================================================


class TestCasLimites:
    """Vérifie que les cas dégénérés ne cassent pas le pipeline."""

    def test_df_facturation_vide(self, regles_cta_synthetiques):
        df_fact = pl.DataFrame(
            schema={
                "pdl": pl.Utf8,
                "debut": pl.Datetime(time_zone="Europe/Paris"),
                "turpe_fixe_eur": pl.Float64,
            }
        )
        df_pdl = pl.DataFrame({"pdl": ["A"], "order_name": ["SO-A"]})
        result = pipeline_cta(df_fact, df_pdl, regles=regles_cta_synthetiques).collect()
        assert result.height == 0

    def test_pdl_absent_de_df_pdl_exclu(self, regles_cta_synthetiques):
        """Un PDL présent en facturation mais absent du mapping Odoo doit être filtré (inner join)."""
        df_fact = pl.DataFrame(
            [
                {"pdl": "A", "debut": datetime(2025, 1, 1), "turpe_fixe_eur": 100.0},
                {"pdl": "ORPHAN", "debut": datetime(2025, 1, 1), "turpe_fixe_eur": 100.0},
            ]
        ).with_columns(pl.col("debut").dt.replace_time_zone("Europe/Paris"))
        df_pdl = pl.DataFrame({"pdl": ["A"], "order_name": ["SO-A"]})
        result = pipeline_cta(df_fact, df_pdl, regles=regles_cta_synthetiques).collect()
        assert set(result["pdl"].to_list()) == {"A"}
