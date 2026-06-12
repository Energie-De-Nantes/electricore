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
        assert set(regles.columns) == {"start", "taux_cta_pct", "reference"}
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
