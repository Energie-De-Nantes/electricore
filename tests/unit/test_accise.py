"""
Tests unitaires pour le pipeline Accise.

Couvre le chargement des règles, le calcul du montant d'accise par mois de
consommation, et l'agrégation par PDL via `pipeline_accise`.

La sélection du taux en vigueur à la date est testée séparément dans
`test_taux.py` — ici on vérifie le wire-through Accise spécifique : conversion
du `mois_consommation` (string) en datetime, formule `× / 1000` et arrondi.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from electricore.core.pipelines.accise import (
    ajouter_accise,
    load_accise_rules,
    pipeline_accise,
)

TZ = ZoneInfo("Europe/Paris")


@pytest.fixture
def regles_accise_synthetiques() -> pl.LazyFrame:
    """
    Historique Accise synthétique avec un changement de taux mi-2025.

    Mimique la structure du CSV de prod pour isoler les tests de la valeur
    exacte du fichier.
    """
    return pl.LazyFrame(
        {
            "start": [
                datetime(2024, 1, 1, tzinfo=TZ),
                datetime(2025, 7, 1, tzinfo=TZ),
            ],
            "taux_accise_eur_mwh": [21.0, 33.7],
        }
    )


def _consommations(rows: list[dict]) -> pl.LazyFrame:
    """Construit un LazyFrame de consommations mensuelles."""
    return pl.LazyFrame(rows)


class TestChargementRegles:
    """Valide le chargement de electricore/config/accise_rules.csv."""

    def test_load_accise_rules_colonnes_et_types(self):
        regles = load_accise_rules().collect()
        assert set(regles.columns) == {"start", "taux_accise_eur_mwh"}
        assert regles["start"].dtype == pl.Datetime(time_unit="us", time_zone="Europe/Paris")
        assert regles["taux_accise_eur_mwh"].dtype == pl.Float64

    def test_load_accise_rules_contient_au_moins_une_ligne(self):
        regles = load_accise_rules().collect()
        assert regles.height >= 1


class TestAjouterAccise:
    """Wire-through Accise : conversion du mois, formule, arrondi."""

    def test_colonnes_ajoutees(self, regles_accise_synthetiques):
        consos = _consommations(
            [{"pdl": "A", "mois_consommation": "2024-06", "energie_kwh": 1000.0}]
        )
        result = ajouter_accise(consos, regles_accise_synthetiques).collect()
        assert "taux_accise_eur_mwh" in result.columns
        assert "energie_mwh" in result.columns
        assert "accise_eur" in result.columns

    def test_conversion_mwh(self, regles_accise_synthetiques):
        """energie_kwh doit être converti en MWh (÷ 1000)."""
        consos = _consommations(
            [{"pdl": "A", "mois_consommation": "2024-06", "energie_kwh": 2500.0}]
        )
        result = ajouter_accise(consos, regles_accise_synthetiques).collect()
        assert result["energie_mwh"].item() == 2.5

    def test_calcul_accise_eur(self, regles_accise_synthetiques):
        """1 MWh × 21 €/MWh = 21.00 €."""
        consos = _consommations(
            [{"pdl": "A", "mois_consommation": "2024-06", "energie_kwh": 1000.0}]
        )
        result = ajouter_accise(consos, regles_accise_synthetiques).collect()
        assert result["accise_eur"].item() == 21.0

    def test_arrondi_2_decimales(self, regles_accise_synthetiques):
        """1234 kWh × 33.7 €/MWh = 41.5858 € → arrondi 41.59 €."""
        consos = _consommations(
            [{"pdl": "A", "mois_consommation": "2025-09", "energie_kwh": 1234.0}]
        )
        result = ajouter_accise(consos, regles_accise_synthetiques).collect()
        assert result["accise_eur"].item() == pytest.approx(41.59, abs=1e-9)

    def test_colonnes_originales_preservees(self, regles_accise_synthetiques):
        """Les colonnes du DF d'entrée doivent rester accessibles après ajout."""
        consos = _consommations(
            [
                {
                    "pdl": "A",
                    "order_name": "SO1",
                    "trimestre": "2024-T2",
                    "mois_consommation": "2024-06",
                    "energie_kwh": 1000.0,
                }
            ]
        )
        result = ajouter_accise(consos, regles_accise_synthetiques).collect()
        assert "order_name" in result.columns
        assert result["order_name"].item() == "SO1"
        assert result["trimestre"].item() == "2024-T2"

    def test_changement_taux_chaque_ligne_recoit_le_sien(self, regles_accise_synthetiques):
        """Avant juillet 2025 : 21 €/MWh ; à partir de juillet 2025 : 33.7 €/MWh."""
        consos = _consommations(
            [
                {"pdl": "A", "mois_consommation": "2025-06", "energie_kwh": 1000.0},
                {"pdl": "A", "mois_consommation": "2025-07", "energie_kwh": 1000.0},
                {"pdl": "A", "mois_consommation": "2025-08", "energie_kwh": 1000.0},
            ]
        )
        result = (
            ajouter_accise(consos, regles_accise_synthetiques)
            .collect()
            .sort("mois_consommation")
        )
        assert result["taux_accise_eur_mwh"].to_list() == [21.0, 33.7, 33.7]
        assert result["accise_eur"].to_list() == [21.0, 33.7, 33.7]


class TestPipelineAccise:
    """Pipeline complet depuis les lignes Odoo : filtre catégories + agrégation + accise."""

    @pytest.fixture
    def lignes_factures_synthetiques(self) -> pl.LazyFrame:
        """
        Lignes de factures Odoo synthétiques.

        invoice_date est la date de facture (mois M+1 par rapport à la conso).
        Le pipeline filtre sur les catégories d'énergie (Base, HP, HC) et
        agrège par PDL + mois_consommation.
        """
        return pl.LazyFrame(
            [
                # PDL A — juin 2024 (facturé en juillet) : 1000 kWh sur HP+HC
                {
                    "x_pdl": "A",
                    "name": "SO-A",
                    "invoice_date": "2024-07-15",
                    "name_product_category": "HP",
                    "quantity": 600.0,
                },
                {
                    "x_pdl": "A",
                    "name": "SO-A",
                    "invoice_date": "2024-07-15",
                    "name_product_category": "HC",
                    "quantity": 400.0,
                },
                # Ligne hors catégorie d'énergie : doit être filtrée
                {
                    "x_pdl": "A",
                    "name": "SO-A",
                    "invoice_date": "2024-07-15",
                    "name_product_category": "Abonnements",
                    "quantity": 9999.0,
                },
            ]
        )

    def test_filtre_categories_energie(self, lignes_factures_synthetiques, regles_accise_synthetiques):
        """Les lignes "Abonnements" ne doivent pas contribuer à l'énergie."""
        result = pipeline_accise(lignes_factures_synthetiques, regles_accise_synthetiques)
        row = result.filter(pl.col("pdl") == "A").row(0, named=True)
        # 600 + 400 = 1000 kWh, pas 1000 + 9999
        assert row["energie_kwh"] == 1000.0

    def test_agregation_et_accise(self, lignes_factures_synthetiques, regles_accise_synthetiques):
        """1 MWh × 21 €/MWh = 21.00 €."""
        result = pipeline_accise(lignes_factures_synthetiques, regles_accise_synthetiques)
        row = result.filter(pl.col("pdl") == "A").row(0, named=True)
        assert row["mois_consommation"] == "2024-06"
        assert row["energie_mwh"] == 1.0
        assert row["accise_eur"] == 21.0

    def test_lignes_sans_invoice_date_sont_exclues(self, regles_accise_synthetiques):
        """Les lignes de factures draft (sans invoice_date) ne sont pas dans l'assiette.

        Sinon, leur `mois_consommation` est null et le pipeline lève une ValueError
        sur la sélection du taux en vigueur (date null < tout start de l'historique).
        """
        lignes = pl.LazyFrame(
            [
                {"x_pdl": "B", "name": "SO-B", "invoice_date": "2024-07-15", "name_product_category": "Base", "quantity": 500.0},
                # Ligne draft sans invoice_date : doit être ignorée
                {"x_pdl": "B", "name": "SO-B", "invoice_date": None, "name_product_category": "Base", "quantity": 999.0},
            ]
        )

        result = pipeline_accise(lignes, regles_accise_synthetiques)

        assert len(result) == 1
        row = result.row(0, named=True)
        assert row["energie_kwh"] == 500.0  # 999 exclu
