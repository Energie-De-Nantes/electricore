"""
Tests unitaires pour le module de sélection des taux en vigueur.

Couvre `ajouter_taux_en_vigueur` qui attache à chaque ligne d'un DataFrame
le taux réglementé applicable à sa date, sur la base d'un historique
versionné par date d'entrée en vigueur.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pandera as pa_errors
import polars as pl
import pytest

from electricore.core.pipelines.taux import ajouter_taux_en_vigueur

TZ = ZoneInfo("Europe/Paris")


def _historique(rows: list[tuple[datetime, float]], taux_col: str = "taux") -> pl.LazyFrame:
    """Construit un historique de taux (start, taux_col) avec TZ Europe/Paris."""
    return pl.LazyFrame(
        {
            "start": [d for d, _ in rows],
            taux_col: [t for _, t in rows],
        }
    ).with_columns(pl.col("start").dt.replace_time_zone("Europe/Paris"))


def _df(dates: list[datetime]) -> pl.LazyFrame:
    """Construit un df d'entrée avec une colonne `date` TZ Europe/Paris."""
    return pl.LazyFrame({"date": dates}).with_columns(pl.col("date").dt.replace_time_zone("Europe/Paris"))


class TestAttacheTaux:
    """Cas nominaux d'attachement du taux."""

    def test_date_exactement_sur_start_attache_le_taux(self):
        """Borne inférieure incluse : une ligne datée du jour exact d'entrée en vigueur prend ce taux."""
        historique = _historique([(datetime(2024, 1, 1), 10.0)])
        df = _df([datetime(2024, 1, 1)])

        result = ajouter_taux_en_vigueur(df, historique, date_col="date", taux_col="taux").collect()

        assert result["taux"].to_list() == [10.0]

    def test_changement_de_taux_chaque_ligne_recoit_le_bon(self):
        """Avec deux taux successifs, chaque ligne reçoit celui en vigueur à sa date."""
        historique = _historique(
            [
                (datetime(2024, 1, 1), 10.0),
                (datetime(2024, 6, 1), 20.0),
            ]
        )
        df = _df(
            [
                datetime(2024, 3, 15),  # avant changement
                datetime(2024, 6, 1),  # jour du changement (borne inclusive)
                datetime(2024, 8, 1),  # après changement
            ]
        )

        result = ajouter_taux_en_vigueur(df, historique, date_col="date", taux_col="taux").collect().sort("date")

        assert result["taux"].to_list() == [10.0, 20.0, 20.0]

    def test_derniere_regle_setend_indefiniment(self):
        """La règle la plus récente reste en vigueur — pas de plafond temporel."""
        historique = _historique([(datetime(2024, 1, 1), 10.0)])
        df = _df([datetime(2099, 12, 31)])  # bien au-delà du dernier start

        result = ajouter_taux_en_vigueur(df, historique, date_col="date", taux_col="taux").collect()

        assert result["taux"].to_list() == [10.0]


class TestCouvertureIncomplete:
    """Erreurs déclenchées par un historique qui ne couvre pas toutes les dates."""

    def test_date_anterieure_au_premier_start_leve_valueerror(self):
        """Une ligne datée avant l'entrée en vigueur de la première règle n'a pas de taux applicable."""
        historique = _historique([(datetime(2024, 1, 1), 10.0)])
        df = _df([datetime(2023, 6, 1)])  # avant le premier start

        with pytest.raises(ValueError, match="sans taux en vigueur"):
            ajouter_taux_en_vigueur(df, historique, date_col="date", taux_col="taux").collect()


class TestInvariances:
    """Propriétés que la fonction doit garantir indépendamment de l'ordre des entrées."""

    def test_historique_non_trie_donne_meme_resultat(self):
        """L'ordre des lignes dans `historique` ne doit pas affecter le résultat."""
        historique_trie = _historique(
            [
                (datetime(2024, 1, 1), 10.0),
                (datetime(2024, 6, 1), 20.0),
                (datetime(2025, 1, 1), 30.0),
            ]
        )
        historique_melange = _historique(
            [
                (datetime(2025, 1, 1), 30.0),
                (datetime(2024, 1, 1), 10.0),
                (datetime(2024, 6, 1), 20.0),
            ]
        )
        df = _df(
            [
                datetime(2024, 3, 1),
                datetime(2024, 8, 1),
                datetime(2025, 5, 1),
            ]
        )

        result_trie = ajouter_taux_en_vigueur(df, historique_trie, date_col="date", taux_col="taux").collect()
        result_melange = ajouter_taux_en_vigueur(df, historique_melange, date_col="date", taux_col="taux").collect()

        assert result_trie.equals(result_melange)
        assert result_trie["taux"].to_list() == [10.0, 20.0, 30.0]


class TestSchemaHistorique:
    """Validation Pandera de l'historique à l'entrée."""

    def test_historique_sans_colonne_start_est_rejete(self):
        """Un historique sans `start` doit déclencher une erreur de schéma claire."""
        historique_invalide = pl.LazyFrame({"taux": [10.0]}).with_columns(pl.col("taux").cast(pl.Float64))
        df = _df([datetime(2024, 1, 1)])

        with pytest.raises(pa_errors.errors.SchemaError):
            ajouter_taux_en_vigueur(df, historique_invalide, date_col="date", taux_col="taux").collect()

    def test_historique_avec_taux_int_au_lieu_de_float_est_rejete(self):
        """Le taux doit être Float64 — un Int64 doit être rejeté plutôt que coercé silencieusement."""
        historique_invalide = pl.LazyFrame(
            {
                "start": [datetime(2024, 1, 1)],
                "taux": [10],  # Int64 par défaut
            }
        ).with_columns(pl.col("start").dt.replace_time_zone("Europe/Paris"))
        df = _df([datetime(2024, 6, 1)])

        with pytest.raises(pa_errors.errors.SchemaError):
            ajouter_taux_en_vigueur(df, historique_invalide, date_col="date", taux_col="taux").collect()
