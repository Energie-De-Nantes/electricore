"""Tests du chargeur de contexte mensuel de facturation.

`charger_contexte_facturation` est l'unique entry point chargé de matérialiser
les éléments dérivés une seule fois pour un mois donné :
- résolution du mois cible (None → dernier mois disponible)
- exécution de `facturation()` une seule fois
- exposition d'un `ContexteFacturation` immutable consommé par les services
  (facturation, CTA…).

Voir issue #17 et ADR-0014.
"""

from datetime import datetime

import polars as pl
import pytest

from electricore.core.loaders import contexte_mensuel
from electricore.core.loaders.contexte_mensuel import (
    ContexteFacturation,
    charger_contexte_facturation,
)


@pytest.fixture
def lf_historique_enrichi_minimal() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "ref_situation_contractuelle": ["RSC001"],
            "pdl": ["12345678901234"],
            "date_evenement": [datetime(2025, 1, 1)],
        }
    ).with_columns(pl.col("date_evenement").dt.replace_time_zone("Europe/Paris"))


@pytest.fixture
def df_facturation_mensuelle_minimal() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC001"],
            "pdl": ["12345678901234"],
            "debut": [datetime(2025, 1, 1)],
            "fin": [datetime(2025, 2, 1)],
            "energie_hp_kwh": [123.45],
            "nb_jours": [31],
        }
    ).with_columns(
        pl.col("debut").dt.replace_time_zone("Europe/Paris"),
        pl.col("fin").dt.replace_time_zone("Europe/Paris"),
    )


@pytest.fixture
def loaders_mockes(monkeypatch, lf_historique_enrichi_minimal, df_facturation_mensuelle_minimal):
    """Patch les loaders et `facturation()` utilisés par `charger_contexte_facturation`.

    Tests unitaires : on isole la composition, pas le pipeline lui-même.
    """
    monkeypatch.setattr(contexte_mensuel, "c15", lambda: _LazyQueryMock(pl.LazyFrame()))
    monkeypatch.setattr(contexte_mensuel, "releves_harmonises", lambda: _LazyQueryMock(pl.LazyFrame()))

    def _fake_facturation(historique, releves):
        # Retourne un tuple positionnel compatible avec ResultatFacturationPolars :
        # (historique_enrichi, abonnements, energie, facturation_mensuelle).
        return (
            lf_historique_enrichi_minimal,
            pl.LazyFrame(),
            pl.LazyFrame(),
            df_facturation_mensuelle_minimal,
        )

    monkeypatch.setattr(contexte_mensuel, "facturation", _fake_facturation)


class _LazyQueryMock:
    """Mock minimal qui mime l'API `.lazy()` des query builders DuckDB."""

    def __init__(self, lf: pl.LazyFrame):
        self._lf = lf

    def lazy(self) -> pl.LazyFrame:
        return self._lf


def test_charger_contexte_facturation_avec_mois_explicite_retourne_contexte_peuple(
    loaders_mockes, lf_historique_enrichi_minimal, df_facturation_mensuelle_minimal
):
    """Tracer bullet : un mois explicite produit un `ContexteFacturation` complet."""
    contexte = charger_contexte_facturation(mois="2025-01-01")

    assert isinstance(contexte, ContexteFacturation)
    assert contexte.mois == "2025-01-01"
    assert contexte.historique_enrichi is lf_historique_enrichi_minimal
    assert contexte.facturation_mensuelle is df_facturation_mensuelle_minimal


def test_contexte_facturation_est_immutable(loaders_mockes):
    """Les champs du `ContexteFacturation` ne peuvent pas être modifiés après construction.

    Garantit l'invariant : un contexte chargé est figé, aucun service ne
    peut le contaminer pour le suivant.
    """
    from dataclasses import FrozenInstanceError

    contexte = charger_contexte_facturation(mois="2025-01-01")

    with pytest.raises(FrozenInstanceError):
        contexte.mois = "2025-02-01"  # type: ignore[misc]


def test_charger_contexte_facturation_resout_mois_none_au_dernier_mois_disponible(
    monkeypatch, lf_historique_enrichi_minimal
):
    """Quand `mois=None`, le mois est résolu au dernier mois présent dans `facturation_mensuelle`.

    Reproduit la logique précédemment dupliquée dans `facturation_service` :
    `debut.dt.truncate("1mo").dt.date().max()`. Doit s'exécuter avant tout
    appel Odoo (ADR-0014).
    """
    df_multi_mois = pl.DataFrame(
        {
            "ref_situation_contractuelle": ["RSC001", "RSC001", "RSC001"],
            "pdl": ["12345678901234"] * 3,
            "debut": [datetime(2025, 1, 1), datetime(2025, 2, 15), datetime(2025, 3, 1)],
            "fin": [datetime(2025, 2, 1), datetime(2025, 3, 1), datetime(2025, 4, 1)],
            "energie_hp_kwh": [10.0, 20.0, 30.0],
        }
    ).with_columns(
        pl.col("debut").dt.replace_time_zone("Europe/Paris"),
        pl.col("fin").dt.replace_time_zone("Europe/Paris"),
    )

    monkeypatch.setattr(contexte_mensuel, "c15", lambda: _LazyQueryMock(pl.LazyFrame()))
    monkeypatch.setattr(contexte_mensuel, "releves_harmonises", lambda: _LazyQueryMock(pl.LazyFrame()))
    monkeypatch.setattr(
        contexte_mensuel,
        "facturation",
        lambda historique, releves: (lf_historique_enrichi_minimal, pl.LazyFrame(), pl.LazyFrame(), df_multi_mois),
    )

    contexte = charger_contexte_facturation(mois=None)

    assert contexte.mois == "2025-03-01"
