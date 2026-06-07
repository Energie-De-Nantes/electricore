"""Tests smoke de l'orchestration facturation + du wrapper ZIP documents.

Depuis #77 :

- Les fonctions `generer_facturation_{rapport,detail}_*` n'existent plus côté
  service — leurs smoke tests sont couverts par les tests d'endpoint
  (`test_facturation_arrow_endpoint.py`).
- Le seul wrapper restant côté service est `generer_documents_facturation`
  (consommé par `/facturation/documents`, à migrer dans la slice #78).
- Les invariants de délégation à `charger_contexte_facturation` (issue #17)
  sont conservés ici en ciblant directement l'orchestration.
"""

import io
import zipfile
from datetime import datetime

import polars as pl
import pytest

from electricore.api.services import facturation_service
from electricore.core.loaders.contexte_mensuel import ContexteFacturation
from electricore.integrations.odoo import facturation as facturation_orchestration


@pytest.fixture
def df_lignes_odoo_minimal() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "x_ref_situation_contractuelle": ["RSC001"],
            "invoice_line_ids": [101],
            "x_pdl": ["12345678901234"],
            "x_lisse": [False],
            "name_account_move": ["INV/2025/0001"],
            "name_product_category": ["HP"],
            "name_product_product": ["Énergie HP"],
            "quantity": [100.0],
            "a_facturer": [True],
            "a_supprimer": [False],
        }
    )


@pytest.fixture
def lf_fact_mensuelle_minimal() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "ref_situation_contractuelle": ["RSC001"],
            "pdl": ["12345678901234"],
            "debut": [datetime(2025, 1, 1)],
            "fin": [datetime(2025, 2, 1)],
            "energie_hp_kwh": [123.45],
            "energie_hc_kwh": [0.0],
            "energie_base_kwh": [0.0],
            "nb_jours": [31],
            "turpe_fixe_eur": [12.34],
            "turpe_variable_eur": [56.78],
            "data_complete": [True],
            "memo_puissance": [""],
        }
    ).with_columns(
        pl.col("debut").dt.replace_time_zone("Europe/Paris"),
        pl.col("fin").dt.replace_time_zone("Europe/Paris"),
    )


@pytest.fixture
def lf_historique_minimal() -> pl.LazyFrame:
    """Historique mock minimal pour rapprocher_facturation_mensuelle."""
    return pl.LazyFrame(
        {
            "ref_situation_contractuelle": ["RSC001"],
            "num_compteur": ["12345678"],
            "type_compteur": ["LINKY"],
        }
    )


@pytest.fixture
def df_flux_vide() -> pl.DataFrame:
    """DataFrame vide qui mime un flux Enedis (F15 ou C15)."""
    return pl.DataFrame(
        schema={
            "pdl": pl.Utf8,
            "date_facture": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "date_evenement": pl.Datetime(time_unit="us", time_zone="Europe/Paris"),
            "unite": pl.Utf8,
            "evenement_declencheur": pl.Utf8,
        }
    )


class _OdooReaderMock:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _QueryMock:
    def __init__(self, df: pl.DataFrame | pl.LazyFrame):
        self._df = df

    def collect(self) -> pl.DataFrame:
        return self._df.collect() if isinstance(self._df, pl.LazyFrame) else self._df

    def lazy(self) -> pl.LazyFrame:
        return self._df if isinstance(self._df, pl.LazyFrame) else self._df.lazy()

    def filter(self, *args, **kwargs):
        return _QueryMock(self.lazy().filter(*args, **kwargs))


@pytest.fixture
def orchestration_mockee(
    monkeypatch, df_lignes_odoo_minimal, lf_fact_mensuelle_minimal, lf_historique_minimal, df_flux_vide
):
    """Patche tout ce qui parle au monde extérieur pour `documents_facturation_du_mois`."""
    from electricore.integrations.odoo import decorators as odoo_decorators

    monkeypatch.setattr(odoo_decorators, "OdooReader", _OdooReaderMock)
    monkeypatch.setattr(
        facturation_orchestration, "lignes_factures_du_mois", lambda odoo, mois: _QueryMock(df_lignes_odoo_minimal)
    )
    monkeypatch.setattr(facturation_orchestration, "c15", lambda: _QueryMock(df_flux_vide))
    monkeypatch.setattr(facturation_orchestration, "f15", lambda: _QueryMock(df_flux_vide))

    def _fake_charger_contexte(mois):
        return ContexteFacturation(
            mois=mois or "2025-01-01",
            historique_enrichi=lf_historique_minimal,
            facturation_mensuelle=lf_fact_mensuelle_minimal.collect(),
        )

    monkeypatch.setattr(facturation_orchestration, "charger_contexte_facturation", _fake_charger_contexte)
    settings_cls = type(odoo_decorators.settings)
    monkeypatch.setattr(settings_cls, "get_odoo_config", lambda self: {})


def test_generer_documents_facturation_smoke(orchestration_mockee):
    """`generer_documents_facturation` produit un ZIP avec les 6 fichiers attendus."""
    zip_bytes, suffix = facturation_service.generer_documents_facturation(mois="2025-01-01")

    assert isinstance(zip_bytes, bytes)
    assert suffix == "2025-01"

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        noms = set(zf.namelist())
    assert noms == {
        "f15_complet.csv",
        "f15_prestas.csv",
        "c15_complet.csv",
        "c15_sorties.csv",
        "reconciliation.csv",
        "changements_puissance.csv",
    }


def test_facturation_du_mois_delegue_a_charger_contexte_facturation(
    monkeypatch, df_lignes_odoo_minimal, lf_fact_mensuelle_minimal, lf_historique_minimal
):
    """`facturation_du_mois` consomme `ContexteFacturation` (issue #17 + #39, ADR-0016).

    Invariant : l'orchestration délègue à `charger_contexte_facturation`
    plutôt que de reconstruire la trio `c15 + releves + facturation()`.
    """
    contexte_prefab = ContexteFacturation(
        mois="2025-01-01",
        historique_enrichi=lf_historique_minimal,
        facturation_mensuelle=lf_fact_mensuelle_minimal.collect(),
    )
    appels_charger: list[str | None] = []

    def _capture_charger(mois):
        appels_charger.append(mois)
        return contexte_prefab

    monkeypatch.setattr(facturation_orchestration, "charger_contexte_facturation", _capture_charger)
    monkeypatch.setattr(
        facturation_orchestration, "lignes_factures_du_mois", lambda odoo, mois: _QueryMock(df_lignes_odoo_minimal)
    )

    result = facturation_orchestration.facturation_du_mois(odoo=None, mois="2025-01-01")

    assert appels_charger == ["2025-01-01"], "charger_contexte_facturation doit être appelé avec le mois fourni"
    assert isinstance(result, pl.DataFrame)


def test_documents_facturation_du_mois_delegue_a_charger_contexte_facturation(
    orchestration_mockee, monkeypatch, lf_historique_minimal, lf_fact_mensuelle_minimal
):
    """`documents_facturation_du_mois` consomme aussi `ContexteFacturation` (issue #17 + #39)."""
    contexte_prefab = ContexteFacturation(
        mois="2025-01-01",
        historique_enrichi=lf_historique_minimal,
        facturation_mensuelle=lf_fact_mensuelle_minimal.collect(),
    )
    appels_charger: list[str | None] = []

    def _capture_charger(mois):
        appels_charger.append(mois)
        return contexte_prefab

    monkeypatch.setattr(facturation_orchestration, "charger_contexte_facturation", _capture_charger)

    facturation_service.generer_documents_facturation(mois="2025-01-01")

    assert appels_charger == ["2025-01-01"], "charger_contexte_facturation doit être appelé avec le mois fourni"
