"""Tests smoke des générateurs facturation côté service.

Couvre les régressions du type `NameError`/`ImportError` à l'intérieur des
fonctions du service — celles qui ne déclenchent pas à l'import, seulement
lors d'un appel réel. Vu en local : oubli d'une constante après refactor.
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


@pytest.fixture
def service_loaders_mockes(
    monkeypatch, df_lignes_odoo_minimal, lf_fact_mensuelle_minimal, lf_historique_minimal, df_flux_vide
):
    """Patch tous les loaders et l'orchestration utilisés par le service.

    `generer_facturation_*_xlsx` et `generer_documents_facturation` chargent
    Odoo + DuckDB + lancent la pipeline facturation. On stub tout ce qui
    parle au monde extérieur pour pouvoir exercer la logique du service
    sans dépendances.
    """

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

    # Depuis #67 : `OdooReader` est ouvert par le décorateur `@with_odoo`
    # qui vit dans `electricore.integrations.odoo.decorators`.
    from electricore.integrations.odoo import decorators as odoo_decorators

    monkeypatch.setattr(odoo_decorators, "OdooReader", _OdooReaderMock)

    # Depuis #39 (ADR-0016) : l'orchestration vit dans `integrations.odoo.facturation`,
    # donc les mocks ciblent ce module-là plutôt que le service.
    monkeypatch.setattr(
        facturation_orchestration, "lignes_factures_du_mois", lambda odoo, mois: _QueryMock(df_lignes_odoo_minimal)
    )
    monkeypatch.setattr(facturation_orchestration, "c15", lambda: _QueryMock(df_flux_vide))
    monkeypatch.setattr(facturation_orchestration, "f15", lambda: _QueryMock(df_flux_vide))

    # ContexteFacturation est résolu par l'orchestration (et plus par le service).
    def _fake_charger_contexte(mois):
        return ContexteFacturation(
            mois=mois or "2025-01-01",
            historique_enrichi=lf_historique_minimal,
            facturation_mensuelle=lf_fact_mensuelle_minimal.collect(),
        )

    monkeypatch.setattr(facturation_orchestration, "charger_contexte_facturation", _fake_charger_contexte)

    # pydantic Settings interdit setattr — on patche la méthode sur la classe.
    settings_cls = type(odoo_decorators.settings)
    monkeypatch.setattr(settings_cls, "get_odoo_config", lambda self: {})


def test_generer_facturation_rapport_xlsx_smoke(service_loaders_mockes):
    """generer_facturation_rapport_xlsx s'exécute sans NameError et produit un XLSX valide."""
    xlsx_bytes = facturation_service.generer_facturation_rapport_xlsx(mois="2025-01-01")

    assert isinstance(xlsx_bytes, bytes)
    assert xlsx_bytes[:2] == b"PK"  # signature ZIP / XLSX


def test_generer_facturation_detail_xlsx_smoke(service_loaders_mockes):
    """generer_facturation_detail_xlsx s'exécute et produit un XLSX mono-onglet valide."""
    xlsx_bytes = facturation_service.generer_facturation_detail_xlsx(mois="2025-01-01")

    assert isinstance(xlsx_bytes, bytes)
    assert xlsx_bytes[:2] == b"PK"


def test_generer_documents_facturation_smoke(service_loaders_mockes):
    """generer_documents_facturation s'exécute sans NameError et produit un ZIP avec les 6 fichiers attendus."""
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


def test_generer_facturation_detail_arrow_smoke(service_loaders_mockes):
    """generer_facturation_detail_arrow s'exécute et retourne un flux Arrow IPC valide."""
    arrow_bytes = facturation_service.generer_facturation_detail_arrow(mois="2025-01-01")

    df = pl.read_ipc_stream(io.BytesIO(arrow_bytes))
    assert len(df) == 1  # une ligne du fixture
    assert df["quantite_enedis"].item() == 123.45


def test_calculer_lignes_facture_rapprochees_delegue_a_charger_contexte_facturation(
    monkeypatch, df_lignes_odoo_minimal, lf_fact_mensuelle_minimal, lf_historique_minimal
):
    """`facturation_du_mois` consomme `ContexteFacturation` (issue #17 + #39, ADR-0016).

    Depuis #39, l'orchestration vit dans `integrations.odoo.facturation`. Le test vérifie
    que la chaîne service → orchestration délègue toujours à `charger_contexte_facturation`
    plutôt que de reconstruire la trio `c15 + releves + facturation()`.
    """
    contexte_prefab = ContexteFacturation(
        mois="2025-01-01",
        historique_enrichi=lf_historique_minimal,
        facturation_mensuelle=lf_fact_mensuelle_minimal.collect(),
    )
    appels_charger = []

    def _capture_charger(mois):
        appels_charger.append(mois)
        return contexte_prefab

    monkeypatch.setattr(facturation_orchestration, "charger_contexte_facturation", _capture_charger)

    class _OdooReaderMock:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class _QueryMock:
        def __init__(self, df):
            self._df = df

        def collect(self):
            return self._df

    from electricore.integrations.odoo import decorators as odoo_decorators

    monkeypatch.setattr(odoo_decorators, "OdooReader", _OdooReaderMock)
    monkeypatch.setattr(
        facturation_orchestration, "lignes_factures_du_mois", lambda odoo, mois: _QueryMock(df_lignes_odoo_minimal)
    )
    settings_cls = type(odoo_decorators.settings)
    monkeypatch.setattr(settings_cls, "get_odoo_config", lambda self: {})

    result = facturation_service.calculer_lignes_facture_rapprochees(mois="2025-01-01")

    assert appels_charger == ["2025-01-01"], "charger_contexte_facturation doit être appelé avec le mois fourni"
    assert isinstance(result, pl.DataFrame)


def test_generer_documents_facturation_delegue_a_charger_contexte_facturation(
    service_loaders_mockes, monkeypatch, lf_historique_minimal, lf_fact_mensuelle_minimal
):
    """`documents_facturation_du_mois` consomme aussi `ContexteFacturation` (issue #17 + #39).

    Depuis #39, l'orchestration ZIP vit dans `integrations.odoo.facturation`. La fonction
    conserve ses responsabilités spécifiques (F15/C15 du mois) mais ne reconstruit plus
    la trio `c15 + releves + facturation()`.
    """
    contexte_prefab = ContexteFacturation(
        mois="2025-01-01",
        historique_enrichi=lf_historique_minimal,
        facturation_mensuelle=lf_fact_mensuelle_minimal.collect(),
    )
    appels_charger = []

    def _capture_charger(mois):
        appels_charger.append(mois)
        return contexte_prefab

    monkeypatch.setattr(facturation_orchestration, "charger_contexte_facturation", _capture_charger)

    facturation_service.generer_documents_facturation(mois="2025-01-01")

    assert appels_charger == ["2025-01-01"], "charger_contexte_facturation doit être appelé avec le mois fourni"
