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
        }
    )


@pytest.fixture
def lf_fact_mensuelle_minimal() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "ref_situation_contractuelle": ["RSC001"],
            "debut": [datetime(2025, 1, 1)],
            "energie_hp_kwh": [123.45],
            "energie_hc_kwh": [0.0],
            "energie_base_kwh": [0.0],
            "nb_jours": [31],
            "memo_puissance": [""],
        }
    ).with_columns(pl.col("debut").dt.replace_time_zone("Europe/Paris"))


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
def service_loaders_mockes(monkeypatch, df_lignes_odoo_minimal, lf_fact_mensuelle_minimal, df_flux_vide):
    """Patch tous les loaders et l'orchestration utilisés par le service.

    `generer_facturation_xlsx` et `generer_documents_facturation` chargent
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

    monkeypatch.setattr(facturation_service, "OdooReader", _OdooReaderMock)
    monkeypatch.setattr(
        facturation_service, "lignes_a_facturer", lambda odoo: _QueryMock(df_lignes_odoo_minimal)
    )
    monkeypatch.setattr(facturation_service, "c15", lambda: _QueryMock(df_flux_vide))
    monkeypatch.setattr(facturation_service, "f15", lambda: _QueryMock(df_flux_vide))
    monkeypatch.setattr(facturation_service, "releves_harmonises", lambda: _QueryMock(df_flux_vide))

    def _fake_facturation(historique, releves):
        return (None, None, None, lf_fact_mensuelle_minimal.collect())

    monkeypatch.setattr(facturation_service, "facturation", _fake_facturation)

    # pydantic Settings interdit setattr — on patche la méthode sur la classe.
    settings_cls = type(facturation_service.settings)
    monkeypatch.setattr(settings_cls, "get_odoo_config", lambda self: {})


def test_generer_facturation_xlsx_smoke(service_loaders_mockes):
    """generer_facturation_xlsx s'exécute sans NameError et produit un XLSX valide."""
    xlsx_bytes = facturation_service.generer_facturation_xlsx(mois="2025-01-01")

    assert isinstance(xlsx_bytes, bytes)
    assert xlsx_bytes[:2] == b"PK"  # signature ZIP / XLSX


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


def test_generer_facturation_arrow_smoke(service_loaders_mockes):
    """generer_facturation_arrow s'exécute et retourne un flux Arrow IPC valide."""
    arrow_bytes = facturation_service.generer_facturation_arrow(mois="2025-01-01")

    df = pl.read_ipc_stream(io.BytesIO(arrow_bytes))
    assert len(df) == 1  # une ligne du fixture
    assert df["quantite_enedis"].item() == 123.45
