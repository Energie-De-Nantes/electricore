"""La fraîcheur des données par table flux (#158).

`get_table_info` expose `derniere_date` : le max de la colonne de date métier
(date de relevé / d'événement / de facture selon le flux), pas la date
d'ingestion technique. Répond à « les données d'hier sont-elles là ? ».
"""

import duckdb
import pytest

from electricore.api.services import duckdb_service


@pytest.fixture
def base_flux(tmp_path, monkeypatch):
    base = tmp_path / "flux.duckdb"
    conn = duckdb.connect(str(base))
    conn.execute("CREATE SCHEMA flux_enedis")
    conn.execute("CREATE TABLE flux_enedis.flux_c15 (pdl VARCHAR, date_evenement TIMESTAMP)")
    conn.execute(
        "INSERT INTO flux_enedis.flux_c15 VALUES ('pdl1', '2026-05-15 00:00:00'), ('pdl2', '2026-06-01 00:00:00')"
    )
    conn.execute("CREATE TABLE flux_enedis.flux_inconnue (pdl VARCHAR)")
    conn.close()
    monkeypatch.setenv("DUCKDB_PATH", str(base))
    return base


def test_get_table_info_expose_la_derniere_date_metier(base_flux):
    info = duckdb_service.get_table_info("c15")

    assert info["derniere_date"] == "2026-06-01"
    assert info["count"] == 2


def test_table_sans_colonne_date_connue_reste_servie(base_flux):
    info = duckdb_service.get_table_info("inconnue")

    assert info["derniere_date"] is None
    assert info["count"] == 0
