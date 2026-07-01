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
    # Mart de relevés canonique : table SANS préfixe `flux_`, date métier `date_releve`.
    conn.execute("CREATE TABLE flux_enedis.releves (pdl VARCHAR, source VARCHAR, date_releve TIMESTAMP)")
    conn.execute(
        "INSERT INTO flux_enedis.releves VALUES "
        "('pdl1', 'flux_R151', '2026-01-15 00:00:00'), ('pdl2', 'flux_C15', '2026-02-01 00:00:00')"
    )
    # #537 : c12 (date_evenement, comme c15), r67 (fin — fin de période mesurée, pas
    # date_creation qui est de la métadonnée de production), affaires (jalon_date_heure).
    conn.execute("CREATE TABLE flux_enedis.flux_c12 (pdl VARCHAR, date_evenement TIMESTAMP)")
    conn.execute("INSERT INTO flux_enedis.flux_c12 VALUES ('pdl1', '2026-03-10 00:00:00')")
    conn.execute("CREATE TABLE flux_enedis.flux_r67 (pdl VARCHAR, fin DATE, date_creation TIMESTAMP)")
    conn.execute("INSERT INTO flux_enedis.flux_r67 VALUES ('pdl1', '2026-04-01', '2026-06-30 00:00:00')")
    conn.execute("CREATE TABLE flux_enedis.flux_affaires (pdl VARCHAR, jalon_date_heure TIMESTAMPTZ)")
    conn.execute("INSERT INTO flux_enedis.flux_affaires VALUES ('pdl1', '2026-05-20 00:00:00+02')")
    conn.close()
    monkeypatch.setenv("DUCKDB__PATH", str(base))
    return base


def test_get_table_info_expose_la_derniere_date_metier(base_flux):
    info = duckdb_service.get_table_info("c15")

    assert info["derniere_date"] == "2026-06-01"
    assert info["count"] == 2


def test_table_sans_colonne_date_connue_reste_servie(base_flux):
    info = duckdb_service.get_table_info("inconnue")

    assert info["derniere_date"] is None
    assert info["count"] == 0


def test_get_table_info_rejette_un_nom_porteur_dinjection(base_flux):
    """Défense en profondeur (#428) : un nom dont l'identifiant physique sort de
    `^[a-z0-9_]+$` est rejeté (`ValueError`) *avant* tout SQL — les positions
    d'identifiant non-bindables (`COUNT(*)` / `max()`) ne peuvent pas porter d'injection."""
    with pytest.raises(ValueError):
        duckdb_service.get_table_info("c15'; DROP TABLE flux_c15; --")


def test_get_table_info_expose_la_derniere_date_pour_c12(base_flux):
    """c12 → date_evenement, comme c15 (#537)."""
    info = duckdb_service.get_table_info("c12")
    assert info["derniere_date"] == "2026-03-10"


def test_get_table_info_expose_la_derniere_date_pour_r67(base_flux):
    """r67 → fin (fin de période mesurée), pas date_creation (métadonnée de
    production, #537)."""
    info = duckdb_service.get_table_info("r67")
    assert info["derniere_date"] == "2026-04-01"


def test_get_table_info_expose_la_derniere_date_pour_affaires(base_flux):
    """affaires → jalon_date_heure, le dernier jalon (#537)."""
    info = duckdb_service.get_table_info("affaires")
    assert info["derniere_date"] == "2026-05-20"


def test_get_table_info_pour_mart_sans_prefixe_flux(base_flux):
    """Le mart `releves` (table sans préfixe `flux_`, #264) est interrogeable.

    `prefix=""` adresse la table physique telle quelle ; `date_column` désigne
    explicitement la colonne de date métier (le mart n'est pas dans COLONNE_DATE_METIER).
    """
    info = duckdb_service.get_table_info("releves", prefix="", date_column="date_releve")

    assert info["table"] == "releves"
    assert info["count"] == 2
    assert info["derniere_date"] == "2026-02-01"
    assert {"name": "date_releve", "type": "TIMESTAMP"} in info["columns"]
