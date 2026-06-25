"""Contrat de filtres du mart `releves` (#265).

Teste `_load_releves_df` contre une base DuckDB réelle (table `flux_enedis.releves`
sans préfixe). Chaque filtre est mappé à UNE colonne câblée en dur côté route
(`prm`→`pdl`, `source`→`source`, `debut`/`fin`→`date_releve`) ; aucune colonne
fournie par le client n'atteint le SQL, et les valeurs sont liées en paramètres.
"""

import duckdb
import pytest

from electricore.api.routers.releves import _load_releves_df

INDEX_COLS = (
    "index_base_kwh",
    "index_hp_kwh",
    "index_hc_kwh",
    "index_hph_kwh",
    "index_hpb_kwh",
    "index_hch_kwh",
    "index_hcb_kwh",
)


@pytest.fixture
def base_releves(tmp_path, monkeypatch):
    """Base DuckDB avec un mart `releves` minimal (3 lignes, sources/dates variées)."""
    base = tmp_path / "flux.duckdb"
    conn = duckdb.connect(str(base))
    conn.execute("CREATE SCHEMA flux_enedis")
    cols_index = ", ".join(f"{c} BIGINT" for c in INDEX_COLS)
    conn.execute(f"CREATE TABLE flux_enedis.releves (pdl VARCHAR, source VARCHAR, date_releve TIMESTAMP, {cols_index})")
    zeros = ", ".join(["0"] * len(INDEX_COLS))
    conn.execute(
        f"INSERT INTO flux_enedis.releves VALUES "
        f"('pdl1', 'flux_R151', '2025-01-10 00:00:00', {zeros}), "
        f"('pdl1', 'flux_C15',  '2025-02-20 00:00:00', {zeros}), "
        f"('pdl2', 'flux_R64',  '2025-03-15 00:00:00', {zeros})"
    )
    conn.close()
    monkeypatch.setenv("DUCKDB__PATH", str(base))
    return base


def test_filtre_prm_restreint_au_pdl(base_releves):
    """`prm` filtre sur la colonne `pdl`."""
    df = _load_releves_df(prm="pdl1", source=None, debut=None, fin=None, limit=100)

    assert df.height == 2
    assert set(df["pdl"].to_list()) == {"pdl1"}


def test_filtre_source_restreint_a_la_source(base_releves):
    """`source` filtre sur la colonne `source`."""
    df = _load_releves_df(prm=None, source="flux_R151", debut=None, fin=None, limit=100)

    assert df.height == 1
    assert df["source"].to_list() == ["flux_R151"]


def test_filtre_debut_borne_basse_inclusive(base_releves):
    """`debut` filtre `date_releve >= debut` (borne incluse)."""
    df = _load_releves_df(prm=None, source=None, debut="2025-02-20", fin=None, limit=100)

    # 2025-02-20 (inclus) et 2025-03-15 → 2 lignes ; 2025-01-10 exclue
    dates = sorted(d.date().isoformat() for d in df["date_releve"].to_list())
    assert dates == ["2025-02-20", "2025-03-15"]


def test_filtre_fin_borne_haute_inclusive(base_releves):
    """`fin` filtre `date_releve <= fin` (borne incluse)."""
    df = _load_releves_df(prm=None, source=None, debut=None, fin="2025-02-20", limit=100)

    # 2025-01-10 et 2025-02-20 (inclus) → 2 lignes ; 2025-03-15 exclue
    dates = sorted(d.date().isoformat() for d in df["date_releve"].to_list())
    assert dates == ["2025-01-10", "2025-02-20"]


def test_filtres_combines_sont_intersectes(base_releves):
    """Plusieurs filtres se cumulent en ET logique."""
    df = _load_releves_df(prm="pdl1", source=None, debut="2025-02-01", fin="2025-02-28", limit=100)

    # pdl1 ∩ fenêtre févr. 2025 → seule la ligne flux_C15 du 2025-02-20
    assert df.height == 1
    assert df["pdl"].to_list() == ["pdl1"]
    assert df["source"].to_list() == ["flux_C15"]


def test_aucun_filtre_retourne_tout_le_mart(base_releves):
    """Sans filtre, tout le mart est servi (borne `limit`)."""
    df = _load_releves_df(limit=100)

    assert df.height == 3


def test_valeur_prm_est_liee_en_parametre_pas_injectee(base_releves):
    """Une valeur `prm` à forme d'injection SQL est liée, pas interprétée.

    Si la valeur était interpolée, `' OR '1'='1` ramènerait tout le mart ;
    liée en paramètre, elle ne matche aucun `pdl` → 0 ligne.
    """
    df = _load_releves_df(prm="pdl1' OR '1'='1", source=None, debut=None, fin=None, limit=100)

    assert df.height == 0
