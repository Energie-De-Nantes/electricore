"""Le service DuckDB de l'API résout le chemin de la base à l'appel (issue #146).

L'import du module en tête de fichier est volontaire : un chemin figé à
l'import ignorerait le `DUCKDB_PATH` posé ensuite par le test — c'est
précisément le comportement corrigé.
"""

from electricore.api.services import duckdb_service


def test_get_freshness_resout_a_l_appel(tmp_path, monkeypatch):
    """DUCKDB_PATH posé après l'import est honoré ; l'erreur nomme le chemin résolu."""
    base_absente = tmp_path / "absente.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(base_absente))

    payload = duckdb_service.get_freshness()

    assert payload["accessible"] is False
    assert str(base_absente) in payload["error"]
