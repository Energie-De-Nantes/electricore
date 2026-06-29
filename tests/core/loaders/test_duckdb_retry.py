"""Tests pour `duckdb_readonly_conn` — context manager avec retry sur lock."""

import logging
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from electricore.core.loaders.duckdb import DuckDBLockError, duckdb_readonly_conn


class TestDuckdbReadonlyConn:
    """Politique : 3 essais × 1s backoff sur lock, propagation immédiate sinon."""

    def test_first_attempt_yields_conn_and_logs_nothing(self, caplog):
        """Connexion réussie au premier essai → pas de warning, conn yieldée."""
        fake_conn = MagicMock(spec=duckdb.DuckDBPyConnection)

        with patch("duckdb.connect", return_value=fake_conn) as mock_connect:
            with caplog.at_level(logging.WARNING, logger="electricore.core.loaders.duckdb.helpers"):
                with duckdb_readonly_conn("/fake/path.duckdb") as conn:
                    assert conn is fake_conn

        mock_connect.assert_called_once_with("/fake/path.duckdb", read_only=True)
        fake_conn.close.assert_called_once()
        assert caplog.records == []

    def test_does_not_exist_propagates_without_retry(self):
        """Erreur 'does not exist' = non récupérable → 1 seul appel, pas de sleep."""
        exc = duckdb.IOException("IO Error: File does not exist")

        with patch("duckdb.connect", side_effect=exc) as mock_connect:
            with patch("time.sleep") as mock_sleep:
                with pytest.raises(duckdb.IOException, match="does not exist"):
                    with duckdb_readonly_conn("/missing.duckdb"):
                        pass  # pragma: no cover

        assert mock_connect.call_count == 1
        mock_sleep.assert_not_called()

    def test_lock_then_success_retries_and_yields(self, caplog):
        """Lock au 1er essai, succès au 2e → 2 connect, 1 sleep, 1 warning."""
        fake_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
        lock_exc = duckdb.IOException("IO Error: Could not set lock on file")

        with patch("duckdb.connect", side_effect=[lock_exc, fake_conn]) as mock_connect:
            with patch("time.sleep") as mock_sleep:
                with caplog.at_level(logging.WARNING, logger="electricore.core.loaders.duckdb.helpers"):
                    with duckdb_readonly_conn("/locked.duckdb") as conn:
                        assert conn is fake_conn

        assert mock_connect.call_count == 2
        mock_sleep.assert_called_once_with(1.0)
        assert len(caplog.records) == 1
        assert "verrouill" in caplog.records[0].message.lower()

    def test_persistent_lock_raises_after_all_attempts(self):
        """Lock à chaque essai → 3 connect, 2 sleep, IOException propagée."""
        lock_exc = duckdb.IOException("IO Error: Conflicting lock is held")

        with patch("duckdb.connect", side_effect=lock_exc) as mock_connect:
            with patch("time.sleep") as mock_sleep:
                with pytest.raises(duckdb.IOException, match="Conflicting lock"):
                    with duckdb_readonly_conn("/locked.duckdb"):
                        pass  # pragma: no cover

        assert mock_connect.call_count == 3
        assert mock_sleep.call_count == 2

    def test_persistent_lock_raises_typed_lock_error(self):
        """Lock persistant → `DuckDBLockError`, que l'API peut convertir en 503
        « ingestion en cours » sans inspecter le message (issue #171)."""
        lock_exc = duckdb.IOException("IO Error: Conflicting lock is held")

        with patch("duckdb.connect", side_effect=lock_exc), patch("time.sleep"):
            with pytest.raises(DuckDBLockError):
                with duckdb_readonly_conn("/locked.duckdb"):
                    pass  # pragma: no cover

    def test_file_not_found_is_not_a_lock_error(self):
        """Fichier introuvable → IOException brute, surtout pas `DuckDBLockError`
        (sinon l'API afficherait un faux « ingestion en cours »)."""
        exc = duckdb.IOException("IO Error: File does not exist")

        with patch("duckdb.connect", side_effect=exc):
            with pytest.raises(duckdb.IOException) as excinfo:
                with duckdb_readonly_conn("/missing.duckdb"):
                    pass  # pragma: no cover

        assert not isinstance(excinfo.value, DuckDBLockError)
