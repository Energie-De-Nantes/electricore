"""Le tool de reset des curseurs DLT honore DUCKDB_PATH (issue #146).

`ingestion/tools/` n'est pas un package : on exécute le module par chemin, comme le
ferait `uv run python tools/reset_incremental_state.py`.
"""

import importlib.util
from pathlib import Path

_CHEMIN_TOOL = Path(__file__).parents[2] / "electricore" / "ingestion" / "tools" / "reset_incremental_state.py"


def _charger_tool():
    spec = importlib.util.spec_from_file_location("reset_incremental_state", _CHEMIN_TOOL)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_le_tool_honore_duckdb_path(monkeypatch):
    """DUCKDB_PATH posé → le tool vise cette base, plus un chemin relatif au CWD."""
    monkeypatch.setenv("DUCKDB_PATH", "/data/autre.duckdb")
    tool = _charger_tool()
    assert tool.DB_PATH == Path("/data/autre.duckdb")
