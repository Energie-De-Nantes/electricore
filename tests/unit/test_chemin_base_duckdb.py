"""Tests du résolveur unique du chemin de la base DuckDB (issue #146).

Le résolveur est l'unique réponse à « où est la base de production ? » :
`DUCKDB_PATH` sinon défaut absolu ancré sur le dépôt. Le paramètre `env`
rend la résolution déterministe en test (le `.env` local peut définir
`DUCKDB_PATH` sans fausser ces tests) ; `env=None` = environnement réel.
"""

from pathlib import Path

from electricore.config import chemin_base_duckdb


def test_duckdb_path_honore_tel_quel():
    """DUCKDB_PATH prime sur le défaut ; la valeur est restituée telle quelle (pas de resolve)."""
    assert chemin_base_duckdb(env={"DUCKDB_PATH": "/data/flux.duckdb"}) == Path("/data/flux.duckdb")


def test_env_reel_honore(monkeypatch):
    """env=None : l'environnement du process est lu (setenv prime sur le .env, jamais écrasé)."""
    monkeypatch.setenv("DUCKDB_PATH", "/data/process.duckdb")
    assert chemin_base_duckdb() == Path("/data/process.duckdb")


def test_defaut_absolu_et_independant_du_cwd(tmp_path, monkeypatch):
    """Sans DUCKDB_PATH : défaut absolu ancré dépôt, identique quel que soit le CWD."""
    depuis_repo = chemin_base_duckdb(env={})
    monkeypatch.chdir(tmp_path)
    depuis_ailleurs = chemin_base_duckdb(env={})

    assert depuis_repo.is_absolute()
    assert depuis_repo == depuis_ailleurs
    assert depuis_repo.name == "flux_enedis_pipeline.duckdb"
