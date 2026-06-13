"""Le pont DBT_DUCKDB_PATH est le seul os.environ que pose le runner (#141, ADR-0025).

dbt est invoqué in-process et `env_var()` est l'unique mécanisme de paramétrage
de `profiles.yml` ; la variable est posée juste autour de l'invocation et restaurée
ensuite pour ne pas survivre dans un éventuel process API.
"""

import os

from electricore.ingestion.runner import pont_dbt_duckdb


def test_pose_la_var_dans_le_scope(monkeypatch):
    monkeypatch.delenv("DBT_DUCKDB_PATH", raising=False)
    with pont_dbt_duckdb("/data/flux.duckdb"):
        assert os.environ["DBT_DUCKDB_PATH"] == "/data/flux.duckdb"


def test_restaure_l_absence_a_la_sortie(monkeypatch):
    monkeypatch.delenv("DBT_DUCKDB_PATH", raising=False)
    with pont_dbt_duckdb("/data/flux.duckdb"):
        pass
    assert "DBT_DUCKDB_PATH" not in os.environ


def test_restaure_la_valeur_anterieure(monkeypatch):
    monkeypatch.setenv("DBT_DUCKDB_PATH", "/ancienne.duckdb")
    with pont_dbt_duckdb("/nouvelle.duckdb"):
        assert os.environ["DBT_DUCKDB_PATH"] == "/nouvelle.duckdb"
    assert os.environ["DBT_DUCKDB_PATH"] == "/ancienne.duckdb"


def test_restaure_meme_sur_exception(monkeypatch):
    monkeypatch.delenv("DBT_DUCKDB_PATH", raising=False)
    try:
        with pont_dbt_duckdb("/data/flux.duckdb"):
            raise RuntimeError("dbt a planté")
    except RuntimeError:
        pass
    assert "DBT_DUCKDB_PATH" not in os.environ
