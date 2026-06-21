"""Fuseau de session de la connexion read-only des loaders (#393, ADR-0042).

`duckdb_readonly_conn` épingle le fuseau de session DuckDB à `Europe/Paris`, de sorte
que toute lecture d'un **instant** (`TIMESTAMPTZ`) ressorte taguée Paris et que les
littéraux de filtre soient interprétés en heure légale française — déterministe quel
que soit le fuseau de l'hôte (poste local en Paris, VPS/CI en UTC).

Le fuseau par défaut de DuckDB est figé au démarrage du processus (ICU lit `TZ` une
fois) : le modifier en cours de processus ne change rien. Sur une machine déjà en
`Europe/Paris`, une assertion en process serait donc verte par accident. On force ici
un hôte étranger via un **sous-processus** sous `TZ` choisi, seul moyen honnête de
prouver que la connexion épingle bien Paris (et non d'hériter du défaut machine).
"""

import subprocess
import sys
import textwrap
from pathlib import Path

import duckdb

# Instant-bord : 00:01 heure de Paris le 4 octobre 2024 (= 2024-10-03 22:01 UTC), juste
# APRÈS minuit Paris du 4 mais AVANT minuit UTC. La comparaison `>= TIMESTAMP '2024-10-04'`
# sur cet instant dépend du fuseau de session : True sous Paris, False sous UTC. C'est la
# sonde qui distingue une session épinglée Paris d'une session héritée de l'hôte.
_REQUETE_BORD = "SELECT TIMESTAMPTZ '2024-10-04 00:01:00+02:00' >= TIMESTAMP '2024-10-04'"


def _comparaison_bord_via_conn(db_path: Path, tz: str) -> str:
    """Évalue la comparaison-bord à travers `duckdb_readonly_conn`, dans un sous-processus
    dont l'hôte est en fuseau `tz`. Renvoie le booléen DuckDB tel quel ('True'/'False')."""
    code = textwrap.dedent(
        f"""
        from electricore.core.loaders.duckdb.config import duckdb_readonly_conn
        with duckdb_readonly_conn({str(db_path)!r}) as conn:
            print(conn.execute({_REQUETE_BORD!r}).fetchone()[0])
        """
    )
    res = subprocess.run(
        [sys.executable, "-c", code],
        env={"TZ": tz, "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, f"sous-processus (TZ={tz}) a échoué :\n{res.stdout}\n{res.stderr}"
    return res.stdout.strip().splitlines()[-1]


def test_filtre_timestamptz_interprete_paris_quel_que_soit_le_fuseau_hote(tmp_path):
    """#393 : la connexion read-only du loader interprète un littéral de filtre sur une
    colonne TIMESTAMPTZ en heure légale française — MÊME résultat sur un hôte UTC (VPS) et
    un hôte Europe/Paris (dev), via la session épinglée à Paris."""
    db = tmp_path / "vide.duckdb"
    duckdb.connect(str(db)).close()  # fichier vide : la sonde n'a besoin d'aucune table

    sous_utc = _comparaison_bord_via_conn(db, "UTC")
    sous_paris = _comparaison_bord_via_conn(db, "Europe/Paris")

    assert sous_utc == sous_paris  # déterminisme : indépendant du fuseau de l'hôte
    assert sous_utc == "True"  # sémantique Europe/Paris : 00:01 Paris ≥ minuit Paris du 4
