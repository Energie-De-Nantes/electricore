"""Tests de la configuration de flux (#535, glossaire « Flux connu »).

`flux_connus()` est une lecture YAML pure — aucun import `dlt` — donc testable
sans l'extra [ingestion].
"""

import ast
from pathlib import Path

from electricore.ingestion.config import flux_connus


def test_config_ne_dependend_pas_de_dlt():
    """Source unique légère (#535) : le module ne doit importer aucun `dlt` —
    core doit rester importable sans l'extra [ingestion]."""
    source = Path(flux_connus.__globals__["__file__"]).read_text()
    imports = {
        n.name.split(".")[0] for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Import) for n in node.names
    }
    imports |= {
        node.module.split(".")[0]
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert "dlt" not in imports


def test_flux_connus_retourne_les_cles_de_flux_yaml_en_minuscules():
    connus = flux_connus()

    assert connus == {"c15", "f12", "f15", "r15", "r151", "r64", "r67", "c12", "affaires"}


def test_flux_connus_est_un_frozenset():
    assert isinstance(flux_connus(), frozenset)
