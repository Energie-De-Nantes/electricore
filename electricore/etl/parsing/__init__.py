"""Noyau pur de linéarisation sélective des flux Enedis (issue #121).

Transforme les documents hiérarchiques (XML, JSON R64) en enregistrements
plats — les lignes des tables DuckDB, aux métadonnées DLT près — en
n'extrayant que les champs décrits par la *Configuration de flux*
(cf. `etl/CONTEXT.md`). Aucune dépendance DLT : les transformers de
`etl/transformers/` sont des adapters minces autour de ce noyau.
"""

from electricore.etl.parsing.r64 import parser_flux_r64
from electricore.etl.parsing.tracabilite import TracabiliteFlux
from electricore.etl.parsing.xml import ConfigFluxXml, match_xml_pattern, parser_flux_xml

__all__ = [
    "ConfigFluxXml",
    "TracabiliteFlux",
    "match_xml_pattern",
    "parser_flux_r64",
    "parser_flux_xml",
]
