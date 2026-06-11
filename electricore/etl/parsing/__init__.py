"""Conversion des documents Enedis pour le landing brut (ADR-0020)."""

from electricore.etl.parsing.xml import xml_vers_dict

__all__ = ["xml_vers_dict"]
