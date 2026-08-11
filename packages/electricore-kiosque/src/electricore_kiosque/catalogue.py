"""Catalogue des notebooks marimo disponibles au Kiosque.

Squelette (#704) : catalogue vide. Les apps réelles (exports, facturation lisible…)
arrivent en tranche suivante (#705) — il suffira de peupler ce dict, `assembler()`
(voir `app.py`) est déjà la couture qui les monte selon `KIOSQUE__APPS`.
"""

from __future__ import annotations

CATALOGUE: dict[str, str] = {}
