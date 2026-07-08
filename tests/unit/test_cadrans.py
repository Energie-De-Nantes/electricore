"""Tests de contrat du module Cadran (`core/models/cadrans.py`) — issue #119.

Le module porte la liste canonique des cadrans et la convention de nommage
`grandeur_cadran_unité` (cf. CONTEXT.md, entrée *Cadran*).
"""

import pytest

from electricore.core.models.cadrans import CADRANS, SOUS_CADRANS, col_energie, col_index, famille_cadrans


class TestCadrans:
    def test_sept_cadrans_canoniques(self):
        assert CADRANS == ("base", "hp", "hc", "hph", "hch", "hpb", "hcb")

    def test_sous_cadrans_relation_de_synthese(self):
        """hp se synthétise depuis hph+hpb, hc depuis hch+hcb (haute/basse saison)."""
        assert SOUS_CADRANS == {"hp": ("hph", "hpb"), "hc": ("hch", "hcb")}

    def test_sous_cadrans_sont_des_cadrans(self):
        for principal, sous in SOUS_CADRANS.items():
            assert principal in CADRANS
            assert all(s in CADRANS for s in sous)


class TestConstructeursDeColonnes:
    def test_col_energie(self):
        assert col_energie("hp") == "energie_hp_kwh"
        assert col_energie("base") == "energie_base_kwh"

    def test_col_index(self):
        assert col_index("hch") == "index_hch_kwh"

    def test_cadran_inconnu_rejete(self):
        """Détection de typo : un cadran hors liste canonique lève ValueError."""
        with pytest.raises(ValueError, match="hpc"):
            col_energie("hpc")
        with pytest.raises(ValueError, match="HP"):
            col_index("HP")  # les cadrans sont en minuscules


class TestFamilleCadrans:
    """Table DI→famille (ADR-0035 §1, #603) : source unique du glossaire *Famille de cadrans*."""

    def test_di000001_base(self):
        assert famille_cadrans("DI000001") == "base"

    def test_di000002_hp_hc(self):
        assert famille_cadrans("DI000002") == "hp_hc"

    def test_di000003_4_cadrans(self):
        assert famille_cadrans("DI000003") == "4_cadrans"

    def test_di_inconnu_absent(self):
        assert famille_cadrans("DI999999") is None

    def test_none_absent(self):
        assert famille_cadrans(None) is None
