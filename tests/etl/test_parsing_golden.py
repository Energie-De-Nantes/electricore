"""Invariants sémantiques du parseur legacy (#121) — partent avec lui (#138).

Les comparaisons golden vivent désormais côté dbt (test_dbt_flux_golden, oracle
re-plateformé en #135) : les golden sont typés et générés par le chemin de
production. Ce module ne garde que les invariants lisibles du parseur legacy
(avant/après C15, filtres calendriers, traçabilité, allowlist ConfigFluxXml),
encore utiles tant que le code legacy existe.
"""

from pathlib import Path

import pytest
import yaml

from electricore.etl.parsing import ConfigFluxXml, TracabiliteFlux, parser_flux_r64, parser_flux_xml

FIXTURES = Path(__file__).parents[1] / "fixtures" / "flux"
RACINE = Path(__file__).parents[2]


@pytest.fixture(scope="module")
def config_flux() -> dict:
    return yaml.safe_load((RACINE / "electricore/etl/config/flux.yaml").read_text())


def _tracabilite(nom: str, flux: str) -> TracabiliteFlux:
    # Mêmes valeurs que generer_golden.py — golden déterministes
    return TracabiliteFlux(
        source_zip="fixture.zip", nom_fichier=nom, flux_type=flux, modification_date="2026-01-01T00:00:00"
    )


def _parser(config_flux: dict, fixture: str, flux: str, idx: int) -> tuple[list[dict], str]:
    entry = config_flux[flux]["xml_configs"][idx]
    config = ConfigFluxXml.depuis_yaml(entry)
    records = list(parser_flux_xml((FIXTURES / fixture).read_bytes(), config, _tracabilite(fixture, flux)))
    return records, entry["name"]


# ---------------------------------------------------------------------------
# Invariants sémantiques — lisibles sans ouvrir les golden
# ---------------------------------------------------------------------------


def test_c15_mct_extrait_releves_avant_et_apres(config_flux):
    """MCT avec changement de calendrier : avant_ sur Base, apres_ sur les 4 cadrans.

    C'est la sémantique des nested_fields (Code_Qualification 1/2 → avant/après,
    Classe_Mesure 1, Sens_Mesure 0) — le cœur historique d'electriflux.
    """
    records, _ = _parser(config_flux, "c15_avec_releves.xml", "C15", 0)
    (record,) = records
    assert record["evenement_declencheur"] == "MCT"
    assert record["avant_index_base_kwh"] == "2531"
    assert record["avant_id_calendrier_distributeur"] == "DI000001"
    assert {f"apres_index_{c}_kwh" for c in ("hph", "hch", "hpb", "hcb")} <= set(record)
    assert record["apres_id_calendrier_distributeur"] == "DI000003"


def test_r15_acc_sans_donnees_acc_n_extrait_aucun_cadran_ea(config_flux):
    """Un R15 standard (Classe_Mesure=1) ne produit aucune colonne ea_* en config ACC."""
    records, _ = _parser(config_flux, "r15.xml", "R15", 1)
    (record,) = records
    assert not [k for k in record if k.startswith("ea_")]


def test_r151_extrait_les_4_cadrans_et_ignore_le_calendrier_inconnu(config_flux):
    """4 cadrans saisonniers extraits ; la classe temporelle INCONNU produit une colonne idoine."""
    records, _ = _parser(config_flux, "r151.xml", "R151", 0)
    (record,) = records
    cadrans = {k for k in record if k.startswith("index_")}
    assert {"index_hph_kwh", "index_hch_kwh", "index_hpb_kwh", "index_hcb_kwh"} <= cadrans


def test_r64_ne_garde_que_les_calendriers_distributeur():
    """Les classes des calendriers fournisseur (FC*) sont filtrées : seuls les
    cadrans distributeur (DI*) produisent des colonnes index_*.

    La fixture contient BASE/HP/HC sous calendriers FC02203x — aucun ne doit
    apparaître ; les 4 cadrans saisonniers DI000003 doivent tous y être.
    """
    records = list(parser_flux_r64((FIXTURES / "r64.json").read_bytes(), _tracabilite("r64.json", "R64")))
    assert len(records) == 2
    for r in records:
        index = {k for k in r if k.startswith("index_")}
        assert index == {"index_hph_kwh", "index_hch_kwh", "index_hpb_kwh", "index_hcb_kwh"}


def test_tracabilite_presente_dans_chaque_record(config_flux):
    records, _ = _parser(config_flux, "f12.xml", "F12", 0)
    assert len(records) == 4
    for r in records:
        assert r["_source_zip"] == "fixture.zip"
        assert r["_flux_type"] == "F12"
        assert r["_xml_name"] == "f12.xml"
        assert r["modification_date"] == "2026-01-01T00:00:00"


# ---------------------------------------------------------------------------
# ConfigFluxXml : le contrat de sélection est validé au chargement
# ---------------------------------------------------------------------------


class TestConfigFluxXml:
    def test_toutes_les_configurations_reelles_chargent(self, config_flux):
        """Chaque entrée xml_configs de flux.yaml passe l'allowlist de clés."""
        entries = [e for data in config_flux.values() for e in data.get("xml_configs", [])]
        assert len(entries) >= 6
        for entry in entries:
            ConfigFluxXml.depuis_yaml(entry)

    def test_cle_inconnue_rejetee(self):
        """Une typo YAML (data_field au lieu de data_fields) explose au chargement."""
        with pytest.raises(ValueError, match="data_field"):
            ConfigFluxXml.depuis_yaml({"name": "x", "row_level": ".//PRM", "data_field": {}})

    def test_row_level_obligatoire(self):
        with pytest.raises(ValueError, match="row_level"):
            ConfigFluxXml.depuis_yaml({"name": "x"})
