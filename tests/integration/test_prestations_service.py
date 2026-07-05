"""Tests du service prestations F15 (pull-tout, souscriptions_odoo#37).

Le seam est le loader `f15` (monkeypatché par un stub dont `.lazy()` rend un
LazyFrame synthétique) ; le filtre `unite='UNITE'`, la projection contrat v1 et
le calcul de `reference` (sha256 de contenu — le F15 n'a pas d'id de ligne)
restent réels.
"""

from datetime import date

import polars as pl

from electricore.api.services import prestations_service


class _StubF15:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def lazy(self) -> pl.LazyFrame:
        return self._df.lazy()


def _f15_synthetique() -> pl.DataFrame:
    """Trois lignes façonnées sur le golden `flux_f15_detail` : une indemnité
    (`NS`, montant négatif), une prestation taxée, une ligne d'énergie (kWh)
    qui doit être écartée par le filtre."""
    return pl.DataFrame(
        {
            "pdl": ["99510061232830", "99510061232830", "99510099999999"],
            "ref_situation_contractuelle": ["833195558", "833195558", "835000001"],
            "id_ev": ["DCOUP_PEN", "F180B", "F180B"],
            "nature_ev": ["03", "01", "01"],
            "libelle_ev": ["Pénalité pour coupure réseau", "Mise en service", "Mise en service"],
            "taux_tva_applicable": ["NS", "20.00", "20.00"],
            "unite": ["UNITE", "UNITE", "kWh"],
            "prix_unitaire": [-24.0, 30.37, 30.37],
            "quantite": [2.0, 1.0, 1.0],
            "montant_ht": [-48.0, 30.37, 30.37],
            "date_debut": [date(2025, 1, 17), date(2025, 2, 3), date(2025, 2, 3)],
            "date_fin": [date(2025, 1, 17), date(2025, 2, 3), date(2025, 2, 3)],
            "num_facture": ["3210619182009", "3210619182010", "3210619182011"],
            "date_facture": [date(2025, 2, 5), date(2025, 2, 5), date(2025, 2, 5)],
        }
    )


def test_prestations_filtre_unite_et_projette_le_contrat(monkeypatch):
    monkeypatch.setattr(prestations_service, "f15", lambda: _StubF15(_f15_synthetique()))

    df = prestations_service.prestations()

    assert df.height == 2  # la ligne kWh (énergie) est écartée
    assert set(df.columns) == set(prestations_service.COLONNES_CONTRAT) | {"reference"}
    assert df["date_debut"].to_list() == ["2025-01-17", "2025-02-03"]  # jours civils en ISO


def test_reference_deterministe_et_unique_par_contenu(monkeypatch):
    monkeypatch.setattr(prestations_service, "f15", lambda: _StubF15(_f15_synthetique()))
    a = prestations_service.prestations()
    monkeypatch.setattr(prestations_service, "f15", lambda: _StubF15(_f15_synthetique()))
    b = prestations_service.prestations()

    assert a["reference"].to_list() == b["reference"].to_list()  # stable entre runs
    assert a["reference"].n_unique() == a.height  # unique par contenu


def test_reference_insensible_au_libelle(monkeypatch):
    """Une retouche de libellé Enedis n'est pas une nouvelle prestation (assiette du hash)."""
    df = _f15_synthetique()
    monkeypatch.setattr(prestations_service, "f15", lambda: _StubF15(df))
    avant = prestations_service.prestations()["reference"].to_list()

    retouche = df.with_columns(pl.lit("Libellé retouché").alias("libelle_ev"))
    monkeypatch.setattr(prestations_service, "f15", lambda: _StubF15(retouche))
    apres = prestations_service.prestations()["reference"].to_list()

    assert avant == apres


def test_prestations_filtre_rsc(monkeypatch):
    monkeypatch.setattr(prestations_service, "f15", lambda: _StubF15(_f15_synthetique()))

    df = prestations_service.prestations(rsc=["833195558"])

    assert df["ref_situation_contractuelle"].unique().to_list() == ["833195558"]
