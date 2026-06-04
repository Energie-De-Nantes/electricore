"""Tests d'invariants du module d'orchestration de facturation.

Couvre les invariants de surface : présence/absence des entry points,
construction obligatoire du `ResultatFacturationPolars`. La validation
fonctionnelle de `facturation()` est exercée par les tests d'intégration
des services API.
"""

import importlib

import polars as pl
import pytest

from electricore.core.pipelines.orchestration import ResultatFacturationPolars


def test_resultat_facturation_polars_requiert_tous_les_champs():
    """`ResultatFacturationPolars` ne doit pas se construire avec des champs manquants.

    Les quatre champs (`historique_enrichi`, `abonnements`, `energie`,
    `facturation`) sont produits par `facturation()` et doivent tous être
    présents. Aucune construction partielle n'est valide.
    """
    lf = pl.LazyFrame({"x": [1]})

    with pytest.raises(TypeError):
        ResultatFacturationPolars()  # type: ignore[call-arg]

    with pytest.raises(TypeError):
        ResultatFacturationPolars(historique_enrichi=lf)  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "nom",
    ["calculer_historique_enrichi", "calculer_abonnements", "calculer_energie"],
)
def test_helpers_partiels_supprimes_du_module_orchestration(nom: str):
    """Les helpers partiels ne sont plus exposés par `orchestration`.

    Ils ont servi pendant la migration pandas → Polars et sont sans caller
    aujourd'hui. Leur retrait verrouille l'unique entry point `facturation`.
    """
    orchestration = importlib.import_module("electricore.core.pipelines.orchestration")
    assert not hasattr(orchestration, nom), f"{nom} ne devrait plus exister dans orchestration"
    assert nom not in orchestration.__all__, f"{nom} ne devrait plus être dans __all__"


@pytest.mark.parametrize(
    "nom",
    ["calculer_historique_enrichi", "calculer_abonnements", "calculer_energie"],
)
def test_helpers_partiels_supprimes_du_package_pipelines(nom: str):
    """Les helpers partiels ne sont plus ré-exportés par `electricore.core.pipelines`.

    Couvre le `__getattr__` lazy du package, qui exposait les helpers sans
    forcer leur import direct.
    """
    pipelines = importlib.import_module("electricore.core.pipelines")
    assert nom not in pipelines.__all__, f"{nom} ne devrait plus être dans pipelines.__all__"
    with pytest.raises(AttributeError):
        getattr(pipelines, nom)
