"""
Pipelines de traitement ElectriCore utilisant Polars.

Ce module contient les implémentations fonctionnelles des pipelines
de traitement de données énergétiques utilisant les expressions Polars.

La composition `facturation()` (et son container `ResultatFacturationPolars`)
a été déplacée dans `electricore.core.builds.contexte_mensuel`
(cf. slice 3 de la refonte Contexte mensuel, issue #89).
"""

# Lazy imports pour éviter de charger Pandera lors d'imports de modules
# indépendants comme turpe.py.

__all__ = [
    "pipeline_accise",
    "load_accise_rules",
    "agreger_consommations_mensuelles",
    "ajouter_accise",
]


def __getattr__(name: str):
    """Lazy import pour `pipeline_accise` & co."""
    if name in __all__:
        from .accise import agreger_consommations_mensuelles, ajouter_accise, load_accise_rules, pipeline_accise

        return {
            "pipeline_accise": pipeline_accise,
            "load_accise_rules": load_accise_rules,
            "agreger_consommations_mensuelles": agreger_consommations_mensuelles,
            "ajouter_accise": ajouter_accise,
        }[name]

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
