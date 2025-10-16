"""
Pipelines de traitement ElectriCore utilisant Polars.

Ce module contient les implémentations fonctionnelles des pipelines
de traitement de données énergétiques utilisant les expressions Polars.
"""

# TODO: Migrer tous les pipelines vers electricore.core._optional pour rendre
# Pandera optionnel partout (requis pour usage WASM sans dépendances lourdes).
# Pour l'instant, on utilise des lazy imports pour éviter de charger facturation.py
# (qui nécessite Pandera) lors de l'import de modules indépendants comme turpe.py

__all__ = [
    "ResultatFacturationPolars",
    "calculer_historique_enrichi",
    "calculer_abonnements",
    "calculer_energie",
    "facturation",
    # Pipeline Accise
    "pipeline_accise",
    "load_accise_rules",
    "agreger_consommations_mensuelles",
    "ajouter_accise",
]


def __getattr__(name: str):
    """
    Lazy import pour éviter de charger automatiquement orchestration/facturation.

    Permet d'importer turpe.py sans déclencher l'import de Pandera via facturation.py.
    Les imports de orchestration ne se font que si explicitement demandés.
    """
    # Pipeline Accise (pas de Pandera)
    if name in ["pipeline_accise", "load_accise_rules", "agreger_consommations_mensuelles", "ajouter_accise"]:
        from .accise import (
            pipeline_accise,
            load_accise_rules,
            agreger_consommations_mensuelles,
            ajouter_accise
        )

        return {
            "pipeline_accise": pipeline_accise,
            "load_accise_rules": load_accise_rules,
            "agreger_consommations_mensuelles": agreger_consommations_mensuelles,
            "ajouter_accise": ajouter_accise,
        }[name]

    # Pipeline orchestration/facturation (avec Pandera)
    if name in __all__:
        from .orchestration import (
            ResultatFacturationPolars,
            calculer_historique_enrichi,
            calculer_abonnements,
            calculer_energie,
            facturation
        )

        return {
            "ResultatFacturationPolars": ResultatFacturationPolars,
            "calculer_historique_enrichi": calculer_historique_enrichi,
            "calculer_abonnements": calculer_abonnements,
            "calculer_energie": calculer_energie,
            "facturation": facturation,
        }[name]

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")