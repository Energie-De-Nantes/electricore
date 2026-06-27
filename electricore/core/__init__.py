"""Cœur de calcul énergétique ElectriCore (ERP-agnostique, ADR-0016).

Les sous-packages s'importent explicitement pour éviter les imports circulaires :
``from electricore.core.pipelines.historique import pipeline_historique``,
``from electricore.core.builds.contexte_mensuel import contexte_du_mois``, etc.
"""
