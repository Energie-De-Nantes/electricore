"""Compositions pures de pipelines `core/` pour un horizon temporel donné.

Voir `core/CONTEXT.md` (entrée *Contexte mensuel de facturation*) pour la
sémantique des concepts exposés. Les modules de ce package ne font pas
d'I/O — l'appelant (adapter ERP, router API) charge les LazyFrames via
`core/loaders/` puis les transmet aux fonctions de build.
"""

from electricore.core.builds.contexte_mensuel import ContexteMensuel, charger, documents, rapprocher

__all__ = ["ContexteMensuel", "charger", "documents", "rapprocher"]
