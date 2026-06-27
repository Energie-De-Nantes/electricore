"""Build autonome de l'*estimation de provision* d'un PDL (à la demande, ADR-0048).

`RapportProvision` est un build **autonome, par PDL et à la demande** — pas un champ de
`ContexteMensuel` (l'amorçage cold-start n'a pas de contexte mensuel ; décision 7
d'ADR-0048). Il livre l'estimation **bout-en-bout** : cœur → endpoint API → commande bot.

- `estimer(r67, pdl, as_of)` : composition **pure** (l'interface des tests, fixtures) —
  prend le `flux_r67` en LazyFrame, ne fait aucune I/O. Un seul `.collect()` au boundary du
  build (ADR-0019).
- `estimation_provision(pdl, as_of=None)` : entrée **I/O** — résout la source par défaut
  (loader DuckDB `r67`, filtre poussé `pdl = …`) puis délègue à `estimer`. `as_of=None` →
  unique lecture d'horloge (`horloge_par_defaut`, décision 7).

N'émet que des **kWh** — aucune valorisation € (le prix fournisseur appartient à l'ERP,
ADR-0016/0027). Voir `core/CONTEXT.md` (entrée *Estimation de provision*).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import polars as pl

from electricore.core.models.provision import EstimationProvision
from electricore.core.pipelines.provision import (
    horloge_par_defaut,
    pipeline_estimation_provision_r67,
)


@dataclass(frozen=True, slots=True)
class RapportProvision:
    """Livrable de l'estimation de provision d'un PDL (sortie WIDE en kWh + métadonnées).

    `estimation` est un DataFrame **0 ou 1 ligne** (vide si le PDL n'a aucune période R67
    dans la fenêtre de 12 mois), validé `EstimationProvision`. `pdl` et `as_of` sont gardés
    pour la traçabilité du livrable.
    """

    pdl: str
    as_of: dt.date
    estimation: pl.DataFrame

    @property
    def trouve(self) -> bool:
        """`True` si une estimation a pu être produite (au moins une période dans la fenêtre)."""
        return self.estimation.height > 0


def estimer(r67: pl.LazyFrame, pdl: str, as_of: dt.date) -> RapportProvision:
    """Compose l'estimation de provision pour un PDL — pure, sans I/O (interface des tests).

    Args:
        r67: `flux_r67` en LazyFrame (déjà filtré sur le PDL, ou parc complet — le pipeline
            agrège par PDL ; le filtre est ici par robustesse).
        pdl: point de livraison à estimer.
        as_of: date « as-of » de la fenêtre glissante (lue une seule fois au boundary).

    Returns:
        `RapportProvision` ; `estimation` matérialisé (collect au boundary, ADR-0019).
    """
    r67_pdl = r67.filter(pl.col("pdl") == pl.lit(pdl))
    estimation = pipeline_estimation_provision_r67(r67_pdl, as_of).collect()
    return RapportProvision(pdl=pdl, as_of=as_of, estimation=estimation)


def estimation_provision(pdl: str, as_of: dt.date | None = None) -> RapportProvision:
    """Entrée I/O : source par défaut (loader `r67`) puis `estimer()` (décision 7 d'ADR-0048).

    Pousse le filtre `pdl` dans DuckDB (`r67().filter({"pdl": pdl})`), ne ramène que les
    périodes du PDL. `as_of=None` → unique lecture d'horloge au boundary.

    Args:
        pdl: point de livraison à estimer.
        as_of: date « as-of » de la fenêtre glissante ; `None` → aujourd'hui (heure Paris).

    Raises:
        FileNotFoundError: si la base DuckDB est absente (levée par le loader).
    """
    from electricore.core.loaders.duckdb import r67

    if as_of is None:
        as_of = horloge_par_defaut()
    return estimer(r67().filter({"pdl": pdl}).lazy(), pdl, as_of)


__all__ = ["RapportProvision", "estimer", "estimation_provision", "EstimationProvision"]
