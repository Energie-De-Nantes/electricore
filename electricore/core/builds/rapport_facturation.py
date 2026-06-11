"""Build du livrable facturation mensuelle (issue #143, symétrique de `rapport_taxe`).

Assemble le `RapportFacturation` (3 onglets : `Résumé` / `Lignes` /
`Changements puissance`) sur le shape agnostique `LignesFacture` — aucune
dépendance ERP : le caller (adaptateur ou service API) fournit les lignes.

L'**ordre des colonnes du livrable facturiste vit ici**, dans
`feuilles_rapport_facturation` (décision #142/#143) : `rapprocher()` ne fait
aucune promesse d'ordre aux livrables.
"""

from dataclasses import dataclass

import polars as pl
from pandera.typing.polars import DataFrame

from electricore.core.builds.contexte_mensuel import (
    _COLONNES_CALCULEES,
    _COLONNES_CONTRAT,
    ContexteMensuel,
    rapprocher,
)
from electricore.core.models.lignes_facture import LignesFacture
from electricore.core.models.rapport_facturation import RapportFacturationResume


@dataclass(frozen=True, slots=True)
class RapportFacturation:
    """Livrable mensuel de facturation (= les 3 onglets de l'XLSX facturiste).

    - `resume` : totaux du mois (1 ligne).
    - `lignes` : sortie de `rapprocher()` (toutes les lignes du mois).
    - `changements_puissance` : sous-ensemble de `lignes` où `memo_puissance != ""`.
      Les lignes apparaissent dans les deux onglets (drill-down, pas partition exclusive).
    """

    resume: pl.DataFrame
    lignes: pl.DataFrame
    changements_puissance: pl.DataFrame


def rapport_facturation(ctx: ContexteMensuel, lignes: DataFrame[LignesFacture]) -> RapportFacturation:
    """Assemble le livrable facturation du mois porté par le contexte.

    Calque de l'interface de `documents()` : le build applique `rapprocher()`
    lui-même — une seule façon d'entrer dans le mensuel. Le mois vient de
    `ctx.mois` (toujours résolu par `charger()`).

    Args:
        ctx: contexte mensuel résolu (`charger(...)`).
        lignes: lignes de facture ERP (shape `LignesFacture`, passe-plat libre).

    Returns:
        `RapportFacturation(resume, lignes, changements_puissance)` — `resume`
        validé `RapportFacturationResume` ; `lignes` validées par le décorateur
        de `rapprocher()`.
    """
    rapprochees = rapprocher(ctx, lignes)
    changements_puissance = rapprochees.filter(pl.col("memo_puissance") != "")

    resume = pl.DataFrame(
        {
            "mois": [ctx.mois],
            "nb_pdl": [rapprochees["pdl"].drop_nulls().n_unique()],
            "total_a_facturer": [int(rapprochees["a_facturer"].fill_null(False).sum())],
            "total_a_supprimer": [int(rapprochees["a_supprimer"].fill_null(False).sum())],
        }
    )
    RapportFacturationResume.validate(resume)
    return RapportFacturation(resume=resume, lignes=rapprochees, changements_puissance=changements_puissance)


def _ordre_facturiste(df: pl.DataFrame) -> pl.DataFrame:
    """Ordre du livrable : passe-plat (ordre d'entrée — les identifiants ERP que
    le facturiste reconnaît), puis contrat, puis colonnes calculées Enedis."""
    fixes = [*_COLONNES_CONTRAT, *_COLONNES_CALCULEES]
    passe_plat = [c for c in df.columns if c not in fixes]
    return df.select([*passe_plat, *[c for c in fixes if c in df.columns]])


def feuilles_rapport_facturation(r: RapportFacturation) -> dict[str, pl.DataFrame]:
    """Mapping onglet → DataFrame pour le livrable XLSX facturation (cf. CONTEXT.md).

    Consommable directement par `xlsx_multi_sheet`. Co-localisée avec
    `rapport_facturation` parce que le shape du livrable et son contenu sont
    indissociables — c'est ici que l'ordre des colonnes facturiste est pinné.
    """
    return {
        "Résumé": r.resume,
        "Lignes": _ordre_facturiste(r.lignes),
        "Changements puissance": _ordre_facturiste(r.changements_puissance),
    }
