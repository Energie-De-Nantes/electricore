"""Contexte mensuel de facturation : composition pure pour un mois donné.

Voir `core/CONTEXT.md` (entrée *Contexte mensuel de facturation*).

`charger()` prend `historique` et `relevés` en LazyFrame — les loaders restent
à la charge de l'appelant (adapter ERP, router API). Le module ne déclenche
aucune I/O.

`rapprocher()` joint les *lignes de facture* (shape agnostique `LignesFacture`)
à la facturation Enedis du mois porté par le `ContexteMensuel`, et dérive en
core les flags ADR-0014 (`a_facturer`, `a_supprimer`) depuis `est_brouillon`
et `quantite`.
"""

from dataclasses import dataclass

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import DataFrame

from electricore.core.models.lignes_facture import LignesFacture
from electricore.core.models.lignes_facture_rapprochees import LignesFactureRapprochees
from electricore.core.pipelines.orchestration import facturation

_MAPPING_CATEGORIE_COLONNE: dict[str, str] = {
    "HP": "energie_hp_kwh",
    "HC": "energie_hc_kwh",
    "Base": "energie_base_kwh",
    "Abonnements": "nb_jours",
}


@dataclass(frozen=True)
class ContexteMensuel:
    """Bundle immutable des 4 frames dérivés pour produire la facturation d'un mois.

    Voir `core/CONTEXT.md` (entrée *Contexte mensuel de facturation*).
    """

    mois: str
    historique_enrichi: pl.LazyFrame
    abonnements: pl.LazyFrame
    energie: pl.LazyFrame
    facturation_mensuelle: pl.DataFrame


def charger(
    historique: pl.LazyFrame,
    releves: pl.LazyFrame,
    mois: str | None = None,
) -> ContexteMensuel:
    """Compose les pipelines de facturation et résout le mois cible.

    Args:
        historique: événements C15 (sortie de `c15().lazy()` côté DuckDB).
        releves: relevés harmonisés (sortie de `releves_harmonises().lazy()`).
        mois: format `YYYY-MM-DD` (premier jour du mois). `None` → dernier mois
            disponible dans `facturation_mensuelle`.
    """
    result = facturation(historique=historique, releves=releves)

    if mois is None:
        debut_mois_expr = pl.col("debut").dt.truncate("1mo").dt.date()
        mois = str(result.facturation.select(debut_mois_expr.alias("m"))["m"].max())

    return ContexteMensuel(
        mois=mois,
        historique_enrichi=result.historique_enrichi,
        abonnements=result.abonnements,
        energie=result.energie,
        facturation_mensuelle=result.facturation,
    )


@pa.check_types(lazy=True)
def rapprocher(
    ctx: ContexteMensuel,
    lignes: DataFrame[LignesFacture],
) -> DataFrame[LignesFactureRapprochees]:
    """Joint les *lignes de facture* (shape agnostique) à la méta-période Enedis du mois.

    - Filtre `ctx.facturation_mensuelle` sur `ctx.mois`.
    - Joint sur `ref_situation_contractuelle`.
    - Calcule `quantite_enedis` selon `categorie_produit` (mapping fixe vers les
      colonnes Enedis `energie_*_kwh` et `nb_jours`).
    - Joint depuis l'historique les identifiants compteur (`num_compteur`, `type_compteur`).
    - Dérive en core les flags ADR-0014 : `a_facturer = est_brouillon ∧ quantite > 0`
      et `a_supprimer = est_brouillon ∧ quantite == 0`.

    Voir `core/CONTEXT.md` (entrée *Rapprochement facturation mensuelle*).
    """
    mois_cible = pl.lit(ctx.mois).str.to_date()
    debut_mois = pl.col("debut").dt.truncate("1mo").dt.date()
    fact_mois = ctx.facturation_mensuelle.lazy().filter(debut_mois == mois_cible)

    compteur_par_rsc = ctx.historique_enrichi.select(
        ["ref_situation_contractuelle", "num_compteur", "type_compteur"]
    ).unique(subset=["ref_situation_contractuelle"], keep="last")

    quantite_enedis_expr = pl.coalesce(
        [
            pl.when(pl.col("categorie_produit") == cat).then(pl.col(col).cast(pl.Float64))
            for cat, col in _MAPPING_CATEGORIE_COLONNE.items()
        ]
    ).alias("quantite_enedis")

    a_facturer_expr = (pl.col("est_brouillon") & (pl.col("quantite") > 0)).alias("a_facturer")
    a_supprimer_expr = (pl.col("est_brouillon") & (pl.col("quantite") == 0)).alias("a_supprimer")

    return (
        lignes.lazy()
        .join(fact_mois, on="ref_situation_contractuelle", how="left")
        .join(compteur_par_rsc, on="ref_situation_contractuelle", how="left")
        .with_columns([quantite_enedis_expr, a_facturer_expr, a_supprimer_expr])
        .select(
            [
                # Identifiants ERP passe-plat
                "invoice_line_ids",
                "x_pdl",
                "x_lisse",
                "name_account_move",
                "name_product_product",
                # Clés métier renommées
                "categorie_produit",
                "quantite",
                # Quantité Enedis + mémo
                "quantite_enedis",
                "memo_puissance",
                # Méta-période Enedis
                "ref_situation_contractuelle",
                "pdl",
                "debut",
                "fin",
                "data_complete",
                "turpe_fixe_eur",
                "turpe_variable_eur",
                # Identifiants compteur
                "num_compteur",
                "type_compteur",
                # Flags ADR-0014 dérivés en core
                "a_facturer",
                "a_supprimer",
            ]
        )
        .collect()
    )
