"""Contexte mensuel de facturation : composition pure pour un mois donné.

Voir `core/CONTEXT.md` (entrée *Contexte mensuel de facturation*).

`charger()` prend `historique` et `relevés` en LazyFrame — les loaders restent
à la charge de l'appelant (adapter ERP, router API). Le module ne déclenche
aucune I/O.

`rapprocher()` joint les *lignes de facture* (shape agnostique `LignesFacture`)
à la facturation Enedis du mois porté par le `ContexteMensuel`, et dérive en
core les flags ADR-0014 (`a_facturer`, `a_supprimer`) depuis `est_brouillon`
et `quantite`.

`documents()` assemble le livrable XLSX multi-onglets de campagne mensuelle :
filtre F15 et C15 sur le mois du contexte, applique `rapprocher()`, extrait
`Changements puissance`, et retourne le dict 6-onglets prêt pour
`xlsx_multi_sheet` + le suffixe `YYYY-MM` pour la nomenclature du fichier.
"""

from dataclasses import dataclass

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import DataFrame

from electricore.core.models.lignes_facture import LignesFacture
from electricore.core.models.lignes_facture_rapprochees import LignesFactureRapprochees
from electricore.core.pipelines.abonnements import pipeline_abonnements
from electricore.core.pipelines.energie import pipeline_energie
from electricore.core.pipelines.facturation import pipeline_facturation
from electricore.core.pipelines.historique import pipeline_historique

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


def _composer(
    historique: pl.LazyFrame, releves: pl.LazyFrame
) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.DataFrame]:
    """Compose la séquence canonique des pipelines de facturation.

    `pipeline_historique` (1 fois) → `pipeline_abonnements` +
    `pipeline_energie` (sur l'historique enrichi) → `pipeline_facturation`
    (agrégation mensuelle collectée). Retourne les 4 frames dans l'ordre
    `(historique_enrichi, abonnements, energie, facturation_mensuelle)`.
    """
    historique_enrichi = pipeline_historique(historique)
    abonnements = pipeline_abonnements(historique_enrichi)
    energie = pipeline_energie(historique_enrichi, releves)
    # `pipeline_abonnements` / `pipeline_energie` retournent des LazyFrame génériques
    # validés à l'exécution par les décorateurs Pandera ; pipeline_facturation attend
    # des LazyFrame typés mais le contrat tient.
    facturation_mensuelle = pipeline_facturation(abonnements, energie)  # type: ignore[arg-type]
    return historique_enrichi, abonnements, energie, facturation_mensuelle


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
    historique_enrichi, abonnements, energie, facturation_mensuelle = _composer(historique, releves)

    if mois is None:
        debut_mois_expr = pl.col("debut").dt.truncate("1mo").dt.date()
        mois = str(facturation_mensuelle.select(debut_mois_expr.alias("m"))["m"].max())

    return ContexteMensuel(
        mois=mois,
        historique_enrichi=historique_enrichi,
        abonnements=abonnements,
        energie=energie,
        facturation_mensuelle=facturation_mensuelle,
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


def documents(
    ctx: ContexteMensuel,
    lignes: pl.DataFrame,
    f15: pl.LazyFrame,
    c15: pl.LazyFrame,
) -> tuple[dict[str, pl.DataFrame], str]:
    """Assemble le livrable XLSX multi-onglets de campagne mensuelle.

    Filtre `f15` et `c15` sur le mois porté par `ctx`, applique `rapprocher()`,
    extrait `Changements puissance` (sous-ensemble `memo_puissance != ""`),
    et retourne le dict 6-onglets prêt pour `xlsx_multi_sheet` + le suffixe
    `YYYY-MM`.

    Args:
        ctx: contexte mensuel résolu (`charger(...)`).
        lignes: lignes de facture ERP (shape `LignesFacture`).
        f15: flux F15 brut chargé par l'appelant (cf. topologie #87).
        c15: flux C15 brut chargé par l'appelant.

    Returns:
        `(documents, suffix)` — dict `{libellé_onglet: DataFrame}` consommable
        directement par `xlsx_multi_sheet`, et `suffix` au format `YYYY-MM`.
    """
    mois_date = pl.lit(ctx.mois).str.to_date()

    reconciliation = rapprocher(ctx, lignes)
    changements_puissance = reconciliation.filter(pl.col("memo_puissance") != "")

    f15_df = f15.filter(pl.col("date_facture").dt.truncate("1mo").dt.date() == mois_date).collect()
    f15_prestas = f15_df.filter(pl.col("unite") == "UNITE")

    c15_df = c15.filter(pl.col("date_evenement").dt.truncate("1mo").dt.date() == mois_date).collect()
    c15_sorties = c15_df.filter(pl.col("evenement_declencheur").is_in(["RES", "CFNS"]))

    return (
        {
            "F15 complet": f15_df,
            "F15 prestations": f15_prestas,
            "C15 complet": c15_df,
            "C15 sorties": c15_sorties,
            "Réconciliation": reconciliation,
            "Changements puissance": changements_puissance,
        },
        ctx.mois[:7],
    )
