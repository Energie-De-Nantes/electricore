"""Contexte mensuel de facturation : composition pure pour un mois donné.

Voir `core/CONTEXT.md` (entrée *Contexte mensuel de facturation*).

`charger()` prend `historique` et `relevés` en LazyFrame et ne déclenche
aucune I/O — c'est la composition pure, l'interface des tests (fixtures).
`contexte_du_mois()` est l'entrée I/O (#145) : elle résout les sources par
défaut (loaders DuckDB `c15` / `releves`) puis délègue à
`charger()` — application du scindage pur/I-O prévu par les « Limites »
d'ADR-0019.

`rapprocher()` joint les *lignes de facture* (shape agnostique `LignesFacture`)
à la facturation Enedis du mois porté par le `ContexteMensuel`, et dérive en
core les flags ADR-0014 (`a_facturer`, `a_supprimer`) depuis `est_brouillon`
et `quantite`. Vraie passe-plat (issue #142) : sortie = colonnes d'entrée +
colonnes calculées, aucune colonne ERP nommée ici.

`documents()` assemble le livrable XLSX multi-onglets de campagne mensuelle :
filtre F15 et C15 sur le mois du contexte, applique `rapprocher()`, extrait
`Changements puissance`, et retourne le dict 6-onglets prêt pour
`xlsx_multi_sheet` + le suffixe `YYYY-MM` pour la nomenclature du fichier.
"""

import datetime as dt
from dataclasses import dataclass

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import DataFrame

from electricore.core.loaders import c15, releves
from electricore.core.models.cadrans import col_energie
from electricore.core.models.lignes_facture import LignesFacture
from electricore.core.models.lignes_facture_rapprochees import LignesFactureRapprochees
from electricore.core.pipelines.abonnements import pipeline_abonnements
from electricore.core.pipelines.energie import chronologie_releves, pipeline_energie
from electricore.core.pipelines.facturation import pipeline_facturation
from electricore.core.pipelines.historique import horizon_par_defaut, pipeline_historique

# Les clés sont des *catégories produit* (labels ERP, cf. LignesFacture) —
# concept distinct des cadrans malgré l'homonymie ; seules les valeurs
# (noms de colonnes Enedis) viennent de la convention cadran.
_MAPPING_CATEGORIE_COLONNE: dict[str, str] = {
    "HP": col_energie("hp"),
    "HC": col_energie("hc"),
    "Base": col_energie("base"),
    "Abonnements": "nb_jours",
}

# Contrat d'entrée `LignesFacture` (les 4 colonnes sur lesquelles rapprocher() branche).
_COLONNES_CONTRAT: tuple[str, ...] = (
    "ref_situation_contractuelle",
    "categorie_produit",
    "quantite",
    "est_brouillon",
)

# Colonnes produites par le rapprochement (jointures Enedis + dérivations ADR-0014).
# Sortie = contrat + calculées + passe-plat (issue #142) ; l'ordre des livrables
# est porté par les feuilles, pas ici.
_COLONNES_CALCULEES: tuple[str, ...] = (
    "quantite_enedis",
    "memo_puissance",
    "pdl",
    "debut",
    "fin",
    "qualite",
    "statut_communication",
    "turpe_fixe_eur",
    "turpe_variable_eur",
    "num_compteur",
    "type_compteur",
    "a_facturer",
    "a_supprimer",
)


@dataclass(frozen=True, slots=True)
class ContexteMensuel:
    """Bundle immutable des 4 frames dérivés pour produire la facturation d'un mois.

    Voir `core/CONTEXT.md` (entrée *Contexte mensuel de facturation*).
    """

    mois: str
    historique_enrichi: pl.LazyFrame
    abonnements: pl.LazyFrame
    energie: pl.LazyFrame
    # Journal des relevés effectivement consommés par le calcul d'énergie (#233,
    # ADR-0029) : un relevé par (RSC, date, ordre_index), registres réels d'index +
    # `releve_id` + `nature_index`, conservé pour la traçabilité jusqu'à la facture.
    # Un changement de config en cours de mois (MCT) y figure sans cas particulier.
    releves_utilises: pl.LazyFrame
    facturation_mensuelle: pl.DataFrame


def _composer(
    historique: pl.LazyFrame, releves: pl.LazyFrame, horizon: dt.datetime
) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
    """Compose la séquence canonique des pipelines de facturation.

    `pipeline_historique` (1 fois, borné par `horizon`) → `pipeline_abonnements` +
    `pipeline_energie` (sur l'historique enrichi) → `pipeline_facturation`
    (agrégation mensuelle, toujours lazy). Retourne les 4 LazyFrames dans
    l'ordre `(historique_enrichi, abonnements, energie, facturation_mensuelle)`.
    La matérialisation de `facturation_mensuelle` est portée par `charger()`,
    au boundary du build (ADR-0019).

    L'`horizon` est résolu *une seule fois* par l'appelant (`charger`) au boundary
    I/O et passé explicitement : le pipeline reste une transformation pure, sans
    lecture d'horloge (issue #179, ADR-0019).
    """
    historique_enrichi = pipeline_historique(historique, horizon=horizon)
    abonnements = pipeline_abonnements(historique_enrichi)
    # `releves` arrive en `pl.LazyFrame` brut depuis l'appelant (DuckDB) ; la
    # conformité à `RelevéIndex` est garantie à l'exécution par le décorateur
    # Pandera de `pipeline_energie`.
    energie = pipeline_energie(historique_enrichi, releves)  # type: ignore[arg-type]
    # Journal des relevés utilisés (#233, ADR-0029) : ré-assemble la chronologie sur
    # les mêmes entrées que `pipeline_energie` (historique impacté + relevés canoniques)
    # et la conserve dans le bundle. Calcul indépendant — les énergies restent intactes.
    releves_utilises = chronologie_releves(
        historique_enrichi.filter(pl.col("impacte_energie")),  # type: ignore[arg-type]
        releves,  # type: ignore[arg-type]
    )
    facturation_mensuelle = pipeline_facturation(abonnements, energie)
    return historique_enrichi, abonnements, energie, releves_utilises, facturation_mensuelle


def charger(
    historique: pl.LazyFrame,
    releves: pl.LazyFrame,
    mois: str | None = None,
    horizon: dt.datetime | None = None,
) -> ContexteMensuel:
    """Compose les pipelines de facturation et résout le mois cible.

    Args:
        historique: événements C15 (sortie de `c15().lazy()` côté DuckDB).
        releves: relevés du modèle canonique (sortie de `releves().lazy()`).
        mois: format `YYYY-MM-DD` (premier jour du mois). `None` → dernier mois
            disponible dans `facturation_mensuelle`.
        horizon: borne de facturation (`datetime` Europe/Paris) passée à
            `pipeline_historique`. `None` → 1er du mois courant, **résolu ici une
            seule fois** (boundary I/O, ADR-0019) — c'est l'unique site de lecture
            d'horloge de la chaîne de facturation (issue #179). Fournir un horizon
            explicite rend le contexte mensuel déterministe (tests, rejeu).
    """
    if horizon is None:
        horizon = horizon_par_defaut()

    historique_enrichi, abonnements, energie, releves_utilises, facturation_mensuelle_lf = _composer(
        historique, releves, horizon
    )

    # Matérialisation au boundary du build (ADR-0019) : pipeline_facturation
    # reste lazy, le build collecte pour stocker dans le bundle ContexteMensuel.
    facturation_mensuelle = facturation_mensuelle_lf.collect()

    if mois is None:
        debut_mois_expr = pl.col("debut").dt.truncate("1mo").dt.date()
        mois = str(facturation_mensuelle.select(debut_mois_expr.alias("m"))["m"].max())

    return ContexteMensuel(
        mois=mois,
        historique_enrichi=historique_enrichi,
        abonnements=abonnements,
        energie=energie,
        releves_utilises=releves_utilises,
        facturation_mensuelle=facturation_mensuelle,
    )


def contexte_du_mois(mois: str | None = None, horizon: dt.datetime | None = None) -> ContexteMensuel:
    """Entrée I/O du contexte mensuel : sources par défaut puis `charger()` (#145).

    Résout les deux sources canoniques (loaders DuckDB `c15` et `releves`,
    ce dernier = modèle de relevés canonique dbt) et délègue la composition à
    `charger()`. Qui dispose déjà de frames (tests, notebooks, autre source)
    appelle `charger()` directement.

    Args:
        mois: format `YYYY-MM-DD` (premier jour du mois). `None` → dernier mois
            disponible.
        horizon: borne de facturation transmise à `charger()` ; `None` → 1er du
            mois courant résolu une seule fois au boundary (issue #179).

    Raises:
        FileNotFoundError: si la base DuckDB est absente (levée par les loaders
            à l'appel — leur `.lazy()` exécute la lecture immédiatement).
    """
    return charger(c15().lazy(), releves().lazy(), mois=mois, horizon=horizon)


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

    Sortie = colonnes d'entrée + colonnes calculées (vraie passe-plat, issue #142),
    dans l'ordre contrat → calculées → passe-plat (ordre d'entrée). Une colonne
    d'entrée homonyme d'une colonne réservée lève `ValueError` au seam.

    Voir `core/CONTEXT.md` (entrée *Rapprochement facturation mensuelle*).
    """
    mois_cible = pl.lit(ctx.mois).str.to_date()
    debut_mois = pl.col("debut").dt.truncate("1mo").dt.date()

    # Colonnes Enedis nécessaires au calcul de `quantite_enedis` — consommées
    # puis écartées de la sortie (intermédiaires, pas des colonnes calculées).
    colonnes_quantite = list(dict.fromkeys(_MAPPING_CATEGORIE_COLONNE.values()))

    # Garde au seam : une colonne d'entrée homonyme d'une calculée ou d'un
    # intermédiaire serait silencieusement ambiguë dans la jointure.
    reservees = set(_COLONNES_CALCULEES) | set(colonnes_quantite)
    collisions = sorted(set(lignes.columns) & reservees)
    if collisions:
        raise ValueError(
            f"Colonnes d'entrée en collision avec les colonnes réservées du rapprochement : {collisions}. "
            "Renommer côté adaptateur ERP avant rapprocher()."
        )
    fact_mois = (
        ctx.facturation_mensuelle.lazy()
        .filter(debut_mois == mois_cible)
        .select(
            [
                "ref_situation_contractuelle",
                "memo_puissance",
                "pdl",
                "debut",
                "fin",
                "qualite",
                "statut_communication",
                "turpe_fixe_eur",
                "turpe_variable_eur",
                *colonnes_quantite,
            ]
        )
    )

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

    # Vraie passe-plat (issue #142) : toute colonne d'entrée hors contrat ressort
    # telle quelle, dans son ordre d'entrée, après contrat + calculées.
    passe_plat = [c for c in lignes.columns if c not in _COLONNES_CONTRAT]

    # Le `.collect()` final retourne un `pl.DataFrame` ; le décorateur
    # `@pa.check_types` valide qu'il matche `LignesFactureRapprochees`.
    return (
        lignes.lazy()
        .join(fact_mois, on="ref_situation_contractuelle", how="left")
        .join(compteur_par_rsc, on="ref_situation_contractuelle", how="left")
        .with_columns([quantite_enedis_expr, a_facturer_expr, a_supprimer_expr])
        .select([*_COLONNES_CONTRAT, *_COLONNES_CALCULEES, *passe_plat])
        .collect()  # type: ignore[return-value]
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

    # `lignes` est typée `pl.DataFrame` à la frontière publique ; `rapprocher`
    # exige le marqueur Pandera mais la validation reste effective.
    reconciliation = rapprocher(ctx, lignes)  # type: ignore[arg-type]
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
