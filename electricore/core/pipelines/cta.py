"""
Calcul de la CTA (Contribution Tarifaire d'Acheminement).

La CTA est une taxe assise sur la part fixe du TURPE. Son taux est fixé par
arrêté ministériel (avec avis de la CRE) et peut changer en cours d'année,
parfois en milieu de trimestre.

Les règles sont chargées depuis electricore/config/cta_rules.csv qui définit
les plages de validité de chaque taux (format identique à accise_rules.csv).

Formule : cta_eur = turpe_fixe_eur × taux_cta_pct / 100
"""

from pathlib import Path

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import LazyFrame

from electricore.core.models.cta_mensuel import CtaMensuel
from electricore.core.pipelines.facturation import expr_calculer_trimestre
from electricore.core.pipelines.taux import ajouter_taux_en_vigueur

# =============================================================================
# CHARGEMENT DES RÈGLES CTA
# =============================================================================


def load_cta_rules() -> pl.LazyFrame:
    """
    Charge l'historique des taux CTA depuis `electricore/config/cta_rules.csv`.

    Returns:
        LazyFrame avec colonnes `start` (datetime Europe/Paris) et
        `taux_cta_pct` (Float64). Chaque ligne représente l'entrée en vigueur
        d'un nouveau taux qui remplace le précédent.
    """
    file_path = Path(__file__).parent.parent.parent / "config" / "cta_rules.csv"

    return (
        pl.scan_csv(file_path)
        .with_columns(pl.col("start").str.to_datetime().dt.replace_time_zone("Europe/Paris"))
        .with_columns(pl.col("taux_cta_pct").cast(pl.Float64))
    )


# =============================================================================
# AJOUT DU TAUX ET DU MONTANT CTA
# =============================================================================


def ajouter_cta(
    df_facturation_mensuel: pl.LazyFrame,
    regles: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """
    Ajoute `taux_cta_pct` et `cta_eur` à chaque ligne de facturation mensuelle.

    Args:
        df_facturation_mensuel: LazyFrame contenant au minimum `debut`
            (datetime Europe/Paris, typiquement le 1er du mois) et
            `turpe_fixe_eur`.
        regles: Historique des taux CTA. Chargé via `load_cta_rules()` si None.

    Returns:
        LazyFrame d'entrée enrichi de `taux_cta_pct` et `cta_eur`
        (arrondi à 2 décimales).
    """
    if regles is None:
        regles = load_cta_rules()

    colonnes_originales = df_facturation_mensuel.collect_schema().names()

    return (
        ajouter_taux_en_vigueur(
            df_facturation_mensuel,
            regles,
            date_col="debut",
            taux_col="taux_cta_pct",
        )
        .with_columns((pl.col("turpe_fixe_eur") * pl.col("taux_cta_pct") / 100).round(2).alias("cta_eur"))
        .select([*colonnes_originales, "taux_cta_pct", "cta_eur"])
    )


# =============================================================================
# PIPELINE AU GRAIN MENSUEL
# =============================================================================


@pa.check_types(lazy=True)
def pipeline_cta(
    facturation_mensuelle: pl.LazyFrame,
    pdl_mapping: pl.LazyFrame,
    regles: pl.LazyFrame | None = None,
) -> LazyFrame[CtaMensuel]:
    """
    Assemble la CTA au grain mensuel (situation contractuelle × mois).

    Frère au bon grain du `pipeline_cta` supprimé par #112 (qui embarquait
    l'agrégation par PDL spécifique à l'onglet Détail du rapport, et n'était
    de ce fait composable par personne) : celui-ci s'arrête au grain mensuel,
    consommé à la fois par `rapport_cta` (build) et par l'export détail brut
    (`cta_par_contrat_service`). Filtre trimestre et agrégations restent à la
    charge des callers.

    Args:
        facturation_mensuelle: Méta-périodes mensuelles (shape `PeriodeMeta`,
            sortie de `charger(...).facturation_mensuelle`). Colonnes requises :
            `pdl`, `debut` (datetime Europe/Paris), `turpe_fixe_eur`.
        pdl_mapping: Mapping `{pdl, order_name}` (une ligne par PDL).
        regles: Historique des taux CTA. Chargé via `load_cta_rules()` si None.

    Returns:
        LazyFrame[CtaMensuel] : facturation mensuelle enrichie de `order_name`,
        `taux_cta_pct`, `cta_eur`, `trimestre`. Matérialisation à la charge du
        caller (ADR-0019) ; le grain (RSC, mois) est garanti par `Config.unique`
        de `CtaMensuel` à la validation du DataFrame collecté.
    """
    jointure = facturation_mensuelle.join(pdl_mapping.select(["pdl", "order_name"]), on="pdl", how="inner")
    return ajouter_cta(jointure, regles).with_columns(expr_calculer_trimestre().alias("trimestre"))


__all__ = [
    "load_cta_rules",
    "ajouter_cta",
    "pipeline_cta",
]
