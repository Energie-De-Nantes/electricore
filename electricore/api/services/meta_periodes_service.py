"""Wire-up de l'endpoint de lecture des méta-périodes (ADR-0027, #227).

Résout le contexte mensuel (loaders DuckDB par défaut via `contexte_du_mois`),
filtre la facturation mensuelle sur le mois cible (+ RSC optionnelles) et projette
les colonnes du **contrat v1** (champs déjà portés par `PeriodeMeta`).

Le réglementaire suit la règle ADR-0027 : *montant € quand electricore possède
l'assiette, taux quand l'ERP la possède.* → `cta_eur` (assiette = `turpe_fixe_eur`,
possédée par electricore) en € ; `taux_accise_eur_mwh` en taux (assiette accise = le
facturé, possédé par l'ERP — pas d'`accise_eur` ici). ERP-agnostique : aucun import
`integrations/odoo` (ADR-0016). L'intégrité (`source_hash`) arrive en #229.
"""

import hashlib

import polars as pl

from electricore.core.builds.contexte_mensuel import contexte_du_mois
from electricore.core.pipelines.accise import load_accise_rules
from electricore.core.pipelines.cta import ajouter_cta
from electricore.core.pipelines.taux import ajouter_taux_en_vigueur

# Version du contrat exposée dans l'enveloppe (#229). Bump sur rupture (cf. ADR-0027,
# évolution additive — un ajout de colonne optionnelle ne change pas la version).
# v2 (#317/#327, ADR-0033) : retrait cassant de `data_complete` / `coverage_abo` /
# `coverage_energie`, remplacés par les verdicts jumeaux `qualite` + `statut_communication`.
CONTRAT_VERSION = 2

# Colonnes du contrat v2 — sous-ensemble de `PeriodeMeta` + bloc réglementaire (#228).
COLONNES_CONTRAT: tuple[str, ...] = (
    "ref_situation_contractuelle",
    "pdl",
    "mois_annee",
    "debut",
    "fin",
    "nb_jours",
    "puissance_moyenne_kva",
    "formule_tarifaire_acheminement",
    "energie_base_kwh",
    "energie_hp_kwh",
    "energie_hc_kwh",
    "turpe_fixe_eur",
    "turpe_variable_eur",
    "cta_eur",
    "taux_accise_eur_mwh",
    "has_changement",
    # Verdicts méta jumeaux — qualité (ADR-0033) + communication (ADR-0036). Remplacent
    # `data_complete` / `coverage_*` (retirés en v2, ADR-0033) : `qualite` distingue
    # réel/estimé/incalculable, `statut_communication` route l'énergie.
    "qualite",
    "statut_communication",
)


def meta_periodes(mois: str | None = None, rsc: list[str] | None = None) -> tuple[str, pl.DataFrame]:
    """Méta-périodes du mois cible, projetées sur le contrat v1.

    Args:
        mois: format `YYYY-MM-DD` (premier jour du mois). `None` → dernier mois
            disponible.
        rsc: filtre optionnel sur une liste de `ref_situation_contractuelle`.

    Returns:
        `(mois_résolu, df)` — le mois effectif (`YYYY-MM-DD`) porté par le contexte,
        et le DataFrame des méta-périodes du mois, colonnes du contrat v1.
    """
    contexte = contexte_du_mois(mois)

    mois_cible = pl.lit(contexte.mois).str.to_date()
    debut_mois = pl.col("debut").dt.truncate("1mo").dt.date()

    lf = contexte.facturation_mensuelle.lazy().filter(debut_mois == mois_cible)
    if rsc:
        lf = lf.filter(pl.col("ref_situation_contractuelle").is_in(rsc))

    # Bloc réglementaire (#228) : CTA en montant € (assiette electricore), accise en
    # taux seul (assiette ERP). Les deux s'appuient sur `debut` pour le taux en vigueur.
    lf = ajouter_cta(lf)
    lf = ajouter_taux_en_vigueur(lf, load_accise_rules(), date_col="debut", taux_col="taux_accise_eur_mwh")

    return contexte.mois, _ajouter_source_hash(lf.select(COLONNES_CONTRAT).collect())


def _ajouter_source_hash(df: pl.DataFrame) -> pl.DataFrame:
    """Ajoute `source_hash` : sha256 (tronqué) de la ligne canonique du contrat (#229).

    Hash de contenu sur **toutes** les colonnes du contrat — déterministe (même état
    DuckDB → même hash) et robuste aux versions de Polars (repr texte stable, pas le
    hash interne). Outille l'upsert non destructif côté Odoo : skip-si-inchangé +
    détection de dérive sous verrou (ADR-0027).
    """
    canon = df.select(
        pl.concat_str(
            [pl.col(c).cast(pl.Utf8).fill_null("∅") for c in COLONNES_CONTRAT],
            separator="␟",
        ).alias("_canon")
    ).to_series()
    hashes = [hashlib.sha256(ligne.encode("utf-8")).hexdigest()[:16] for ligne in canon.to_list()]
    return df.with_columns(pl.Series("source_hash", hashes, dtype=pl.Utf8))
