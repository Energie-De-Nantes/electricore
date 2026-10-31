"""Déclaration CTA parc entier, sans Odoo (#651, ADR-0058).

Un seul calcul, deux représentations : `declaration_cta()` produit le **cœur** (grain
RSC × mois, servi tel quel en Arrow) et `feuilles_declaration_cta()` l'habille en onglets
xlsx. Assiette = TURPE fixe recalculé par electricore (ADR-0027), CTA via `ajouter_cta()`
— jamais `pipeline_cta` legacy (jointure Odoo). Sans état : recalcul à chaque requête,
c'est le document posté (déclaration, facture ERP) qui fige le montant ; les empreintes
permettent de repérer une dérive ultérieure. ERP-agnostique : zéro `integrations` (ADR-0016).
"""

import datetime as dt
import hashlib

import polars as pl
from fastapi import HTTPException

from electricore.api.serializers import empreinte_contenu
from electricore.core.builds.contexte_mensuel import contexte_du_mois
from electricore.core.builds.rapport_taxe import agreger_par_taux, agreger_resume
from electricore.core.pipelines.cta import ajouter_cta
from electricore.core.pipelines.facturation import expr_calculer_trimestre
from electricore.core.pipelines.historique import horizon_par_defaut

# Version du contrat d'empreinte (ADR-0058) : bump dès que `COLONNES_EMPREINTE` ou la
# construction de `empreinte_trimestre` change — les empreintes d'une autre version ne
# sont pas comparables.
VERSION_EMPREINTE = 1

# Colonnes de montant couvertes par les empreintes — `trimestre` exclu (dérivé de
# `mois_annee`). Ordre = contrat d'octets.
COLONNES_EMPREINTE: tuple[str, ...] = (
    "ref_situation_contractuelle",
    "pdl",
    "mois_annee",
    "turpe_fixe_eur",
    "taux_cta_pct",
    "cta_eur",
)

COLONNES_DECLARATION: tuple[str, ...] = (*COLONNES_EMPREINTE[:3], "trimestre", *COLONNES_EMPREINTE[3:])


def trimestre_de(jour: dt.date) -> str:
    """Trimestre `YYYY-TN` d'un jour — même format que `expr_calculer_trimestre`."""
    return f"{jour.year}-T{(jour.month - 1) // 3 + 1}"


def _empreinte_trimestre(source_hashes: list[str]) -> str:
    """Empreinte d'un trimestre : sha256 tronqué des `source_hash` triés (indépendant de l'ordre)."""
    return hashlib.sha256("␟".join(sorted(source_hashes)).encode()).hexdigest()[:16]


def declaration_cta(trimestre: str | None = None, horizon: dt.datetime | None = None) -> pl.DataFrame:
    """Cœur de la déclaration CTA : une ligne par (RSC, mois), parc entier.

    Args:
        trimestre: `YYYY-TN`. Un trimestre **non terminé** à l'horizon → 422
            `precondition`. `None` → tous les trimestres terminés.
        horizon: borne de calcul ; `None` → 1ᵉʳ du mois courant (`horizon_par_defaut`).
            Un trimestre est terminé ssi il précède celui de l'horizon.

    Returns:
        `COLONNES_DECLARATION` + `source_hash` (par ligne) + `empreinte_trimestre`
        (répétée sur chaque ligne du trimestre), trié par `trimestre`, `pdl`, `mois_annee`.
    """
    horizon = horizon or horizon_par_defaut()
    trimestre_en_cours = trimestre_de(horizon.date())
    if trimestre is not None and trimestre >= trimestre_en_cours:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Le trimestre {trimestre} n'est pas terminé (calcul arrêté au "
                f"{horizon.date().isoformat()}) : impossible de le déclarer."
            ),
            headers={"X-Error-Kind": "precondition"},
        )

    filtre = pl.col("trimestre") == trimestre if trimestre else pl.col("trimestre") < trimestre_en_cours
    df = (
        ajouter_cta(contexte_du_mois(horizon=horizon).facturation_mensuelle.lazy())
        .with_columns(expr_calculer_trimestre().alias("trimestre"))
        .filter(filtre)
        .select(COLONNES_DECLARATION)
        .sort("trimestre", "pdl", "mois_annee")
        .collect()
    )
    df = df.with_columns(empreinte_contenu(df, list(COLONNES_EMPREINTE)).alias("source_hash"))
    empreintes = {
        cle[0]: _empreinte_trimestre(groupe["source_hash"].to_list()) for cle, groupe in df.group_by("trimestre")
    }
    return df.with_columns(
        pl.col("trimestre").replace_strict(empreintes, return_dtype=pl.Utf8, default=None).alias("empreinte_trimestre")
    )


def feuilles_declaration_cta(declaration: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """Onglets xlsx de la déclaration : Résumé (avec empreinte), Par taux, Détail."""
    resume = agreger_resume(
        declaration,
        cle_assiette="turpe_fixe_eur",
        cle_montant="cta_eur",
        nom_assiette_total="turpe_fixe_total_eur",
        nom_montant_total="cta_total_eur",
    ).join(declaration.select("trimestre", "empreinte_trimestre").unique(), on="trimestre", how="left")
    par_taux = agreger_par_taux(
        declaration,
        cle_taux="taux_cta_pct",
        cle_assiette="turpe_fixe_eur",
        cle_montant="cta_eur",
        extra_groupby=("trimestre",),
    )
    return {
        "Résumé": resume.with_columns(pl.lit(VERSION_EMPREINTE).alias("version_empreinte")),
        "Par taux": par_taux,
        "Détail": declaration,
    }
