"""Vue read-time des affaires SGE (suivi opérationnel, #276).

Transformation pure roulant les jalons de `flux_affaires` (grain = un jalon) jusqu'à
une vue cockpit : une ligne par affaire *non soldée* (statut COURS), avec son dernier
état et son ancienneté. L'ancienneté dépend de « maintenant » — elle est donc calculée
à la lecture, jamais matérialisée (cf. `electricore/core/CONTEXT.md`, ADR de non-stockage
des dérivés temporels). ERP-agnostique : aucune dépendance Odoo, observabilité read-only.
"""

from datetime import datetime

import polars as pl


def affaires_ouvertes(
    jalons: pl.LazyFrame | pl.DataFrame,
    maintenant: datetime,
    exclure_prestations: tuple[str, ...] = ("AME",),
) -> pl.DataFrame:
    """Roule les jalons en une vue des affaires non soldées (statut COURS).

    Args:
        jalons: lignes de `flux_affaires` (grain = un jalon par affaire).
        maintenant: instant de référence pour l'ancienneté.
        exclure_prestations: codes prestation écartés du cockpit. Par défaut `AME`
            (souscription de flux de données ≈ 45 % du volume, hors périmètre).

    Returns:
        Une ligne par affaire en cours, avec son dernier état (jalon de `num` max) et
        son ancienneté en jours (maintenant − premier jalon).
    """
    lf = jalons.lazy().filter(pl.col("statut") == "COURS")
    if exclure_prestations:
        # is_in(...) renvoie null pour une prestation nulle → fill_null(False) garde la
        # réclamation (prestation absente), seules les prestations exclues sont retirées.
        lf = lf.filter(~pl.col("prestation").is_in(exclure_prestations).fill_null(False))

    return (
        lf.sort("jalon_num")
        .group_by("affaire_id")
        .agg(
            pl.col("origine").last(),
            pl.col("prestation").last(),
            pl.col("prestation_libelle").last(),
            pl.col("pdl").last(),
            pl.col("segment").last(),
            pl.col("affaire_etat").last().alias("dernier_etat"),
            pl.col("affaire_etat_libelle").last().alias("dernier_etat_libelle"),
            pl.col("jalon_date_heure").max().alias("dernier_jalon_date"),
            pl.col("jalon_date_heure").min().alias("premier_jalon_date"),
        )
        .with_columns(
            # Ancienneté = âge de l'affaire (depuis le dépôt) → « bloquée depuis X jours ».
            (pl.lit(maintenant) - pl.col("premier_jalon_date")).dt.total_days().alias("anciennete_jours")
        )
        .collect()
    )
