"""Sources Odoo normalisées consommées par les builds core (ADR-0019, issues #108, #144).

Expose les fonctions d'extraction Odoo qui fournissent les frames attendus par
les builds `core/builds/` (taxes : `rapport_taxe.py` ; facturation :
`rapport_facturation.py`, `contexte_mensuel.py`). Aucune logique métier —
lecture et normalisation uniquement.
"""

from datetime import date

import polars as pl

from .helpers import commandes_lignes, query
from .reader import OdooReader


def lignes_factures_taxe(odoo: OdooReader) -> pl.LazyFrame:
    """Lignes de commandes Odoo normalisées pour le calcul d'accise.

    Equivalent de l'extraction dans l'ancien `accise_par_contrat` :
    retourne le `LazyFrame` brut des lignes de commandes (shape compatible
    avec `pipeline_accise`).

    Args:
        odoo: `OdooReader` déjà ouvert.

    Returns:
        `LazyFrame` des lignes de commandes (colonnes Odoo brutes).
    """
    return commandes_lignes(odoo).lazy()


def mapping_pdl_order(odoo: OdooReader) -> pl.DataFrame:
    """Mapping PDL → nom de commande Odoo (pour enrichissement CTA).

    Extrait les `sale.order` ayant un `x_pdl`, retourne un DataFrame
    `{pdl, order_name}` dédupliqué par PDL.

    Args:
        odoo: `OdooReader` déjà ouvert.

    Returns:
        `DataFrame` avec colonnes `pdl` (str) et `order_name` (str).
    """
    return (
        query(odoo, "sale.order", domain=[("x_pdl", "!=", False)], fields=["name", "x_pdl"])
        .filter(pl.col("x_pdl").is_not_null())
        .select(
            pl.col("x_pdl").str.strip_chars().alias("pdl"),
            pl.col("name").alias("order_name"),
        )
        .collect()
        .unique("pdl")
    )


def _expr_est_brouillon() -> pl.Expr:
    """Expression : `est_brouillon = x_invoicing_state == 'draft' ∧ account.move.state == 'draft'`.

    Surface le statut « brouillon facturable » côté Odoo vers la colonne agnostique
    consommée par `core.builds.contexte_mensuel.rapprocher` (cf. ADR-0014 +
    slice 2 de la refonte Contexte mensuel). Les flags dérivés (`a_facturer`,
    `a_supprimer`) sont calculés en core depuis cet `est_brouillon` et `quantite`.
    """
    return ((pl.col("x_invoicing_state") == "draft") & (pl.col("state_account_move") == "draft")).alias("est_brouillon")


def date_ancre(mois: str) -> str:
    """Date-ancre de la campagne de facturation du mois cible (#561, ADR-0054).

    Odoo pose `invoice_date = 05/(M+1)` sur tous les brouillons de la campagne
    du mois `mois` au moment de « lancer la facturation du mois » (conso de
    juin → factures datées 05/07). Convention inter-systèmes : Odoo la pose,
    electricore la lit par égalité stricte — si l'un des deux bascule l'ancre,
    l'autre casse silencieusement (d'où le check pré-campagne, #564, qui
    réutilise ce calcul pour l'« ancre courante »).

    Args:
        mois: Premier jour du mois cible au format "YYYY-MM-DD".

    Returns:
        Date-ancre au format "YYYY-MM-DD" (05 du mois suivant ; rollover
        décembre géré : conso de décembre N → 05/01/(N+1)).
    """
    d = date.fromisoformat(mois)
    annee, mois_suivant = (d.year + 1, 1) if d.month == 12 else (d.year, d.month + 1)
    return date(annee, mois_suivant, 5).isoformat()


def lignes_factures_du_mois(odoo: OdooReader, mois: str, domain: list | None = None) -> pl.LazyFrame:
    """Lignes de factures Odoo du mois cible, sélectionnées par date-ancre (#561, ADR-0054).

    La campagne du mois cible est entièrement datée à l'ancre `date_ancre(mois)`
    (posée par Odoo à la génération) : la sélection est une égalité stricte sur
    `invoice_date`, pas une fenêtre. Un brouillon sans date ou daté ailleurs est
    une violation de la convention — elle n'est pas absorbée ici silencieusement,
    elle est détectée en amont par le check pré-campagne (#564).

    Source du chemin facturation (déménagée de `helpers.py`, #144 — symétrie
    avec les sources taxes ci-dessus). Aucun filtre sur `x_invoicing_state` ni
    `account.move.state` côté Odoo : les distinctions métier sont matérialisées
    en core, à partir de `est_brouillon` et `quantite` (cf. ADR-0014). La sortie
    respecte le schéma agnostique `LignesFacture` : clés métier renommées
    (`ref_situation_contractuelle`, `categorie_produit`, `quantite`,
    `est_brouillon`) et identifiants ERP passe-plat conservés tels quels.

    Args:
        odoo: Instance OdooReader connectée.
        mois: Premier jour du mois cible au format "YYYY-MM-DD".
        domain: Filtres additionnels sur `sale.order`.

    Returns:
        LazyFrame Polars `LignesFacture`-compatible : une ligne par
        `account.move.line` de la campagne du mois cible.
    """
    ancre = date_ancre(mois)
    base_domain = [("state", "=", "sale")] + (domain or [])

    return (
        query(
            odoo,
            "sale.order",
            domain=base_domain,
            fields=[
                "name",
                "state",  # nécessaire pour forcer le rename de account.move.state → state_account_move
                "x_pdl",
                "x_ref_situation_contractuelle",
                "x_lisse",
                "x_invoicing_state",
                "invoice_ids",
            ],
        )
        .follow(
            "invoice_ids",
            # Égalité stricte sur la date-ancre de la campagne (#561, ADR-0054).
            domain=[("invoice_date", "=", ancre)],
            fields=["name", "invoice_date", "state", "invoice_line_ids"],
        )
        .follow("invoice_line_ids", fields=["name", "product_id", "quantity"])
        .follow("product_id", fields=["name", "categ_id"])
        .enrich("categ_id", fields=["name"])
        .lazy()
        .with_columns(_expr_est_brouillon())
        .rename(
            {
                "x_ref_situation_contractuelle": "ref_situation_contractuelle",
                "name_product_category": "categorie_produit",
                "quantity": "quantite",
            }
        )
    )
