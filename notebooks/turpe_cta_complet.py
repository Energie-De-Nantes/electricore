"""TURPE + CTA parc complet — sans jointure Odoo (analyse rapide, non embarqué dans le wheel).

Lit directement le DuckDB local via `contexte_du_mois()` : tout le parc du flux
Enedis, tous les mois, `turpe_fixe_eur` / `turpe_variable_eur` + `cta_eur`
(assiette = TURPE fixe). Lancer : `uv run marimo edit notebooks/turpe_cta_complet.py`
"""

import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl

    from electricore.core.builds.contexte_mensuel import contexte_du_mois
    from electricore.core.pipelines.cta import ajouter_cta
    from electricore.core.pipelines.facturation import expr_calculer_trimestre


@app.cell
def _():
    ctx = contexte_du_mois()  # parc entier, tous les mois disponibles
    detail = (
        ajouter_cta(ctx.facturation_mensuelle.lazy())
        .with_columns(expr_calculer_trimestre().alias("trimestre"))
        .select(
            "ref_situation_contractuelle",
            "pdl",
            "mois_annee",
            "trimestre",
            "turpe_fixe_eur",
            "turpe_variable_eur",
            "taux_cta_pct",
            "cta_eur",
        )
        .sort("pdl", "mois_annee")
        .collect()
    )
    mo.md(f"**{detail['pdl'].n_unique()} PDL** × {detail['mois_annee'].n_unique()} mois — {detail.height} lignes")
    return (detail,)


@app.cell
def _(detail):
    par_mois = (
        detail.group_by("mois_annee")
        .agg(
            pl.len().alias("nb_lignes"),
            pl.col("turpe_fixe_eur").sum().round(2),
            pl.col("turpe_variable_eur").sum().round(2),
            pl.col("cta_eur").sum().round(2),
        )
        .sort("mois_annee")
    )
    par_mois
    return (par_mois,)


@app.cell
def _(detail):
    par_trimestre = (
        detail.group_by("trimestre")
        .agg(
            pl.col("turpe_fixe_eur").sum().round(2),
            pl.col("turpe_variable_eur").sum().round(2),
            pl.col("cta_eur").sum().round(2),
        )
        .sort("trimestre")
    )
    par_trimestre
    return (par_trimestre,)


@app.cell
def _(detail):
    detail
    return


@app.cell
def _(detail):
    mo.download(
        detail.write_csv().encode(),
        filename="turpe_cta_complet.csv",
        label="⬇️ Télécharger le détail (CSV)",
    )
    return


if __name__ == "__main__":
    app.run()
