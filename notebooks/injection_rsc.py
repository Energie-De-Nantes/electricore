import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl
    import logging
    from pathlib import Path
    import sys
    import tomllib

    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    from electricore.core.loaders import OdooReader, c15
    from electricore.core.loaders.odoo import query

    logging.basicConfig(level=logging.INFO)

    secrets_paths = [
        Path.cwd() / '.dlt' / 'secrets.toml',
        Path.cwd() / 'electricore' / 'etl' / '.dlt' / 'secrets.toml'
    ]

    config = {}
    secrets_file_found = None

    for secrets_path in secrets_paths:
        if secrets_path.exists():
            with open(secrets_path, 'rb') as f:
                config = tomllib.load(f).get('odoo', {})
                secrets_file_found = secrets_path
            break

    _msg = mo.md(f"**Config** : `{secrets_file_found}`") if config else mo.md("⚠️ **Configuration Odoo non trouvée**")
    _msg


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Attribution des ref_situation_contractuelle aux sale.order

    Ce notebook identifie la `ref_situation_contractuelle` (RSC) Enedis correspondant
    à chaque `sale.order` Odoo, via un **asof join temporel** sur le PDL et la date.

    **Mécanisme** : pour un order créé à `date_order` sur le PDL `x_pdl`,
    la RSC active est celle dont la date d'entrée C15 est la plus récente avant `date_order`.

    **Lecture seule** — aucune écriture dans Odoo.
    """)
    return


@app.cell
def _():
    mo.md("""
    ## 1. Contrats C15 (une ligne par RSC)
    """)
    return


@app.cell
def _():
    contrats_par_pdl = (
        c15().lazy()
        .group_by(["pdl", "ref_situation_contractuelle"])
        .agg(pl.col("date_evenement").min().alias("date_debut_contrat"))
        .collect()
        # Supprimer la timezone pour compatibilité avec les dates Odoo
        .with_columns(pl.col("date_debut_contrat").dt.replace_time_zone(None))
        .sort(["pdl", "date_debut_contrat"])
    )
    mo.vstack([
        mo.md(f"**{contrats_par_pdl['pdl'].n_unique()} PDLs**, **{len(contrats_par_pdl)} contrats** dans le C15"),
        mo.ui.table(contrats_par_pdl.head(10)),
    ])
    return (contrats_par_pdl,)


@app.cell
def _():
    mo.md("""
    ## 2. Sale orders Odoo avec PDL
    """)
    return


@app.cell
def _():
    with OdooReader(config=config) as _odoo:
        orders_df = (
            query(_odoo, 'sale.order',
                  domain=[('x_pdl', '!=', False), ('state', '=', 'sale')],
                  fields=['name', 'x_pdl', 'date_order', 'partner_id'])
            .collect()
        )
    # Normaliser date_order en Datetime naïf (Odoo envoie UTC en string)
    orders_df = orders_df.with_columns(
        pl.col("date_order").str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False)
    )
    mo.vstack([
        mo.md(f"**{len(orders_df)} sale.orders** avec PDL"),
        mo.ui.table(orders_df.head(10)),
    ])
    return (orders_df,)


@app.cell
def _():
    mo.md("""
    ## 3. Asof join : PDL + date_order → RSC
    """)
    return


@app.cell
def _(contrats_par_pdl, orders_df):
    # Renommer pdl → x_pdl pour aligner les noms de colonnes des deux côtés
    # (évite que join_asof ajoute une colonne 'pdl' en doublon dans le résultat)
    _contrats = (
        contrats_par_pdl
        .rename({"pdl": "x_pdl"})
        .sort("date_debut_contrat")
    )
    _orders_sorted = orders_df.sort("date_order")

    orders_avec_rsc = (
        _orders_sorted
        .join_asof(
            _contrats,
            left_on="date_order",
            right_on="date_debut_contrat",
            by="x_pdl",
            strategy="nearest",
        )
        .select(["sale_order_id", "name", "x_pdl", "date_order",
                 "ref_situation_contractuelle"])
    )
    return (orders_avec_rsc,)


@app.cell
def _(orders_avec_rsc):
    _ok       = orders_avec_rsc.filter(pl.col("ref_situation_contractuelle").is_not_null())
    _sans_rsc = orders_avec_rsc.filter(pl.col("ref_situation_contractuelle").is_null())

    mo.vstack([
        mo.md(f"""
        ### Résultats

        | | |
        |---|---|
        | ✅ Matchés | **{len(_ok)}** orders |
        | ❌ Sans correspondance C15 | **{len(_sans_rsc)}** orders |
        """),
        mo.ui.table(orders_avec_rsc),
        mo.md("### Orders sans correspondance C15") if not _sans_rsc.is_empty() else mo.md(""),
        mo.ui.table(_sans_rsc) if not _sans_rsc.is_empty() else mo.md(""),
    ])
    return


@app.cell
def _(orders_avec_rsc):
    _rsc_counts = (
        orders_avec_rsc
        .filter(pl.col("ref_situation_contractuelle").is_not_null())
        .group_by("ref_situation_contractuelle")
        .agg(pl.len().alias("n"), pl.col("sale_order_id").alias("orders"))
        .filter(pl.col("n") > 1)
        .sort("n", descending=True)
    )
    mo.vstack([
        mo.md(f"### RSC en doublon : {len(_rsc_counts)} RSC partagées par plusieurs orders"),
        mo.ui.table(
            orders_avec_rsc.filter(
                pl.col("ref_situation_contractuelle").is_in(_rsc_counts["ref_situation_contractuelle"])
            ).sort("ref_situation_contractuelle")
        ),
    ])
    return


if __name__ == "__main__":
    app.run()
