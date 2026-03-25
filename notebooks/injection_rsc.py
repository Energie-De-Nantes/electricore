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
    mo.md("## 1. Contrats C15 (une ligne par RSC)")
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
    mo.md("## 2. Sale orders Odoo avec PDL")
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
    mo.md("## 3. Asof join : PDL + date_order → RSC")
    return


@app.cell
def _(contrats_par_pdl, orders_df):
    # Stratégie backward : RSC dont la date d'entrée est la plus récente AVANT date_order
    _orders_sorted = orders_df.sort("date_order")
    _contrats_sorted = contrats_par_pdl.sort("date_debut_contrat")

    _backward = _orders_sorted.join_asof(
        _contrats_sorted,
        left_on="date_order",
        right_on="date_debut_contrat",
        by_left="x_pdl",
        by_right="pdl",
        strategy="backward",
    ).with_columns(
        pl.when(pl.col("ref_situation_contractuelle").is_not_null())
          .then(pl.lit("backward"))
          .alias("match_strategy")
    )

    # Fallback nearest : pour les orders créés avant le premier C15 du PDL
    _sans_rsc = _backward.filter(pl.col("ref_situation_contractuelle").is_null())
    _nearest = (
        _sans_rsc.drop(["ref_situation_contractuelle", "match_strategy"])
        .join_asof(
            _contrats_sorted,
            left_on="date_order",
            right_on="date_debut_contrat",
            by_left="x_pdl",
            by_right="pdl",
            strategy="nearest",
        )
        .with_columns(
            pl.when(pl.col("ref_situation_contractuelle").is_not_null())
              .then(pl.lit("nearest"))
              .alias("match_strategy")
        )
    )

    orders_avec_rsc = (
        pl.concat([
            _backward.filter(pl.col("ref_situation_contractuelle").is_not_null()),
            _nearest,
        ])
        .sort("date_order")
        .select(["sale_order_id", "name", "x_pdl", "date_order",
                 "ref_situation_contractuelle", "match_strategy"])
    )
    return (orders_avec_rsc,)


@app.cell
def _(orders_avec_rsc):
    _ok       = orders_avec_rsc.filter(pl.col("ref_situation_contractuelle").is_not_null())
    _nearest  = orders_avec_rsc.filter(pl.col("match_strategy") == "nearest")
    _sans_rsc = orders_avec_rsc.filter(pl.col("ref_situation_contractuelle").is_null())

    mo.vstack([
        mo.md(f"""
        ### Résultats

        | | |
        |---|---|
        | ✅ Matchés (backward) | **{len(_ok) - len(_nearest)}** orders |
        | ⚠️ Matchés (nearest — order avant C15) | **{len(_nearest)}** orders |
        | ❌ Sans correspondance C15 | **{len(_sans_rsc)}** orders |
        """),
        mo.ui.table(orders_avec_rsc),
        mo.md("### Orders sans correspondance C15") if not _sans_rsc.is_empty() else mo.md(""),
        mo.ui.table(_sans_rsc) if not _sans_rsc.is_empty() else mo.md(""),
    ])
    return


if __name__ == "__main__":
    app.run()
