import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl
    import logging
    from pathlib import Path
    import sys

    # Ajouter le chemin vers electricore
    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    from electricore.core.loaders import OdooReader
    from electricore.core.loaders.odoo import query, lignes_a_facturer, lignes_quantite_zero
    from electricore.core.loaders import c15, releves_harmonises
    from electricore.core.pipelines.orchestration import facturation
    # Configuration du logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    """Configuration Odoo depuis .env"""
    import os
    from electricore.config import charger_config_odoo

    try:
        config = charger_config_odoo()
        _env = os.getenv("ODOO_ENV", "test")
        config_msg = mo.md(f"**Configuration Odoo chargée** (env: `{_env}`)\n\n- URL: `{config['url']}`\n- Base: `{config['db']}`\n- Utilisateur: `{config['username']}`\n- Mot de passe: `***`")
    except ValueError as e:
        config = {}
        config_msg = mo.md(f"⚠️ **Configuration Odoo manquante**\n\n{e}\n\nDéfinissez les variables `ODOO_*` dans `.env`.")


@app.cell
def _():
    config_msg
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Récupération des données Odoo
    """)
    return


@app.cell
def _():
    with OdooReader(config=config) as _odoo:
        lignes_a_facturer_df = (
            lignes_a_facturer(_odoo)
            .filter(pl.col('name_product_category').is_in(['Abonnements', 'HP', 'HC', 'Base']))
            .collect()
            .select(['sale_order_id',
                     'name',
                     'x_pdl',
                     'x_lisse',
                     'x_ref_situation_contractuelle', 
                     'invoice_ids',
                     'invoice_line_ids', 
                     'quantity',
                     'name_product_product',
                     'name_account_move',
                     'name_product_category'])
        )
    lignes_a_facturer_df
    return (lignes_a_facturer_df,)


@app.cell
def _():
    with OdooReader(config=config) as _odoo:
        lignes_a_supprimer = lignes_quantite_zero(_odoo).collect()
    lignes_a_supprimer
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Récupération des données Enedis
    """)
    return


@app.cell
def _():
    lf_historique = c15().lazy()
    lf_releves = releves_harmonises().lazy() # releves_harmonises = R151 et R64 fusionnés
    return lf_historique, lf_releves


@app.cell
def _(lf_historique, lf_releves):
    hist, abo, energie, fact = facturation(
        historique=lf_historique,
        releves=lf_releves,
    )
    return (fact,)


@app.cell
def _(fact):
    mois_en_cours = fact["mois_annee"][fact["debut"].arg_max()]
    mo.md(f"**Mois en cours** : {mois_en_cours}")
    return (mois_en_cours,)


@app.cell
def _(fact, mois_en_cours):
    fact_mois = fact.filter(pl.col("mois_annee") == mois_en_cours)
    fact_mois
    return (fact_mois,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Déménagements

    On détecte ici les déménagements du mois, en regardant les pdls qui ont deux ref_contractuelles. Les factures associées sont normalement correctement peuplées, mais il faut potentiellement vérifier.
    """)
    return


@app.cell
def _(fact_mois):
    _pdl_counts = fact_mois.group_by("pdl").agg(pl.len().alias("n"))
    pdls_doublons = _pdl_counts.filter(pl.col("n") > 1)["pdl"].to_list()

    fact_déménagements = fact_mois.filter(pl.col("pdl").is_in(pdls_doublons))
    mo.ui.table(fact_déménagements) if pdls_doublons else mo.md("")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Réconciliation Enedis → Odoo
    """)
    return


@app.cell
def _():
    MAPPING_CATEGORIE = {
        "HP":          "energie_hp_kwh",
        "HC":          "energie_hc_kwh",
        "Base":        "energie_base_kwh",
        "Abonnements": "nb_jours",
    }
    return (MAPPING_CATEGORIE,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Changements de puissance en cours de mois

    Lignes de facturation dont le PDL a changé de puissance souscrite pendant le mois.
    La quantité Enedis est une puissance moyenne pondérée par le nombre de jours — à vérifier
    avant validation de la facture.
    """)
    return


@app.cell
def _(updates_rsc):
    updates_rsc.filter(pl.col('memo_puissance') != '')
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Version RSC (cible — une fois `x_ref_situation_contractuelle` peuplé dans Odoo)

    Join direct sur `ref_situation_contractuelle` : gère nativement les déménagements
    (deux contrats sur le même PDL en cours de mois) sans exclusion manuelle.

    **Prérequis** : champ `x_ref_situation_contractuelle` créé et peuplé sur `sale.order`
    via le notebook `injection_rsc.py`.
    """)
    return


@app.cell
def _(MAPPING_CATEGORIE, fact_mois, lignes_a_facturer_df):
    _quantite_enedis_rsc = pl.coalesce([
        pl.when(pl.col("name_product_category") == cat).then(pl.col(col).cast(pl.Float64))
        for cat, col in MAPPING_CATEGORIE.items()
    ]).alias("quantite_enedis")

    updates_rsc = (
        lignes_a_facturer_df
        .join(
            fact_mois,
            left_on="x_ref_situation_contractuelle",
            right_on="ref_situation_contractuelle",
            how="left",
        )
        .with_columns(_quantite_enedis_rsc)
        .select([
            "invoice_line_ids", "x_pdl", "x_lisse", "name_account_move",
            "name_product_category", "name_product_product",
            "quantity", "quantite_enedis", "memo_puissance",
        ])
    )
    mo.ui.table(updates_rsc)
    return (updates_rsc,)


@app.cell
def _(updates_rsc):
    _sans_match_rsc = updates_rsc.filter(pl.col("quantite_enedis").is_null())
    mo.md(f"❌ **{_sans_match_rsc['x_pdl'].n_unique()} PDL(s) sans correspondance Enedis**") \
        if not _sans_match_rsc.is_empty() \
        else mo.md("✅ Tous les PDLs ont une correspondance Enedis")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Préparation des quantités → Odoo
    """)
    return


@app.cell
def _(updates_rsc):
    from electricore.core.writers import OdooWriter

    # TODO filtrer les lissés
    _a_injecter = updates_rsc.filter(pl.col("quantite_enedis").is_not_null())
    _sans_match  = updates_rsc.filter(pl.col("quantite_enedis").is_null())

    sim_mode   = mo.ui.checkbox(label="Mode simulation (aucune écriture réelle)", value=True)
    run_button = mo.ui.run_button(label="Injecter dans Odoo")

    mo.vstack([
        mo.md(f"**{len(_a_injecter)}** lignes à mettre à jour "
              f"· **{len(_sans_match)}** sans correspondance Enedis (ignorées)"),
        mo.ui.table(_a_injecter.select([
            "name_account_move", "x_pdl", "name_product_category",
            "name_product_product", "quantity", "quantite_enedis", "memo_puissance",
        ])),
        sim_mode,
        run_button,
    ])
    return OdooWriter, run_button, sim_mode


@app.cell
def _(updates_rsc):
    lines_records = (
        updates_rsc
        .filter(pl.col("quantite_enedis").is_not_null())
        .select([
            pl.col("invoice_line_ids").cast(pl.Int64).alias("id"),
            pl.col("quantite_enedis").alias("quantity"),
        ])
        .to_dicts()
    )
    return (lines_records,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Préparation statut abonnement
    """)
    return


@app.cell
def _():
    taux_verification = mo.ui.slider(
        0, 20, value=5, step=1,
        label="% d'orders à vérifier (→ populated)",
        show_value=True,
    )
    taux_verification
    return (taux_verification,)


@app.cell
def _(fact_mois, lignes_a_facturer_df, taux_verification):
    import numpy as np

    _odoo_df = (
        lignes_a_facturer_df
        .select(['sale_order_id', 'x_lisse', 'x_ref_situation_contractuelle'])
        .unique()
    )
    _enedis_df = fact_mois.select(['data_complete', 'ref_situation_contractuelle'])

    _df = (
        _odoo_df
        .join(_enedis_df, left_on="x_ref_situation_contractuelle",
              right_on="ref_situation_contractuelle", how="left")
        .with_columns((pl.col("data_complete") | pl.col("x_lisse")).alias("a_jour"))
        .with_columns(pl.Series("rand", np.random.rand(len(_odoo_df))))
    )

    orders_records = (
        _df
        .with_columns(
            pl.when(~pl.col("a_jour"))
              .then(pl.lit("draft"))
              .when(pl.col("rand") < taux_verification.value / 100)
              .then(pl.lit("populated"))
              .otherwise(pl.lit("checked"))
              .alias("x_invoicing_state")
        )
        .select(['sale_order_id', 'x_invoicing_state'])
        .rename({"sale_order_id": "id"})
        .to_dicts()
    )

    _preview = pl.DataFrame(orders_records)
    mo.vstack([
        mo.md(f"**{_preview.filter(pl.col('x_invoicing_state') == 'draft')['id'].len()}** draft · "
              f"**{_preview.filter(pl.col('x_invoicing_state') == 'populated')['id'].len()}** populated · "
              f"**{_preview.filter(pl.col('x_invoicing_state') == 'checked')['id'].len()}** checked"),
        mo.ui.table(_preview),
    ])
    return (orders_records,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Préparation données factures
    """)
    return


@app.cell
def _(fact_mois, lf_historique, lignes_a_facturer_df):
    _histo_df = (
        lf_historique
        .collect()
        .select(['ref_situation_contractuelle', 'num_compteur', "type_compteur"])
    )

    _odoo_df = (
        lignes_a_facturer_df
        .select(['invoice_ids', 'x_ref_situation_contractuelle'])
        .unique()
    )

    _enedis_df = fact_mois.select(
        ['ref_situation_contractuelle', 
         'debut', 'fin', 
         'turpe_fixe_eur', 'turpe_variable_eur'])

    _to_rename = {
        'invoice_ids':'id',
        'turpe':'x_turpe',
        'debut':'x_start_invoice_period',
        'fin':'x_end_invoice_period',
        'type_compteur':'x_type_compteur',
        'num_compteur':'x_num_serie_compteur',
    }

    _df = (
        _odoo_df
        .join(_enedis_df, 
              left_on="x_ref_situation_contractuelle",
              right_on="ref_situation_contractuelle", 
              how="left")
        .join(_histo_df, 
              left_on="x_ref_situation_contractuelle",
              right_on="ref_situation_contractuelle",
              how="left")
        .with_columns([
            (pl.col('turpe_fixe_eur') + pl.col('turpe_variable_eur')).alias('turpe'),
            pl.col('debut').dt.strftime("%Y-%m-%d"),
            pl.col('fin').dt.strftime("%Y-%m-%d"),
        ])
        .drop(['turpe_fixe_eur', 'turpe_variable_eur', 'x_ref_situation_contractuelle'])
        .rename(mapping=_to_rename)
    )
    invoices_records = _df.to_dicts()
    _df
    return (invoices_records,)


@app.cell
def _(
    OdooWriter,
    invoices_records,
    lines_records,
    orders_records,
    run_button,
    sim_mode,
):
    mo.stop(not run_button.value, mo.md("Vérifiez les données ci-dessus puis cliquez sur **Injecter**."))

    with OdooWriter(config=config, sim=sim_mode.value) as _writer:
        _writer.update("account.move.line", lines_records)
        _writer.update("account.move", invoices_records)
        _writer.update("sale.order", orders_records)

    _label = "simulées" if sim_mode.value else "mises à jour"
    mo.callout(
        mo.md(f"✅ **{len(lines_records)} lignes** account.move.line {_label} (`quantity`)."),
        kind="success",
    )
    return


if __name__ == "__main__":
    app.run()
