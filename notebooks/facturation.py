import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup:
    import logging
    import os
    import sys
    from pathlib import Path

    import httpx
    import marimo as mo
    import polars as pl

    # Ajouter le chemin vers electricore
    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    from electricore.client import ElectricoreClient
    from electricore.core.loaders import OdooReader
    from electricore.core.loaders.odoo import lignes_a_facturer, lignes_quantite_zero

    # Configuration du logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    """Configuration Odoo + ElectricoreClient depuis .env"""
    from electricore.config import charger_config_odoo

    try:
        config = charger_config_odoo()
        _env = os.getenv("ODOO_ENV", "test")
        config_msg = mo.md(
            f"**Configuration Odoo chargée** (env: `{_env}`)\n\n"
            f"- URL: `{config['url']}`\n- Base: `{config['db']}`\n"
            f"- Utilisateur: `{config['username']}`\n- Mot de passe: `***`"
        )
    except ValueError as e:
        config = {}
        config_msg = mo.md(
            f"⚠️ **Configuration Odoo manquante**\n\n{e}\n\nDéfinissez les variables `ODOO_*` dans `.env`."
        )

    # Client API electricore (cf. ADR-0009 + ADR-0012)
    _api_url = os.getenv("ELECTRICORE_API_URL", "https://electricore.localhost")
    _api_key = os.getenv("ELECTRICORE_API_KEY", "")
    # verify=False : Caddy local TLS auto-signé (cf. docs/deploiement.md)
    _http_client = httpx.Client(verify=False, timeout=httpx.Timeout(30.0, read=120.0))
    client = ElectricoreClient(url=_api_url, api_key=_api_key, http_client=_http_client)


@app.cell
def _():
    config_msg
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Récupération des données Odoo

    `OdooReader` reste utilisé en notebook pour la lecture des lignes Odoo et
    la persistance via `OdooWriter` (cf. ADR-0012). Les flux Enedis et le
    rapprochement passent par l'API.
    """)
    return


@app.cell
def _():
    with OdooReader(config=config) as _odoo:
        lignes_a_facturer_df = (
            lignes_a_facturer(_odoo)
            .filter(pl.col("name_product_category").is_in(["Abonnements", "HP", "HC", "Base"]))
            .collect()
            .select(
                [
                    "sale_order_id",
                    "name",
                    "x_pdl",
                    "x_lisse",
                    "x_ref_situation_contractuelle",
                    "invoice_ids",
                    "invoice_line_ids",
                    "quantity",
                    "name_product_product",
                    "name_account_move",
                    "name_product_category",
                ]
            )
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

    Un seul appel à `client.facturation(mois)` qui retourne déjà
    `lignes_facture_rapprochees` (Odoo × Enedis × compteur) calculé côté serveur.

    Sélectionne le mois (laisser vide → dernier mois disponible côté serveur).
    """)
    return


@app.cell
def _():
    mois_input = mo.ui.text(label="Mois (YYYY-MM-DD)", placeholder="vide = dernier mois disponible")
    mois_input
    return (mois_input,)


@app.cell
def _(mois_input):
    _mois = mois_input.value or None
    fact_mois = client.facturation(mois=_mois)
    mo.md(f"**{len(fact_mois)}** lignes récupérées · mois demandé: `{_mois or 'dernier disponible'}`")
    return (fact_mois,)


@app.cell
def _(fact_mois):
    fact_mois
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Déménagements

    PDLs avec plusieurs `ref_situation_contractuelle` distinctes dans le mois.
    Dédoublonnage sur `(pdl, ref_situation_contractuelle)` requis (la table
    `fact_mois` a une ligne par invoice_line, pas par RSC).
    """)
    return


@app.cell
def _(fact_mois):
    _pdl_rsc = fact_mois.select(["pdl", "ref_situation_contractuelle"]).unique()
    _pdl_counts = _pdl_rsc.group_by("pdl").agg(pl.len().alias("n"))
    pdls_doublons = _pdl_counts.filter(pl.col("n") > 1)["pdl"].to_list()

    fact_déménagements = fact_mois.filter(pl.col("pdl").is_in(pdls_doublons))
    mo.ui.table(fact_déménagements) if pdls_doublons else mo.md("✅ Aucun déménagement détecté")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Réconciliation Enedis → Odoo

    `fact_mois` (= `lignes_facture_rapprochees`) contient déjà le rapprochement
    par `invoice_line_ids`. Plus besoin de joindre côté client : on enrichit
    juste avec les colonnes Odoo non exposées par l'API (`sale_order_id`,
    `invoice_ids`).
    """)
    return


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
def _(fact_mois):
    fact_mois.filter(pl.col("memo_puissance") != "")
    return


@app.cell
def _(fact_mois):
    _sans_match = fact_mois.filter(pl.col("quantite_enedis").is_null())
    mo.md(
        f"❌ **{_sans_match['x_pdl'].n_unique()} PDL(s) sans correspondance Enedis**"
    ) if not _sans_match.is_empty() else mo.md("✅ Tous les PDLs ont une correspondance Enedis")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Préparation des quantités → Odoo
    """)
    return


@app.cell
def _():
    filtre_a_injecter = pl.col("quantite_enedis").is_not_null() & ~pl.col("x_lisse").fill_null(False)
    return (filtre_a_injecter,)


@app.cell
def _(fact_mois, filtre_a_injecter):
    from electricore.core.writers import OdooWriter

    _a_injecter = fact_mois.filter(filtre_a_injecter)
    _sans_match = fact_mois.filter(pl.col("quantite_enedis").is_null())

    sim_mode = mo.ui.checkbox(label="Mode simulation (aucune écriture réelle)", value=True)
    run_button = mo.ui.run_button(label="Injecter dans Odoo")

    mo.vstack(
        [
            mo.md(
                f"**{len(_a_injecter)}** lignes à mettre à jour "
                f"· **{len(_sans_match)}** sans correspondance Enedis (ignorées)"
            ),
            mo.ui.table(
                _a_injecter.select(
                    [
                        "name_account_move",
                        "x_pdl",
                        "name_product_category",
                        "name_product_product",
                        "quantity",
                        "quantite_enedis",
                        "memo_puissance",
                    ]
                )
            ),
            sim_mode,
            run_button,
        ]
    )
    return OdooWriter, run_button, sim_mode


@app.cell
def _(fact_mois, filtre_a_injecter):
    lines_records = (
        fact_mois.filter(filtre_a_injecter)
        .select(
            [
                pl.col("invoice_line_ids").cast(pl.Int64).alias("id"),
                pl.col("quantite_enedis").alias("quantity"),
            ]
        )
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
        0,
        20,
        value=5,
        step=1,
        label="% d'orders à vérifier (→ populated)",
        show_value=True,
    )
    taux_verification
    return (taux_verification,)


@app.cell
def _(fact_mois, lignes_a_facturer_df, taux_verification):
    import numpy as np

    # Côté Odoo : sale_order_id + x_lisse, unique par RSC
    _odoo_df = lignes_a_facturer_df.select(["sale_order_id", "x_lisse", "x_ref_situation_contractuelle"]).unique()
    # Côté Enedis : data_complete par RSC (déduplication car fact_mois a 1 ligne / invoice_line)
    _enedis_df = fact_mois.select(["data_complete", "ref_situation_contractuelle"]).unique()

    _df = (
        _odoo_df.join(
            _enedis_df,
            left_on="x_ref_situation_contractuelle",
            right_on="ref_situation_contractuelle",
            how="left",
        )
        .with_columns((pl.col("data_complete") | pl.col("x_lisse")).alias("a_jour"))
        .with_columns(pl.Series("rand", np.random.rand(len(_odoo_df))))
    )

    orders_records = (
        _df.with_columns(
            pl.when(~pl.col("a_jour"))
            .then(pl.lit("draft"))
            .when(pl.col("rand") < taux_verification.value / 100)
            .then(pl.lit("populated"))
            .otherwise(pl.lit("checked"))
            .alias("x_invoicing_state")
        )
        .select(["sale_order_id", "x_invoicing_state"])
        .rename({"sale_order_id": "id"})
        .to_dicts()
    )

    _preview = pl.DataFrame(orders_records)
    mo.vstack(
        [
            mo.md(
                f"**{_preview.filter(pl.col('x_invoicing_state') == 'draft')['id'].len()}** draft · "
                f"**{_preview.filter(pl.col('x_invoicing_state') == 'populated')['id'].len()}** populated · "
                f"**{_preview.filter(pl.col('x_invoicing_state') == 'checked')['id'].len()}** checked"
            ),
            mo.ui.table(_preview),
        ]
    )
    return (orders_records,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Préparation données factures
    """)
    return


@app.cell
def _(fact_mois, lignes_a_facturer_df):
    # Côté Odoo : invoice_ids unique par RSC
    _odoo_df = lignes_a_facturer_df.select(["invoice_ids", "x_ref_situation_contractuelle"]).unique()

    # Côté Enedis : 1 ligne par RSC avec debut/fin/turpe/compteur (dédoublonnage : fact_mois a 1 ligne / invoice_line)
    _enedis_df = fact_mois.select(
        [
            "ref_situation_contractuelle",
            "debut",
            "fin",
            "turpe_fixe_eur",
            "turpe_variable_eur",
            "num_compteur",
            "type_compteur",
        ]
    ).unique()

    _to_rename = {
        "invoice_ids": "id",
        "turpe": "x_turpe",
        "debut": "x_start_invoice_period",
        "fin": "x_end_invoice_period",
        "type_compteur": "x_type_compteur",
        "num_compteur": "x_num_serie_compteur",
    }

    _df = (
        _odoo_df.join(
            _enedis_df,
            left_on="x_ref_situation_contractuelle",
            right_on="ref_situation_contractuelle",
            how="left",
        )
        .with_columns(
            [
                (pl.col("turpe_fixe_eur") + pl.col("turpe_variable_eur")).alias("turpe"),
                pl.col("debut").dt.strftime("%Y-%m-%d"),
                pl.col("fin").dt.strftime("%Y-%m-%d"),
            ]
        )
        .drop(["turpe_fixe_eur", "turpe_variable_eur", "x_ref_situation_contractuelle"])
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
