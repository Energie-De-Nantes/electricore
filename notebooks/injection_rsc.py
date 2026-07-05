import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl
    import logging
    from pathlib import Path
    import sys

    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    import os

    import httpx

    from electricore.integrations.odoo import OdooReader, query
    from electricore_client.arrow import ElectricoreArrowClient as ElectricoreClient

    logging.basicConfig(level=logging.INFO)

    from electricore.config import charger_config_odoo

    try:
        config = charger_config_odoo()
        _msg = mo.md(f"**Configuration Odoo chargée** — `{config['url']}`")
    except ValueError as e:
        config = {}
        _msg = mo.md(f"⚠️ **Configuration Odoo manquante**\n\n{e}\n\nDéfinissez `ODOO__*` dans `.env`.")

    # Client HTTP vers l'API electricore (cf. ADR-0009 : notebooks consomment l'API,
    # pas la base DuckDB locale).
    _api_url = os.getenv("ELECTRICORE_API_URL", "https://electricore.localhost")
    _api_key = os.getenv("ELECTRICORE_API_KEY") or os.getenv("API_KEYS", "").split(",")[0].strip()
    _http_client = httpx.Client(verify=False, timeout=httpx.Timeout(30.0, read=120.0))
    client = ElectricoreClient(url=_api_url, api_key=_api_key, http_client=_http_client)
    _msg


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Attribution des ref_situation_contractuelle aux sale.order

    Ce notebook identifie la `ref_situation_contractuelle` (RSC) Enedis correspondant
    à chaque `sale.order` Odoo actif (`state = sale`).

    **Règle d'attribution (#580)** : la RSC du PDL dont le **dernier état C15** n'est pas
    `RESILIE`. Un PDL dont le contrat a été re-créé (RES→MES le même jour, CFN entrant…)
    change de RSC active sans que la commande ne bouge — un asof join sur `date_order`
    fige à tort la RSC contemporaine de la souscription et ne se répare jamais. L'asof
    reste affiché ci-dessous à titre de **diagnostic/départage**, plus comme source
    d'attribution.

    0 ou plusieurs RSC en service sur le PDL → commande listée à part, **jamais
    d'écriture silencieuse**.

    **Lecture seule** — aucune écriture dans Odoo.
    """)
    return


@app.cell
def _():
    mo.md("""
    ## 1. Contrats C15 (une ligne par RSC)
    """)
    return


@app.cell(hide_code=True)
def _():
    # Lecture C15 via l'API (cf. ADR-0009). Le serveur retourne un DataFrame collecté ;
    # on agrège ensuite localement. `dernier_etat_contractuel` (#580) : l'état C15 à
    # l'événement le plus récent de la RSC (EN SERVICE / RESILIE) — `.unique(keep="last")`
    # sur un flux trié par date_evenement, même pattern que `compteur_par_rsc` dans
    # contexte_mensuel.py.
    _c15 = client.flux("c15").sort("date_evenement")

    _debuts = _c15.group_by(["pdl", "ref_situation_contractuelle"]).agg(
        pl.col("date_evenement").min().alias("date_debut_contrat")
    )
    _derniers_etats = (
        _c15.unique(subset=["pdl", "ref_situation_contractuelle"], keep="last")
        .select(["pdl", "ref_situation_contractuelle", "etat_contractuel"])
        .rename({"etat_contractuel": "dernier_etat_contractuel"})
    )

    contrats_par_pdl = (
        _debuts.join(_derniers_etats, on=["pdl", "ref_situation_contractuelle"], how="left")
        # Supprimer la timezone pour compatibilité avec les dates Odoo
        .with_columns(pl.col("date_debut_contrat").dt.replace_time_zone(None))
        .sort(["pdl", "date_debut_contrat"])
    )
    mo.vstack(
        [
            mo.md(f"**{contrats_par_pdl['pdl'].n_unique()} PDLs**, **{len(contrats_par_pdl)} contrats** dans le C15"),
            mo.ui.table(contrats_par_pdl.head(10)),
        ]
    )
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
        orders_df = query(
            _odoo,
            "sale.order",
            domain=[("x_pdl", "!=", False), ("state", "=", "sale")],
            fields=["name", "x_pdl", "date_order", "partner_id"],
        ).collect()
    # Normaliser date_order en Datetime naïf (Odoo envoie UTC en string)
    orders_df = orders_df.with_columns(pl.col("date_order").str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False))
    mo.vstack(
        [
            mo.md(f"**{len(orders_df)} sale.orders** avec PDL"),
            mo.ui.table(orders_df.head(10)),
        ]
    )
    return (orders_df,)


@app.cell
def _():
    mo.md("""
    ## 3. Asof join : PDL + date_order → RSC (diagnostic)

    Contemporain de la souscription, pas de l'état actuel du PDL — gardé pour le
    départage visuel, plus utilisé comme source d'attribution (#580, cf. section 3bis).
    """)
    return


@app.cell
def _(contrats_par_pdl, orders_df):
    # Renommer pdl → x_pdl pour aligner les noms de colonnes des deux côtés
    # (évite que join_asof ajoute une colonne 'pdl' en doublon dans le résultat)
    _contrats = contrats_par_pdl.rename({"pdl": "x_pdl"}).sort("date_debut_contrat")
    _orders_sorted = orders_df.sort("date_order")

    orders_avec_rsc = _orders_sorted.join_asof(
        _contrats,
        left_on="date_order",
        right_on="date_debut_contrat",
        by="x_pdl",
        strategy="nearest",
    ).select(["sale_order_id", "name", "x_pdl", "date_order", "ref_situation_contractuelle"])
    return (orders_avec_rsc,)


@app.cell
def _(orders_avec_rsc):
    _ok = orders_avec_rsc.filter(pl.col("ref_situation_contractuelle").is_not_null())
    _sans_rsc = orders_avec_rsc.filter(pl.col("ref_situation_contractuelle").is_null())

    mo.vstack(
        [
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
        ]
    )
    return


@app.cell
def _(orders_avec_rsc):
    _rsc_counts = (
        orders_avec_rsc.filter(pl.col("ref_situation_contractuelle").is_not_null())
        .group_by("ref_situation_contractuelle")
        .agg(pl.len().alias("n"), pl.col("sale_order_id").alias("orders"))
        .filter(pl.col("n") > 1)
        .sort("n", descending=True)
    )
    mo.vstack(
        [
            mo.md(f"### RSC en doublon : {len(_rsc_counts)} RSC partagées par plusieurs orders"),
            mo.ui.table(
                orders_avec_rsc.filter(
                    pl.col("ref_situation_contractuelle").is_in(_rsc_counts["ref_situation_contractuelle"])
                ).sort("ref_situation_contractuelle")
            ),
        ]
    )
    return


@app.cell
def _():
    mo.md(r"""
    ## 3bis. RSC en service par PDL (règle d'attribution, #580)

    Pour chaque PDL, les RSC dont le **dernier état C15** n'est pas `RESILIE`. Exactement
    une candidate → attribuée à la commande. Zéro ou plusieurs → **à traiter à part**,
    jamais d'écriture silencieuse.
    """)
    return


@app.cell
def _(contrats_par_pdl):
    _en_service = contrats_par_pdl.filter(pl.col("dernier_etat_contractuel") != "RESILIE")
    rsc_en_service_par_pdl = _en_service.group_by("pdl").agg(
        pl.col("ref_situation_contractuelle").alias("rsc_en_service"),
        pl.len().alias("nb_rsc_en_service"),
    )
    return (rsc_en_service_par_pdl,)


@app.cell
def _(orders_avec_rsc, rsc_en_service_par_pdl):
    # Attribution cible (#580) : ignore l'asof (`rsc_asof`, gardé en diagnostic) — une seule
    # RSC en service sur le PDL est attribuée ; 0 ou plusieurs → ref_situation_contractuelle
    # null (jamais d'écriture silencieuse, cf. cellules 4 ci-dessous).
    orders_attribution = (
        orders_avec_rsc.rename({"ref_situation_contractuelle": "rsc_asof"})
        .join(rsc_en_service_par_pdl, left_on="x_pdl", right_on="pdl", how="left")
        .with_columns(pl.col("nb_rsc_en_service").fill_null(0))
        .with_columns(
            pl.when(pl.col("nb_rsc_en_service") == 1)
            .then(pl.col("rsc_en_service").list.first())
            .otherwise(None)
            .alias("ref_situation_contractuelle")
        )
    )
    return (orders_attribution,)


@app.cell
def _(orders_attribution):
    _attribuees = orders_attribution.filter(pl.col("nb_rsc_en_service") == 1)
    _a_part = orders_attribution.filter(pl.col("nb_rsc_en_service") != 1).with_columns(
        pl.when(pl.col("nb_rsc_en_service") == 0)
        .then(pl.lit("aucune RSC en service sur le PDL"))
        .otherwise(pl.lit("plusieurs RSC en service sur le PDL"))
        .alias("motif")
    )
    mo.vstack(
        [
            mo.md(f"""
        ### Résultats de l'attribution

        | | |
        |---|---|
        | ✅ Attribuées (1 RSC en service) | **{len(_attribuees)}** orders |
        | ⚠️ À traiter à part (0 ou plusieurs) | **{len(_a_part)}** orders |
        """),
            mo.ui.table(
                _attribuees.select(["sale_order_id", "name", "x_pdl", "rsc_asof", "ref_situation_contractuelle"])
            ),
            mo.md("### Commandes à traiter à part (jamais d'écriture silencieuse)")
            if not _a_part.is_empty()
            else mo.md(""),
            mo.ui.table(_a_part.select(["sale_order_id", "name", "x_pdl", "motif", "nb_rsc_en_service"]))
            if not _a_part.is_empty()
            else mo.md(""),
        ]
    )
    return


@app.cell
def _():
    mo.md("""
    ## 4. Injection RSC → Odoo
    """)
    return


@app.cell
def _(orders_attribution):
    from electricore.integrations.odoo import OdooWriter

    _a_injecter = orders_attribution.filter(pl.col("nb_rsc_en_service") == 1)
    _a_part = orders_attribution.filter(pl.col("nb_rsc_en_service") != 1)

    sim_mode = mo.ui.checkbox(label="Mode simulation (aucune écriture réelle)", value=True)
    run_button = mo.ui.run_button(label="Injecter dans Odoo")

    mo.vstack(
        [
            mo.md(
                f"**{len(_a_injecter)}** orders à mettre à jour · "
                f"**{len(_a_part)}** à traiter à part (ignorés, jamais d'écriture silencieuse)"
            ),
            mo.ui.table(_a_injecter.select(["name", "x_pdl", "date_order", "ref_situation_contractuelle"])),
            sim_mode,
            run_button,
        ]
    )
    return OdooWriter, run_button, sim_mode


@app.cell
def _(OdooWriter, orders_attribution, run_button, sim_mode):
    mo.stop(not run_button.value, mo.md("Vérifiez les données ci-dessus puis cliquez sur **Injecter**."))

    _records = (
        orders_attribution.filter(pl.col("nb_rsc_en_service") == 1)
        .rename({"sale_order_id": "id", "ref_situation_contractuelle": "x_ref_situation_contractuelle"})
        .select(["id", "x_ref_situation_contractuelle"])
        .to_dicts()
    )

    with OdooWriter(config=config, sim=sim_mode.value) as _writer:
        _writer.update("sale.order", _records)

    _label = "simulés" if sim_mode.value else "écrits"
    mo.callout(mo.md(f"✅ **{len(_records)} sale.orders** {_label} (`x_ref_situation_contractuelle`)."), kind="success")
    return


if __name__ == "__main__":
    app.run()
