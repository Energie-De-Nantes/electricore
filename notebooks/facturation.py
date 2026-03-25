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

    """Configuration Odoo depuis secrets.toml"""
    import tomllib

    # Chercher le fichier secrets.toml
    secrets_paths = [
        Path.cwd() / '.dlt' / 'secrets.toml',
        Path.cwd() / 'electricore' / 'etl' / '.dlt' / 'secrets.toml'
    ]

    config = {}
    secrets_file_found = None

    for secrets_path in secrets_paths:
        if secrets_path.exists():
            with open(secrets_path, 'rb') as f:
                config_data = tomllib.load(f)
                config = config_data.get('odoo', {})
                secrets_file_found = secrets_path
            break

    if not config:
        _msg = mo.md("""
        ⚠️ **Configuration Odoo non trouvée**

        Créez le fichier `.dlt/secrets.toml` ou `electricore/etl/.dlt/secrets.toml` avec :
        ```toml
        [odoo]
        url = "https://votre-instance.odoo.com"
        db = "votre_database"
        username = "votre_username"
        password = "votre_password"
        ```
        """)
    else:
        _msg = mo.md(f"""
        **Configuration chargée depuis**: `{secrets_file_found}`

        - URL: `{config.get('url', 'NON CONFIGURÉ')}`
        - Base: `{config.get('db', 'NON CONFIGURÉ')}`
        - Utilisateur: `{config.get('username', 'NON CONFIGURÉ')}`
        - Mot de passe: `{'***' if config.get('password') else 'NON CONFIGURÉ'}`
        """)
    _msg


@app.cell
def _():
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
    lf_releves = releves_harmonises().lazy()
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
    # Réconciliation Enedis → Odoo
    """)
    return


@app.cell
def _(fact_mois):
    _pdl_counts = fact_mois.group_by("pdl").agg(pl.len().alias("n"))
    pdls_doublons = _pdl_counts.filter(pl.col("n") > 1)["pdl"].to_list()

    fact_simple   = fact_mois.filter(~pl.col("pdl").is_in(pdls_doublons))
    fact_doublons = fact_mois.filter(pl.col("pdl").is_in(pdls_doublons))
    fact_doublons
    return fact_doublons, fact_simple, pdls_doublons


@app.cell
def _():
    MAPPING_CATEGORIE = {
        "HP":          "energie_hp_kwh",
        "HC":          "energie_hc_kwh",
        "Base":        "energie_base_kwh",
        "Abonnements": "nb_jours",
    }
    return (MAPPING_CATEGORIE,)


@app.cell
def _(MAPPING_CATEGORIE, fact_simple, lignes_a_facturer_df, pdls_doublons):
    _cats = list(MAPPING_CATEGORIE.items())
    _expr = pl.when(pl.col("name_product_category") == _cats[0][0]).then(pl.col(_cats[0][1]).cast(pl.Float64))
    for _cat, _col in _cats[1:]:
        _expr = _expr.when(pl.col("name_product_category") == _cat).then(pl.col(_col).cast(pl.Float64))
    _quantite_enedis = _expr.otherwise(pl.lit(None, dtype=pl.Float64)).alias("quantite_enedis")

    updates = (
        lignes_a_facturer_df
        .filter(~pl.col("x_pdl").is_in(pdls_doublons))
        .join(fact_simple, left_on="x_pdl", right_on="pdl", how="left")
        .with_columns(_quantite_enedis)
        .select([
            "invoice_line_ids", "x_pdl", "name_account_move",
            "name_product_category", "name_product_product",
            "quantity", "quantite_enedis",
        ])
    )
    mo.ui.table(updates)
    return (updates,)


@app.cell
def _(fact_doublons, pdls_doublons, updates):
    _sans_match = updates.filter(pl.col("quantite_enedis").is_null())
    mo.vstack([
        mo.md(f"⚠️ **{len(pdls_doublons)} PDL(s) multi-contrats exclus** — révision manuelle requise")
          if pdls_doublons else mo.md(""),
        mo.md(f"❌ **{_sans_match['x_pdl'].n_unique()} PDL(s) sans correspondance Enedis**")
          if not _sans_match.is_empty() else mo.md("✅ Tous les PDLs ont une correspondance Enedis"),
        mo.ui.table(fact_doublons) if pdls_doublons else mo.md(""),
    ])
    return


if __name__ == "__main__":
    app.run()
