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
    return


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
    fact
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


if __name__ == "__main__":
    app.run()
