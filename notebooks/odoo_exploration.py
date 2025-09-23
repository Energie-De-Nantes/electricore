import marimo

__generated_with = "0.16.0"
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

    from electricore.etl.connectors.odoo import OdooReader

    # Configuration du logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)


@app.cell
def _():
    mo.md(
        r"""
    # Exploration des données Odoo

    Ce notebook explore la connexion à Odoo et la structure des données
    pour extraire les factures par PDL.
    """
    )
    return


@app.cell
def _():
    mo.md(
        r"""
    ## 1. Configuration de connexion à Odoo

    ⚠️ **Important**: Modifiez les paramètres ci-dessous selon votre environnement Odoo.
    En production, utilisez plutôt les secrets dlt dans `.dlt/secrets.toml`.
    """
    )
    return


@app.cell(hide_code=True)
def config_odoo():
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
    return (config,)


@app.cell(hide_code=True)
def test_connexion(config):
    """Test de connexion à Odoo"""
    try:
        with OdooReader(config=config) as _odoo:
            _msg = mo.md("✅ **Connexion réussie** à Odoo !")
            connection_ok = True
    except Exception as e:
        _msg = mo.md(f"""
        ❌ **Erreur de connexion**: {e}

        Vérifiez :
        - URL et port du serveur Odoo
        - Nom de la base de données
        - Identifiants utilisateur
        - Connexion réseau
        """)
        connection_ok = False
    _msg
    return


@app.cell(hide_code=True)
def _():
    min_invoices_slider = mo.ui.slider(
        start=0,
        stop=20,
        step=1,
        value=0,
        label=f"Commande à explorer (parmi commandes avec factures)"
    )
    return


@app.cell
def demo_query_builder(config):
    """Démonstration du Query Builder"""

    _intro = mo.md("""
    ## 2. Démonstration de la nouvelle API Query Builder 🚀

    **Architecture refactorisée** avec séparation claire entre navigation et enrichissement.
    """)

    with OdooReader(config=config) as _odoo:
        # Démonstration des 2 approches : follow() vs enrich()

        # Approche 1: Navigation pure avec follow()
        navigation_result = (
            _odoo.query('sale.order', #domain=[('id', 'in', sample_ids)],
                fields=['name', 'x_pdl', 'partner_id', 'invoice_ids'])
            .follow('invoice_ids',  # Navigate → account.move
                fields=['name', 'invoice_date', 'invoice_line_ids'])
            .follow('invoice_line_ids',  # Navigate → account.move.line
                fields=['name', 'product_id', 'quantity', 'price_unit', 'price_total'])
            .follow('product_id',  # Enrichit avec détails produit
                fields=['name', 'categ_id'])
            .enrich('categ_id',  # Enrichit avec détails produit
                fields=['name',])
            .select([
                pl.col('x_pdl').alias('pdl'),
                pl.col('name').alias('order_name'),
                pl.col('name_account_move').alias('invoice_name'),
                pl.col('invoice_date'),
                pl.col('name_product_product').alias('product_name'),
                pl.col('quantity'),
                pl.col('price_unit'),
                pl.col('price_total'),
                pl.col('name_product_category').alias('categorie')
            ])
            .filter(pl.col('quantity') > 0)
            .collect()
        )

        # Approche 2: Enrichissement sur la commande
        enrichment_result = (
            _odoo.query('sale.order', #domain=[('id', 'in', sample_ids)],
                fields=['name', 'x_pdl', 'partner_id'])
            .enrich('partner_id', fields=['name', 'email'])  # Enrichit partenaire, reste sur sale.order
            .collect()
        )

    _comparison = mo.md(f"""
    ### Nouvelle API avec follow() et enrich()

    **🧭 Navigation avec follow()** (change le modèle courant):
    ```python
    result = (odoo.query('sale.order')
        .follow('invoice_ids')      # → account.move
        .follow('invoice_line_ids') # → account.move.line
        .enrich('product_id')       # Enrichit avec produit
        .collect())
    ```
    Résultat: {navigation_result.shape[0]} lignes × {navigation_result.shape[1]} colonnes

    **🔗 Enrichissement avec enrich()** (garde le modèle courant):
    ```python
    result = (odoo.query('sale.order')
        .enrich('partner_id')       # Ajoute détails partenaire
        .collect())                 # Reste sur sale.order
    ```
    Résultat: {enrichment_result.shape[0]} lignes × {enrichment_result.shape[1]} colonnes

    **✨ Avantages**:
    - **follow()**: Pour explorer/naviguer dans les relations
    - **enrich()**: Pour compléter avec des détails
    - **Auto-détection**: Plus besoin de spécifier les modèles
    - **Flexible**: Compose les 2 approches selon le besoin
    """)

    _msg = mo.vstack([
        _intro,
        _comparison,
        mo.md("**📊 Résultat Navigation (DataFrame long avec détails produits)** :"),
        navigation_result,
        mo.md("**🔗 Résultat Enrichissement (Commandes avec partenaires)** :"),
        enrichment_result
    ])

    _msg
    return


if __name__ == "__main__":
    app.run()
