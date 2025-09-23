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


@app.cell
def explore_sale_orders(config, connection_ok):
    """Exploration des commandes de vente avec PDL"""
    if not connection_ok:
        _msg = mo.md("🛑 **Arrêt**: Impossible de se connecter à Odoo.")
        orders = None
        nb_orders = 0
    else:
        _intro = mo.md("""
        ## 2. Exploration des commandes de vente (sale.order)

        Recherchons les commandes qui contiennent un PDL dans le champ `x_pdl`.
        """)

        with OdooReader(config=config) as _odoo:
            # Récupérer quelques sale.order avec PDL
            orders = _odoo.search_read(
                'sale.order',
                domain=[['x_pdl', '!=', False]],  # Seulement ceux avec PDL
                fields=['name', 'x_pdl', 'partner_id', 'date_order', 'invoice_ids', 'state']
            )

            nb_orders = len(orders)

            if nb_orders > 0:
                # Afficher les premières lignes
                display_orders = orders.head(5) if nb_orders > 5 else orders
                _msg = mo.vstack([
                    _intro,
                    mo.md(f"**Résultat**: {nb_orders} commandes trouvées avec un PDL"),
                    mo.as_html(display_orders)
                ])
            else:
                _msg = mo.vstack([
                    _intro,
                    mo.md("⚠️ Aucune commande trouvée avec un PDL (champ `x_pdl`)")
                ])

    _msg
    return orders, nb_orders


@app.cell
def select_sample_order(orders, nb_orders):
    """Sélection d'une commande d'exemple pour explorer les factures"""
    if nb_orders == 0:
        _msg = mo.md("🛑 **Pas de commandes disponibles**")
        sample_order = None
    else:
        _intro = mo.md("""
        ## 3. Sélection d'une commande avec factures

        Trouvons une commande qui a des factures associées pour explorer la structure.
        """)

        # Prendre la première commande qui a des factures
        sample_order = None
        for row in orders.iter_rows(named=True):
            if row['invoice_ids'] and len(row['invoice_ids']) > 0:
                sample_order = row
                break

        if sample_order:
            _msg = mo.vstack([
                _intro,
                mo.md(f"""
                **Commande sélectionnée**: `{sample_order['name']}`
                - **PDL**: `{sample_order['x_pdl']}`
                - **Client**: {sample_order['partner_id'][1] if sample_order['partner_id'] else 'N/A'}
                - **Factures**: {len(sample_order['invoice_ids'])} facture(s) associée(s)
                - **IDs factures**: {sample_order['invoice_ids']}
                """)
            ])
        else:
            _msg = mo.vstack([
                _intro,
                mo.md("⚠️ Aucune commande trouvée avec des factures associées")
            ])

    _msg
    return sample_order


@app.cell
def explore_invoices(config, sample_order):
    """Exploration des factures associées à la commande"""
    if not sample_order:
        _msg = mo.md("🛑 **Pas de commande sélectionnée**")
        invoices = None
    else:
        _intro = mo.md("""
        ## 4. Exploration des factures (account.move)

        Explorons les factures associées à notre commande d'exemple.
        """)

        with OdooReader(config=config) as _odoo:
            # Récupérer les détails des factures
            invoice_ids = sample_order['invoice_ids']
            invoices = _odoo.read(
                'account.move',
                invoice_ids,
                fields=['name', 'invoice_date', 'amount_total', 'state', 'move_type', 'invoice_line_ids']
            )

            _msg = mo.vstack([
                _intro,
                mo.md(f"**Factures de la commande `{sample_order['name']}`**:"),
                mo.as_html(invoices)
            ])

    _msg
    return invoices


@app.cell
def explore_invoice_lines(config, invoices):
    """Exploration des lignes de facture"""
    if invoices is None or len(invoices) == 0:
        _msg = mo.md("🛑 **Pas de factures disponibles**")
        sample_invoice = None
        lines = None
    else:
        # Prendre la première facture
        sample_invoice = invoices.row(0, named=True)
        invoice_line_ids = sample_invoice['invoice_line_ids']

        _intro = mo.md("""
        ## 5. Exploration des lignes de facture (account.move.line)

        Explorons les lignes de la première facture pour voir le détail des produits/services.
        """)

        _invoice_info = mo.md(f"""
        **Facture sélectionnée**: `{sample_invoice['name']}`
        - **Date**: {sample_invoice['invoice_date']}
        - **Montant total**: {sample_invoice['amount_total']}€
        - **Lignes**: {len(invoice_line_ids)} ligne(s)
        """)

        if invoice_line_ids:
            with OdooReader(config=config) as _odoo:
                # Récupérer les détails des lignes de facture
                lines = _odoo.read(
                    'account.move.line',
                    invoice_line_ids,
                    fields=[
                        'name', 'product_id', 'quantity', 'price_unit',
                        'price_subtotal', 'price_total', 'account_id'
                    ]
                )

                _msg = mo.vstack([
                    _intro,
                    _invoice_info,
                    mo.md("**Lignes de facture**:"),
                    mo.as_html(lines)
                ])
        else:
            lines = None
            _msg = mo.vstack([
                _intro,
                _invoice_info,
                mo.md("⚠️ Aucune ligne de facture trouvée")
            ])

    _msg
    return sample_invoice, lines


@app.cell
def analyze_structure(sample_order, sample_invoice, lines):
    """Analyse de la structure des données"""
    if not all([sample_order, sample_invoice, lines]):
        _msg = mo.md("🛑 **Données incomplètes pour l'analyse**")
    else:
        _intro = mo.md("""
        ## 6. Analyse de la structure des données

        Basé sur l'exploration, voici la structure recommandée pour extraire les factures par PDL.
        """)

        # Analyser les types de produits
        if lines is not None:
            products_summary = (
                lines
                .select(['product_id', 'name', 'quantity', 'price_unit', 'price_total'])
                .with_columns([
                    pl.when(pl.col('product_id').is_not_null())
                    .then(pl.col('product_id').list.get(1))  # Nom du produit depuis many2one
                    .otherwise(pl.col('name'))
                    .alias('product_name')
                ])
            )

            _summary = mo.md(f"""
            ## 7. Structure cible du DataFrame

            **Données extraites de l'exemple**:
            - **PDL**: `{sample_order['x_pdl']}`
            - **Commande**: `{sample_order['name']}`
            - **Facture**: `{sample_invoice['name']}`
            - **Date**: `{sample_invoice['invoice_date']}`
            - **Lignes**: {len(lines)} produits/services

            **Structure proposée pour le DataFrame final**:
            ```
            | pdl | order_ref | invoice_ref | invoice_date | product_name | quantity | price_unit | price_total |
            ```
            """)

            _msg = mo.vstack([
                _intro,
                mo.md("**Types de produits/services facturés**:"),
                mo.as_html(products_summary.select(['product_name', 'quantity', 'price_unit', 'price_total'])),
                _summary
            ])

    _msg
    return


@app.cell
def extraction_function():
    """Définition de la fonction d'extraction complète"""
    mo.md("""
    ## 8. Fonction d'extraction complète

    Voici la fonction qui extrait toutes les factures par PDL dans le format souhaité.
    """)

    def extract_factures_par_pdl(odoo_connector, limit=None):
        """
        Extrait toutes les factures par PDL dans un DataFrame Polars.

        Args:
            odoo_connector: Instance OdooReader
            limit: Limiter le nombre de commandes (pour test)

        Returns:
            pl.DataFrame: DataFrame avec colonnes pdl, order_ref, invoice_ref,
                         invoice_date, product_name, quantity, price_unit, price_total
        """
        results = []

        # 1. Récupérer toutes les commandes avec PDL
        domain = [['x_pdl', '!=', False]]

        orders = odoo_connector.search_read(
            'sale.order',
            domain=domain,
            fields=['name', 'x_pdl', 'invoice_ids']
        )

        orders_to_process = orders.head(limit) if limit else orders

        # 2. Pour chaque commande, traiter ses factures
        for order_row in orders_to_process.iter_rows(named=True):
            pdl = order_row['x_pdl']
            order_name = order_row['name']
            invoice_ids = order_row['invoice_ids']

            if not invoice_ids:
                continue

            # 3. Récupérer les factures
            invoices = odoo_connector.read(
                'account.move',
                invoice_ids,
                fields=['name', 'invoice_date', 'invoice_line_ids', 'state']
            )

            # 4. Pour chaque facture, traiter ses lignes
            for invoice_row in invoices.iter_rows(named=True):
                if invoice_row['state'] not in ['posted']:  # Seulement les factures validées
                    continue

                invoice_ref = invoice_row['name']
                invoice_date = invoice_row['invoice_date']
                line_ids = invoice_row['invoice_line_ids']

                if not line_ids:
                    continue

                # 5. Récupérer les lignes de facture
                lines = odoo_connector.read(
                    'account.move.line',
                    line_ids,
                    fields=['product_id', 'name', 'quantity', 'price_unit', 'price_total']
                )

                # 6. Ajouter chaque ligne au résultat
                for line_row in lines.iter_rows(named=True):
                    product_name = (
                        line_row['product_id'][1] if line_row['product_id']
                        else line_row['name']
                    )

                    results.append({
                        'pdl': pdl,
                        'order_ref': order_name,
                        'invoice_ref': invoice_ref,
                        'invoice_date': invoice_date,
                        'product_name': product_name,
                        'quantity': line_row['quantity'],
                        'price_unit': line_row['price_unit'],
                        'price_total': line_row['price_total']
                    })

        return pl.DataFrame(results)

    mo.md("✅ Fonction `extract_factures_par_pdl` définie")
    return


@app.cell
def test_extraction(config, connection_ok, extract_factures_par_pdl):
    """Test de la fonction d'extraction complète"""
    if not connection_ok:
        _msg = mo.md("🛑 **Pas de connexion disponible**")
        df_test = None
    else:
        _intro = mo.md("""
        ## 9. Test de l'extraction complète

        Testons la fonction d'extraction sur un échantillon limité.
        """)

        try:
            with OdooReader(config=config) as _odoo:
                # Test avec limite de 3 commandes
                df_test = extract_factures_par_pdl(_odoo, limit=3)

                if len(df_test) > 0:
                    # Statistiques rapides
                    stats = {
                        'nb_pdl_uniques': df_test['pdl'].n_unique(),
                        'nb_factures_uniques': df_test['invoice_ref'].n_unique(),
                        'montant_total': df_test['price_total'].sum(),
                        'nb_lignes': len(df_test)
                    }

                    _stats = mo.md(f"""
                    **Statistiques de l'extraction**:
                    - **PDL uniques**: {stats['nb_pdl_uniques']}
                    - **Factures uniques**: {stats['nb_factures_uniques']}
                    - **Lignes extraites**: {stats['nb_lignes']}
                    - **Montant total**: {stats['montant_total']:.2f}€
                    """)

                    _msg = mo.vstack([
                        _intro,
                        mo.md(f"**Résultat du test** ({len(df_test)} lignes extraites):"),
                        mo.as_html(df_test.head(10)),
                        _stats
                    ])
                else:
                    _msg = mo.vstack([
                        _intro,
                        mo.md("⚠️ Aucune donnée extraite. Vérifiez les filtres et l'état des factures.")
                    ])

        except Exception as e:
            df_test = None
            _msg = mo.vstack([
                _intro,
                mo.md(f"❌ **Erreur lors du test**: {e}")
            ])

    _msg
    return df_test


@app.cell
def next_steps(df_test):
    """Prochaines étapes"""
    if df_test is not None and len(df_test) > 0:
        mo.md("""
        ## 10. Prochaines étapes

        ✅ **Exploration réussie !**

        La structure des données Odoo a été validée. Vous pouvez maintenant :

        ### Étapes suivantes :

        1. **Configurer les secrets dlt**
           ```toml
           # .dlt/secrets.toml
           [odoo]
           url = "https://votre-instance.odoo.com"
           db = "votre_db"
           username = "votre_username"
           password = "votre_password"
           ```

        2. **Créer une source dlt** (`electricore/etl/sources/odoo_factures.py`)
           - Utiliser le connecteur Odoo
           - Intégrer la logique d'extraction
           - Ajouter le chargement incrémental

        3. **Pipeline vers DuckDB** (`electricore/etl/pipeline_odoo.py`)
           - Charger les données dans DuckDB
           - Modes test et production

        4. **Intégration avec Polars**
           ```python
           # Jointure avec données Enedis
           factures = pl.read_database("SELECT * FROM odoo_factures", conn)
           energie = pl.scan_parquet("energie_polars.parquet")
           result = factures.join(energie, on="pdl")
           ```

        ### Structure validée :
        **Format long** avec tous les détails de facturation par PDL, parfait pour :
        - Analyses détaillées par type de produit/service
        - Jointures avec données Enedis
        - Agrégations flexibles selon les besoins
        """)
    else:
        mo.md("""
        ## 10. Résolution des problèmes

        L'extraction n'a pas fonctionné. Vérifiez :

        - **Connexion Odoo** : Paramètres de connexion corrects
        - **Données disponibles** : Commandes avec PDL et factures validées
        - **Permissions** : Accès aux modèles `sale.order`, `account.move`, `account.move.line`
        - **Structure des données** : Champ `x_pdl` existe-t-il vraiment ?
        """)
    return


if __name__ == "__main__":
    app.run()
