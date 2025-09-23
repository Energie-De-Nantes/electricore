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
    return (connection_ok,)


@app.cell(hide_code=True)
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
                fields=['name', 'x_pdl', 'partner_id', 'invoice_ids', 'state']
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
    return nb_orders, orders


@app.cell(hide_code=True)
def _():
    min_invoices_slider = mo.ui.slider(
        start=0,
        stop=20,
        step=1,
        value=0,
        label=f"Commande à explorer (parmi commandes avec factures)"
    )
    return (min_invoices_slider,)


@app.cell
def select_sample_order(min_invoices_slider, nb_orders, orders):
    """Sélection d'une commande d'exemple pour explorer les factures"""
    if nb_orders == 0:
        _msg = mo.md("🛑 **Pas de commandes disponibles**")
        sample_order = None
    else:
        _intro = mo.md("""
        ## 3. Sélection d'une commande avec factures

        Trouvons une commande qui a des factures associées pour explorer la structure.
        """)
        sample_order = None
        # Filtrer les commandes qui ont des factures
        for row in orders.iter_rows(named=True):
            if row['invoice_ids'] and len(row['invoice_ids']) > min_invoices_slider.value:
                sample_order = row

        if sample_order:
            _msg = mo.vstack([
                _intro,
                min_invoices_slider,
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
                min_invoices_slider,
                mo.md(f"Pas d'abonnement avec min {min_invoices_slider.value} factures trouvé")
            ])
    _msg
    return (sample_order,)


@app.cell(hide_code=True)
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
    return (invoices,)


@app.cell(hide_code=True)
def explore_invoice_lines(config, invoices, orders):
    """Exploration des lignes de facture"""
    if invoices is None or len(invoices) == 0:
        _msg = mo.md("🛑 **Pas de factures disponibles**")
        sample_invoice = None
        lines = None
    else:
        # Récupérer TOUTES les lignes de TOUTES les factures
        _intro = mo.md("""
        ## 5. Exploration des lignes de facture (account.move.line)

        Récupérons toutes les lignes de toutes les factures pour avoir une vue d'ensemble.
        """)

        # Collecter tous les IDs de lignes de factures
        all_invoice_line_ids = []
        for _row in invoices.iter_rows(named=True):
            if _row['invoice_line_ids']:
                all_invoice_line_ids.extend(_row['invoice_line_ids'])

        _invoice_info = mo.md(f"""
        **Toutes les factures analysées**: {len(invoices)} facture(s)
        - **Total lignes de factures**: {len(all_invoice_line_ids)} ligne(s)
        """)

        if all_invoice_line_ids:
            with OdooReader(config=config) as _odoo:
                # Approche propre avec explode() et join()

                # 1. Explode orders sur invoice_ids -> une ligne par facture
                orders_exploded = orders.explode('invoice_ids').rename({'invoice_ids': 'invoice_id'})

                # 2. Récupérer les factures depuis Odoo (filtrer les None)
                unique_invoice_ids = [id for id in orders_exploded['invoice_id'].unique().to_list() if id is not None]
                if not unique_invoice_ids:
                    lines_long = pl.DataFrame()
                else:
                    invoices_df = _odoo.read('account.move', unique_invoice_ids,
                                           fields=['name', 'invoice_date', 'amount_total', 'invoice_line_ids'])

                    # Renommer les colonnes pour éviter les conflits
                    invoices_df = invoices_df.rename({'name': 'invoice_name'})

                    # 3. Join orders + invoices
                    df_with_invoices = orders_exploded.join(
                        invoices_df,
                        left_on='invoice_id',
                        right_on='account_move_id'
                    )

                    # 4. Explode sur invoice_line_ids -> une ligne par ligne de facture
                    df_exploded = df_with_invoices.explode('invoice_line_ids').rename({'invoice_line_ids': 'line_id'})

                    # 5. Récupérer les lignes depuis Odoo (filtrer les None)
                    unique_line_ids = [id for id in df_exploded['line_id'].unique().to_list() if id is not None]
                    if not unique_line_ids:
                        lines_long = pl.DataFrame()
                    else:
                        lines_df = _odoo.read('account.move.line', unique_line_ids,
                                            fields=['name', 'product_id', 'quantity', 'price_unit', 'price_total'])

                        # Renommer les colonnes pour éviter les conflits
                        lines_df = lines_df.rename({'name': 'line_name'})

                        # 6. Join final pour le DataFrame long complet
                        lines_long = df_exploded.join(
                            lines_df,
                            left_on='line_id',
                            right_on='account_move_line_id'
                        )

                        # 7. Nettoyer et sélectionner les colonnes finales
                        lines_long = lines_long.select([
                            pl.col('x_pdl').alias('pdl'),
                            pl.col('name').alias('order_name'),
                            pl.col('invoice_name'),
                            pl.col('invoice_date'),
                            pl.when(pl.col('product_id').is_not_null())
                            .then(pl.col('product_id').list.get(1))
                            .otherwise(pl.col('line_name'))
                            .alias('product_name'),
                            pl.col('quantity'),
                            pl.col('price_unit'),
                            pl.col('price_total')
                        ])

                _msg = mo.vstack([
                    _intro,
                    _invoice_info,
                    mo.md("**Format long : avec explode() et join() Polars**:"),
                    mo.as_html(lines_long.head(10))
                ])
        else:
            lines_long = None
            _msg = mo.vstack([
                _intro,
                _invoice_info,
                mo.md("⚠️ Aucune ligne de facture trouvée")
            ])

        # Pour la compatibilité, on prend la première facture comme échantillon
        sample_invoice = invoices.row(0, named=True) if len(invoices) > 0 else None

    _msg
    return (lines_long,)


@app.cell
def analyze_structure(lines_long):
    """Analyse de la structure des données"""

    _intro = mo.md("""
    ## 6. Analyse de la structure des données

    Basé sur l'exploration, voici la structure recommandée pour extraire les factures par PDL.
    """)

    # Analyser les types de produits
    if lines_long is not None:
        _summary = mo.md(f"""
        ## 6. Structure du DataFrame long créé

        **Format long avec toutes les informations contextuelles** :
    
        - ✅ **PDL** : Point de livraison depuis la commande
        - ✅ **Commande** : Numéro de commande (order_name)
        - ✅ **Facture** : Numéro et date de facture
        - ✅ **Produit** : Nom et ID du produit/service
        - ✅ **Quantité/Prix** : Détails de facturation

        **Colonnes du DataFrame** :
        ```
        | pdl | order_name | invoice_name | invoice_date | product_name | quantity | price_unit | price_total |
        ```

        **Aperçu des données** :
        """)

        _msg = mo.vstack([
            _intro,
            _summary,
            mo.as_html(lines_long.head(10))
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
    return (extract_factures_par_pdl,)


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
    return (df_test,)


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
