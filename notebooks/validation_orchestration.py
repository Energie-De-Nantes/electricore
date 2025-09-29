import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import marimo as mo
    import polars as pl
    import pandas as pd
    import sys
    from pathlib import Path
    from datetime import datetime, timezone, date
    import time
    import calendar
    from typing import Dict, List, Optional, Tuple

    # Ajouter le chemin du projet
    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    # Imports des loaders DuckDB
    from electricore.core.loaders.duckdb_loader import f15, c15, r151, releves_harmonises, execute_custom_query

    from electricore.core.pipelines import facturation
    from electricore.etl.connectors.odoo import OdooReader
    import tomllib


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Préparation des données""")
    return


@app.cell(hide_code=True)
def load_pipeline_data():
    """Chargement des données pour le pipeline de calcul TURPE variable"""

    mo.md("## 🔧 Calcul TURPE Variable via le pipeline")

    print("🔄 Chargement des données pour le pipeline...")

    # Charger l'historique C15 enrichi
    print("📄 Chargement historique C15...")
    historique_lf = c15().lazy()

    # Charger les relevés R151
    print("📄 Chargement relevés R151...")
    releves_lf = releves_harmonises().lazy()
    return historique_lf, releves_lf


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Calcul du TURPE Variable""")
    return


@app.cell(hide_code=True)
def calculate_turpe_variable(historique_lf, releves_lf):
    # Pipeline complet
    result = facturation(historique_lf, releves_lf)

    # Accéder aux résultats
    print("Historique enrichi:", result.historique_enrichi.collect().shape)
    print("Abonnements:", result.abonnements.collect().shape)
    print("Énergie:", result.energie.collect().shape)
    print("Facturation:", result.facturation.shape)  # Déjà collecté

    # Unpacking pour récupérer les 3 DataFrames
    hist, abo, ener, fact = result
    return abo, ener, fact


@app.cell
def _(abo, ener, fact):
    # Affichage des DataFrames pour inspection
    mo.vstack([
        mo.md("**Facturation (fact)** :"),
        fact.head(5),
        mo.md("**Abonnements (abo)** :"),
        abo.head(5),
        mo.md("**Énergies (ener)** :"),
        ener.head(5)
    ])
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Comparaison avec les factures Odoo""")
    return


@app.cell(hide_code=True)
def _():
    """Configuration Odoo depuis secrets.toml"""
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
        print("⚠️ Configuration Odoo non trouvée")
        print("Créez le fichier .dlt/secrets.toml avec la section [odoo]")
        config = None
    else:
        print(f"✅ Configuration Odoo chargée depuis {secrets_file_found}")
    return (config,)


@app.cell(hide_code=True)
def _(config):
    """Extraction des factures Odoo"""
    if config is None:
        print("❌ Pas de configuration Odoo disponible")
        odoo_factures = None
    else:
        print("🔄 Extraction des factures depuis Odoo...")

        try:
            with OdooReader(config=config) as odoo:
                # Récupérer les commandes avec PDL et factures
                odoo_factures = (
                    odoo.query('sale.order',
                        domain=[('state', '=', 'sale'), ('x_pdl', '!=', False)],
                        fields=['x_pdl', 'name', 'x_lisse', 'invoice_ids'])
                    .follow('invoice_ids',  # Navigation vers account.move
                        fields=['name', 'invoice_date', 'amount_total', 'invoice_line_ids'])
                    .follow('invoice_line_ids',  # Navigation vers account.move.line
                        fields=['name', 'product_id', 'quantity', 'price_unit', 'price_total'])
                    .follow('product_id',  # Enrichissement avec produit
                        fields=['name', 'categ_id'])
                    .enrich('categ_id',  # Enrichissement avec catégorie
                        fields=['name'])
                    .filter(pl.col('price_total') != 0)  # Exclure les lignes nulles
                    .select([
                        pl.col('x_pdl').alias('pdl'),
                        pl.col('name').alias('order_name'),
                        pl.col('x_lisse').alias('lisse'),
                        pl.col('name_account_move').alias('invoice_name'),
                        pl.col('invoice_date'),
                        pl.col('name_product_product').alias('product_name'),
                        pl.col('name_product_category').alias('categorie'),
                        pl.col('quantity'),
                        pl.col('price_unit'),
                        pl.col('price_total'),
                    ])
                    .collect()
                )

            print(f"✅ lignes de factures extraites")

        except Exception as e:
            print(f"❌ Erreur lors de l'extraction Odoo: {e}")
            odoo_factures = None
    return (odoo_factures,)


@app.cell
def _(odoo_factures):
    if odoo_factures is not None:
        odoo_factures.head(10)
    else:
        mo.md("⚠️ Pas de données Odoo disponibles")
    return


@app.cell(hide_code=True)
def _(fact, odoo_factures):
    """Prépare les données physiques pour la comparaison"""
    if odoo_factures is None:
        print("❌ Pas de données Odoo pour la comparaison")
        electricore_agg, odoo_agg = None, None
    else:
        print("🔄 Préparation des données pour la comparaison...")

        # Préparer les données ElectriCore - Utiliser directement les méta-périodes
        # fact contient déjà les quantités physiques agrégées par mois
        electricore_agg = (
            fact.with_columns([
                pl.col('debut').dt.strftime('%Y-%m').alias('periode'),
            ])
            .select([
                'pdl', 'periode',
                # Quantités physiques
                'nb_jours',  # jours d'abonnement
                'puissance_moyenne',  # kVA souscrite
                'base_energie',  # kWh Base
                'hp_energie',   # kWh HP
                'hc_energie',   # kWh HC
                # Montants TURPE (seuls montants à comparer)
                'turpe_fixe',
                'turpe_variable'
            ])
            .rename({
                'nb_jours': 'jours_abo_electricore',
                'puissance_moyenne': 'puissance_electricore',
                'base_energie': 'base_kwh_electricore',
                'hp_energie': 'hp_kwh_electricore',
                'hc_energie': 'hc_kwh_electricore',
                'turpe_fixe': 'turpe_fixe_electricore',
                'turpe_variable': 'turpe_variable_electricore'
            })
        )

        # Préparer les données Odoo - PAR TYPE DE PRODUIT
        odoo_agg = (
            odoo_factures.with_columns([
                # DÉCALAGE D'UN MOIS : date facture Odoo (mois N+1) → période consommation (mois N)
                # Car facture émise le mois suivant la consommation
                pl.col('invoice_date').str.to_date().dt.offset_by('-1mo').dt.strftime('%Y-%m').alias('periode'),
                # Classification des produits par catégorie ou nom
                pl.when(pl.col('categorie').str.contains('(?i)abonnement|fixe'))
                  .then(pl.lit('ABONNEMENT'))
                  .when(pl.col('categorie').str.contains('(?i)hp|pleine'))
                  .then(pl.lit('HP'))
                  .when(pl.col('categorie').str.contains('(?i)hc|creuse'))
                  .then(pl.lit('HC'))
                  .when(pl.col('categorie').str.contains('(?i)base|unique'))
                  .then(pl.lit('BASE'))
                  .when(pl.col('categorie').str.contains('(?i)turpe'))
                  .then(pl.lit('TURPE'))
                  .otherwise(pl.lit('AUTRE'))
                  .alias('type_produit')
            ])
            # FILTRAGE : Exclure les abonnements lissés (écarts normaux non pertinents)
            .filter(
                ~((pl.col('type_produit') == 'ABONNEMENT') & (pl.col('lisse') == True))
            )
            .group_by(['pdl', 'periode', 'type_produit'])
            .agg([
                pl.col('quantity').sum().alias('quantite_odoo'),
                pl.col('price_total').sum().alias('montant_odoo')
            ])
        )

        print(f"✅ ElectriCore: {len(electricore_agg)} PDL×périodes")
        print(f"✅ Odoo: {len(odoo_agg)} PDL×périodes")
    return electricore_agg, odoo_agg


@app.cell(hide_code=True)
def _(electricore_agg, odoo_agg):
    """Crée la comparaison entre ElectriCore et Odoo"""
    if electricore_agg is None or odoo_agg is None:
        comparison = None
    else:
        print("🔄 Jointure des données...")

        # Pour simplifier la comparaison, transformons d'abord odoo_agg en format large
        # On va créer un pivot pour avoir une ligne par PDL+période avec toutes les quantités
        odoo_pivot = (
            odoo_agg
            .pivot(index=['pdl', 'periode'], columns='type_produit', values='quantite_odoo')
            .fill_null(0)
        )

        # Renommer les colonnes Odoo selon les types de produits disponibles
        # Les colonnes seront nommées automatiquement selon les valeurs de type_produit

        # Filtrer les données avec PDL valides avant jointure
        print(f"Avant filtrage - ElectriCore: {len(electricore_agg)} lignes")
        print(f"Avant filtrage - Odoo: {len(odoo_pivot)} lignes")

        # Filtrer ElectriCore : PDL non vide
        electricore_clean = electricore_agg.filter(
            (pl.col('pdl').is_not_null()) &
            (pl.col('pdl') != '') &
            (pl.col('periode').is_not_null())
        )

        # Filtrer Odoo : PDL non vide
        odoo_clean = odoo_pivot.filter(
            (pl.col('pdl').is_not_null()) &
            (pl.col('pdl') != '') &
            (pl.col('periode').is_not_null())
        )

        print(f"Après filtrage - ElectriCore: {len(electricore_clean)} lignes")
        print(f"Après filtrage - Odoo: {len(odoo_clean)} lignes")

        # Jointure sur PDL + période
        comparison = electricore_clean.join(
            odoo_clean,
            on=['pdl', 'periode'],
            how='outer'  # Garder toutes les données (ElectriCore ET Odoo)
        ).fill_null(0)  # Gérer toutes les valeurs manquantes

        # Nettoyer les colonnes dupliquées de la jointure
        comparison = comparison.drop([col for col in comparison.columns if col.endswith('_right')])

        # Vérification finale : supprimer les lignes avec PDL encore vide (ne devrait plus arriver)
        comparison = comparison.filter(
            (pl.col('pdl').is_not_null()) & (pl.col('pdl') != '')
        )

        # Calculer les écarts entre ElectriCore et Odoo
        # Identifier les colonnes Odoo disponibles après pivot (exclure les colonnes ElectriCore)
        odoo_cols = [col for col in comparison.columns if col not in [
            'pdl', 'periode', 'jours_abo_electricore', 'puissance_electricore',
            'base_kwh_electricore', 'hp_kwh_electricore', 'hc_kwh_electricore',
            'turpe_fixe_electricore', 'turpe_variable_electricore'
        ]]

        print("📊 Colonnes Odoo détectées:", odoo_cols)

        # Ajouter les calculs d'écarts selon les colonnes disponibles
        ecart_expressions = []

        # Écart jours abonnement (si ABONNEMENT existe)
        if 'ABONNEMENT' in odoo_cols:
            ecart_expressions.extend([
                (pl.col('jours_abo_electricore') - pl.col('ABONNEMENT')).alias('ecart_jours_abo'),
                pl.when(pl.col('ABONNEMENT') > 0)
                  .then(((pl.col('jours_abo_electricore') - pl.col('ABONNEMENT')) / pl.col('ABONNEMENT') * 100))
                  .otherwise(None)
                  .alias('ecart_pct_jours_abo')
            ])

        # Écart HP (si HP existe)
        if 'HP' in odoo_cols:
            ecart_expressions.extend([
                (pl.col('hp_kwh_electricore') - pl.col('HP')).alias('ecart_hp_kwh'),
                pl.when(pl.col('HP') > 0)
                  .then(((pl.col('hp_kwh_electricore') - pl.col('HP')) / pl.col('HP') * 100))
                  .otherwise(None)
                  .alias('ecart_pct_hp_kwh')
            ])

        # Écart HC (si HC existe)
        if 'HC' in odoo_cols:
            ecart_expressions.extend([
                (pl.col('hc_kwh_electricore') - pl.col('HC')).alias('ecart_hc_kwh'),
                pl.when(pl.col('HC') > 0)
                  .then(((pl.col('hc_kwh_electricore') - pl.col('HC')) / pl.col('HC') * 100))
                  .otherwise(None)
                  .alias('ecart_pct_hc_kwh')
            ])

        # Écart Base (si BASE existe)
        if 'BASE' in odoo_cols:
            ecart_expressions.extend([
                (pl.col('base_kwh_electricore') - pl.col('BASE')).alias('ecart_base_kwh'),
                pl.when(pl.col('BASE') > 0)
                  .then(((pl.col('base_kwh_electricore') - pl.col('BASE')) / pl.col('BASE') * 100))
                  .otherwise(None)
                  .alias('ecart_pct_base_kwh')
            ])

        # Ajouter les colonnes d'écarts si des expressions existent
        if ecart_expressions:
            comparison = comparison.with_columns(ecart_expressions)

        # Ajouter des indicateurs de concordance
        concordance_expressions = []

        # Flag pour écarts significatifs (> 5%)
        ecart_pct_cols = [col for col in comparison.columns if col.startswith('ecart_pct_')]
        if ecart_pct_cols:
            # Créer une condition OR pour tous les écarts > 5%
            significant_ecart_condition = None
            for col in ecart_pct_cols:
                condition = pl.col(col).abs() > 5
                significant_ecart_condition = condition if significant_ecart_condition is None else (significant_ecart_condition | condition)

            if significant_ecart_condition is not None:
                concordance_expressions.append(
                    significant_ecart_condition.alias('ecart_significatif')
                )

        # Flag pour données présentes des deux côtés
        concordance_expressions.append(
            ((pl.col('jours_abo_electricore') > 0) |
             (pl.col('base_kwh_electricore') > 0) |
             (pl.col('hp_kwh_electricore') > 0) |
             (pl.col('hc_kwh_electricore') > 0)).alias('present_electricore')
        )

        # Flag pour données présentes côté Odoo (au moins une colonne > 0)
        if odoo_cols:
            odoo_present_condition = None
            for col in odoo_cols:
                condition = pl.col(col) > 0
                odoo_present_condition = condition if odoo_present_condition is None else (odoo_present_condition | condition)

            if odoo_present_condition is not None:
                concordance_expressions.append(odoo_present_condition.alias('present_odoo'))

        # Ajouter les indicateurs de concordance
        if concordance_expressions:
            comparison = comparison.with_columns(concordance_expressions)

        print(f"✅ Comparaison créée: {len(comparison)} lignes")
        print("📊 Colonnes disponibles:", comparison.columns)
    return (comparison,)


@app.cell(hide_code=True)
def _(comparison):
    """Filtrer les périodes présentes dans les deux systèmes"""
    if comparison is None:
        comparison_commune = None
    else:
        comparison_commune = comparison.filter(
            pl.col('present_electricore') & pl.col('present_odoo')
        )
        print(f"🔍 Périodes communes: {len(comparison_commune)} lignes (sur {len(comparison)} total)")
    comparison_commune
    return (comparison_commune,)


@app.cell(hide_code=True)
def _(comparison, comparison_commune):
    """Statistiques de comparaison des quantités physiques"""
    if comparison_commune is None:
        _msg = mo.md("⚠️ Pas de données de comparaison")
    else:
        # Utiliser les périodes communes déjà filtrées
        _comparaisons = comparison_commune

        if len(_comparaisons) == 0:
            _msg = mo.md("⚠️ Aucune période avec données ElectriCore")
        else:
            nb_comp = len(_comparaisons)

            # Calculer écarts par type de quantité (selon colonnes Odoo disponibles)
            cols_odoo = [col for col in comparison.columns if not col.endswith('_electricore') and col not in ['pdl', 'periode']]

            _msg = mo.md(f"""
            ## 📊 Statistiques de comparaison des quantités physiques

            **Couverture** : {nb_comp} périodes avec données ElectriCore

            **Colonnes ElectriCore** :
            - Jours abonnement : {_comparaisons.select(pl.col('jours_abo_electricore').sum()).item()}
            - Base (kWh) : {_comparaisons.select(pl.col('base_kwh_electricore').sum()).item():.0f}
            - HP (kWh) : {_comparaisons.select(pl.col('hp_kwh_electricore').sum()).item():.0f}
            - HC (kWh) : {_comparaisons.select(pl.col('hc_kwh_electricore').sum()).item():.0f}
            - TURPE fixe : {_comparaisons.select(pl.col('turpe_fixe_electricore').sum()).item():.0f}€
            - TURPE variable : {_comparaisons.select(pl.col('turpe_variable_electricore').sum()).item():.0f}€

            **Colonnes Odoo disponibles** : {cols_odoo}

            **Prochaines étapes** :

            1. Vérifier le mapping des catégories de produits Odoo
            2. Comparer les quantités correspondantes
            3. Analyser les écarts sur TURPE uniquement
            """)
    _msg
    return


@app.cell(hide_code=True)
def _(comparison):
    """Analyse des données par PDL"""
    if comparison is None:
        _msg = mo.md("⚠️ Pas de données de comparaison")
    else:
        # Synthèse par PDL des quantités ElectriCore
        synthese_pdl = (
            comparison
            .group_by('pdl')
            .agg([
                pl.len().alias('nb_periodes'),
                pl.col('jours_abo_electricore').sum().alias('total_jours_abo'),
                pl.col('base_kwh_electricore').sum().alias('total_base_kwh'),
                pl.col('hp_kwh_electricore').sum().alias('total_hp_kwh'),
                pl.col('hc_kwh_electricore').sum().alias('total_hc_kwh'),
                pl.col('turpe_fixe_electricore').sum().alias('total_turpe_fixe'),
                pl.col('turpe_variable_electricore').sum().alias('total_turpe_variable')
            ])
            .sort('total_turpe_fixe', descending=True)
        )

        _msg = mo.vstack([
            mo.md("## 📊 Synthèse par PDL - Quantités ElectriCore"),
            mo.md("Totaux par Point de Livraison :"),
            synthese_pdl.head(10)
        ])
    _msg
    return


if __name__ == "__main__":
    app.run()
