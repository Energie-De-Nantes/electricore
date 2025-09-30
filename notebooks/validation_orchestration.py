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
    from electricore.core.loaders import f15, c15, r151, releves_harmonises, execute_custom_query

    from electricore.core.pipelines import facturation
    from electricore.core.loaders import OdooReader
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
    mo.md(r"""# Calcul Facturation""")
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
    return (fact,)


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
                    .with_columns([
                        pl.col('lisse').fill_null(False).alias('lisse')
                    ])
                )

            print(f"✅ lignes de factures extraites")

        except Exception as e:
            print(f"❌ Erreur lors de l'extraction Odoo: {e}")
            odoo_factures = None
    return (odoo_factures,)


@app.cell
def _(odoo_factures):
    odoo_factures
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        """
    ## 🎯 Préparation des données Odoo pour comparaison

    **Objectif** : Filtrer et préparer les données de facturation Odoo pour une comparaison pertinente.

    **Étapes** :

    1. **Exclusion des abonnements lissés** : Les abonnements lissés ne correspondent pas aux consommations réelles
    2. **Décalage temporel** : Date facture (mois N+1) → Période consommation (mois N)
    3. **Classification des produits** : Identifier BASE, HP, HC, ABONNEMENT, TURPE
    4. **Détection du type de tarification** :
       - Si BASE facturé → Client en tarif Base
       - Si HP ou HC facturé → Client en tarif HP/HC

    **Résultat** : DataFrame `odoo_prepare` avec le type de tarification identifié pour chaque PDL×période
    """
    )
    return


@app.cell(hide_code=True)
def _(odoo_factures):
    odoo_prepare = (
        odoo_factures
        .filter(pl.col('lisse') == False)
        .with_columns([
            # DÉCALAGE D'UN MOIS : date facture Odoo (mois N+1) → période consommation (mois N)
            pl.col('invoice_date').str.to_date().dt.offset_by('-1mo').dt.strftime('%Y-%m').alias('periode'),
        ])
        .group_by(['pdl', 'periode', 'categorie'])
        .agg([
            pl.col('quantity').sum().alias('quantite'),
            pl.col('price_total').sum().alias('montant')
        ])
        .pivot(on='categorie', index=['pdl', 'periode'], values='quantite')
        .rename(mapping={'Base': 'base'})
        .with_columns([
            pl.when(pl.col('base') > 0)
              .then(pl.lit('BASE'))
              .when((pl.col('HP') > 0) | (pl.col('HC') > 0))
              .then(pl.lit('HPHC'))
              .otherwise(pl.lit('INCONNU'))
              .alias('type_tarification')
        ])
    )

    print(f"✅ Odoo préparé: {len(odoo_prepare)} PDL×périodes")
    print(f"📊 Colonnes finales: {odoo_prepare.columns}")
    return (odoo_prepare,)


@app.cell
def _(odoo_prepare):
    odoo_prepare
    return


@app.cell(hide_code=True)
def _():
    mo.md(
        """
    ## 🔧 Préparation des données ElectriCore

    **Objectif** : Préparer les quantités physiques calculées par ElectriCore pour la comparaison.

    **Données utilisées** :

    - DataFrame `fact` : Méta-périodes de facturation (déjà agrégées par mois)
    - Quantités physiques : kWh Base/HP/HC, jours d'abonnement, puissance souscrite
    - Montants TURPE : seuls montants calculés à comparer avec Odoo

    **Résultat** : DataFrame `electricore_prepare` prêt pour la jointure avec les données Odoo
    """
    )
    return


@app.cell(hide_code=True)
def _(fact):
    """Prépare les données ElectriCore : quantités physiques calculées"""
    if fact is None:
        print("❌ Pas de données ElectriCore")
        electricore_prepare = None
    else:
        print("🔄 Préparation des données ElectriCore...")

        # Utiliser directement les méta-périodes (fact contient déjà les agrégations mensuelles)
        electricore_prepare = (
            fact.with_columns([
                pl.col('debut').dt.strftime('%Y-%m').alias('periode'),
            ])
            .filter(pl.col('data_complete') == True)
            .select([
                'pdl', 'periode',
                'ref_situation_contractuelle',  # Référence contrat pour groupby
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
                'nb_jours': 'jours_abo',
                'puissance_moyenne': 'puissance_kva',
                'base_energie': 'base_kwh',
                'hp_energie': 'hp_kwh',
                'hc_energie': 'hc_kwh',
                'turpe_fixe': 'turpe_fixe_eur',
                'turpe_variable': 'turpe_variable_eur'
            })
        )

        print(f"✅ ElectriCore préparé: {len(electricore_prepare)} PDL×périodes")
        print(f"📊 Colonnes: {electricore_prepare.columns}")
    return (electricore_prepare,)


@app.cell(hide_code=True)
def _():
    mo.md(
        """
    ## 🔀 Jointure et comparaison des données

    **Objectif** : Joindre les données ElectriCore et Odoo puis calculer les écarts selon le type de tarification.

    **Logique de comparaison** :

    1. **Jointure** sur PDL + période
    2. **Filtrage** : Garder uniquement les périodes présentes dans les deux systèmes
    3. **Comparaison adaptée** selon le type de tarification :
       - **Tarif Base** : Comparer uniquement `base_kwh` vs `BASE`
       - **Tarif HP/HC** : Comparer `hp_kwh` vs `HP` et `hc_kwh` vs `HC`
    4. **Calculs d'écarts** : Différences absolues et relatives

    **Résultat** : DataFrame `comparison` avec écarts pertinents uniquement
    """
    )
    return


@app.cell(hide_code=True)
def _(electricore_prepare, odoo_prepare):
    """Jointure et calculs de comparaison selon le type de tarification"""
    if electricore_prepare is None or odoo_prepare is None:
        print("❌ Impossible de faire la jointure")
        comparison = None
    else:
        print("🔄 Jointure des données...")

        # Étape 1: Filtrer les données avec PDL valides
        electricore_clean = electricore_prepare.filter(
            (pl.col('pdl').is_not_null()) &
            (pl.col('pdl') != '') &
            (pl.col('periode').is_not_null())
        )

        odoo_clean = odoo_prepare.filter(
            (pl.col('pdl').is_not_null()) &
            (pl.col('pdl') != '') &
            (pl.col('periode').is_not_null())
        )

        print(f"📊 ElectriCore clean: {len(electricore_clean)} lignes")
        print(f"📊 Odoo clean: {len(odoo_clean)} lignes")

        # Étape 2: Jointure sur PDL + période
        joined = electricore_clean.join(
            odoo_clean,
            on=['pdl', 'periode'],
            how='inner'  # Garder uniquement les périodes communes
        )

        print(f"🔗 Après jointure: {len(joined)} lignes communes")

        # Étape 3: Calculs d'écarts selon le type de tarification
        comparison = joined.with_columns([
            # Écarts jours abonnement (pas d'abonnement dans les données Odoo pour l'instant)
            pl.lit(None).alias('ecart_jours_abo'),

            # Écarts énergétiques SEULEMENT selon le type de tarification
            pl.when(pl.col('type_tarification') == 'BASE')
              .then(pl.col('base_kwh') - pl.col('base'))  # Comparer Base vs base
              .otherwise(None)
              .alias('ecart_base_kwh'),

            pl.when(pl.col('type_tarification') == 'HPHC')
              .then(pl.col('hp_kwh') - pl.col('HP'))  # Comparer HP vs HP
              .otherwise(None)
              .alias('ecart_hp_kwh'),

            pl.when(pl.col('type_tarification') == 'HPHC')
              .then(pl.col('hc_kwh') - pl.col('HC'))  # Comparer HC vs HC
              .otherwise(None)
              .alias('ecart_hc_kwh')
        ])

        # Étape 4: Calculs des écarts relatifs (en %)
        comparison = comparison.with_columns([
            # Écarts relatifs jours abonnement (pas d'abonnement dans les données Odoo)
            pl.lit(None).alias('ecart_pct_jours_abo'),

            # Écarts relatifs énergétiques (seulement pour les quantités pertinentes)
            pl.when((pl.col('type_tarification') == 'BASE') & (pl.col('base') > 0))
              .then((pl.col('ecart_base_kwh') / pl.col('base') * 100))
              .otherwise(None)
              .alias('ecart_pct_base_kwh'),

            pl.when((pl.col('type_tarification') == 'HPHC') & (pl.col('HP') > 0))
              .then((pl.col('ecart_hp_kwh') / pl.col('HP') * 100))
              .otherwise(None)
              .alias('ecart_pct_hp_kwh'),

            pl.when((pl.col('type_tarification') == 'HPHC') & (pl.col('HC') > 0))
              .then((pl.col('ecart_hc_kwh') / pl.col('HC') * 100))
              .otherwise(None)
              .alias('ecart_pct_hc_kwh')
        ])

        print(f"✅ Comparaison créée: {len(comparison)} lignes")
        print(f"📊 Colonnes: {comparison.columns}")
    return (comparison,)


@app.cell(hide_code=True)
def _(comparison):
    """Affichage des statistiques de comparaison"""
    if comparison is None:
        print("❌ Pas de données de comparaison")
    else:
        print(f"📊 Statistiques de la comparaison:")
        print(f"   • Nombre total de PDL×périodes: {len(comparison)}")

        # Statistiques par type de tarification
        stats_tarif = comparison.group_by('type_tarification').len().sort('len', descending=True)
        print(f"   • Répartition par tarification:")
        for row in stats_tarif.iter_rows():
            tarif, count = row
            print(f"     - {tarif}: {count} lignes")
    return


@app.cell
def _(comparison):
    comparison
    return


@app.cell(hide_code=True)
def _(comparison):
    """Analyse des écarts de la comparaison"""
    if comparison is None:
        print("❌ Pas de données de comparaison")
    else:
        print(f"🔍 Analyse des écarts significatifs (> 5%):")

        # Compter les écarts significatifs par type
        ecarts_base = comparison.filter(
            (pl.col('type_tarification') == 'BASE') &
            (pl.col('ecart_pct_base_kwh').abs() > 5)
        )

        ecarts_hp = comparison.filter(
            (pl.col('type_tarification') == 'HPHC') &
            (pl.col('ecart_pct_hp_kwh').abs() > 5)
        )

        ecarts_hc = comparison.filter(
            (pl.col('type_tarification') == 'HPHC') &
            (pl.col('ecart_pct_hc_kwh').abs() > 5)
        )

        # Pas d'abonnements dans les données Odoo actuelles
        ecarts_abo_count = 0

        print(f"   • Écarts Base > 5%: {len(ecarts_base)} lignes")
        print(f"   • Écarts HP > 5%: {len(ecarts_hp)} lignes")
        print(f"   • Écarts HC > 5%: {len(ecarts_hc)} lignes")
        print(f"   • Écarts Abonnement > 5%: {ecarts_abo_count} lignes")
    return


@app.cell(hide_code=True)
def _(comparison):
    # Agrégation par référence contractuelle
    ecarts_par_contrat = (
        comparison
        .group_by('ref_situation_contractuelle')
        .agg([
            pl.len().alias('nb_periodes'),
            pl.col('pdl').first().alias('pdl'),  # Garder le PDL
            pl.col('type_tarification').first().alias('type_tarif'),

            # Totaux quantités ElectriCore
            pl.col('base_kwh').sum().alias('total_base_kwh_electricore'),
            pl.col('hp_kwh').sum().alias('total_hp_kwh_electricore'),
            pl.col('hc_kwh').sum().alias('total_hc_kwh_electricore'),

            # Totaux quantités Odoo
            pl.col('base').sum().alias('total_base_kwh_odoo'),
            pl.col('HP').sum().alias('total_hp_kwh_odoo'),
            pl.col('HC').sum().alias('total_hc_kwh_odoo'),

            # Totaux écarts absolus
            pl.col('ecart_base_kwh').sum().alias('total_ecart_base_kwh'),
            pl.col('ecart_hp_kwh').sum().alias('total_ecart_hp_kwh'),
            pl.col('ecart_hc_kwh').sum().alias('total_ecart_hc_kwh'),

            # Moyennes écarts relatifs (sans les null)
            pl.col('ecart_pct_base_kwh').mean().alias('moy_ecart_pct_base'),
            pl.col('ecart_pct_hp_kwh').mean().alias('moy_ecart_pct_hp'),
            pl.col('ecart_pct_hc_kwh').mean().alias('moy_ecart_pct_hc'),
        ])
        .sort('nb_periodes', descending=True)
    )
    ecarts_par_contrat
    return


if __name__ == "__main__":
    app.run()
