import marimo

__generated_with = "0.16.5"
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
    from electricore.core.loaders import (
        c15, releves_harmonises
    )
    from electricore.core.pipelines.orchestration import (
        facturation
    )
    from electricore.core.pipelines.facturation import (
        expr_calculer_trimestre
    )

    # Import connecteur Odoo
    from electricore.core.loaders import (
        OdooReader,
        query,
        commandes_lignes
    )

    # Import pipeline Accise
    from electricore.core.pipelines.accise import pipeline_accise


@app.cell(hide_code=True)
def _():
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


@app.cell
def _():
    mo.md(r"""# CTA""")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Récupération du périmètre PDL depuis Odoo""")
    return


@app.cell(hide_code=True)
def load_odoo_perimeter(config):
    """Récupération du périmètre de PDL depuis Odoo"""

    mo.md("## 🏢 Récupération du périmètre PDL (Odoo)")

    try:
        print("🔄 Connexion à Odoo...")
        with OdooReader(config=config) as odoo:
            df_pdl_odoo = (
                query(odoo, 'sale.order',
                    domain=[('x_pdl', '!=', False)],
                    fields=['name', 'x_pdl', 'partner_id'])
                .filter(pl.col('x_pdl').is_not_null())
                .select([
                    pl.col('x_pdl').str.strip_chars().alias('pdl'),
                    pl.col('name').alias('order_name')
                ])
                .collect()
                # .with_columns(pl.lit('EDN').alias('marque'))
                .unique('pdl')
            )

        nb_pdl_odoo = len(df_pdl_odoo)
        print(f"✅ {nb_pdl_odoo} PDL récupérés depuis Odoo")

        if nb_pdl_odoo > 0:
            print(f"📊 Exemples PDL: {df_pdl_odoo.select('pdl').to_series().to_list()[:5]}")

    except Exception as e:
        print(f"⚠️ Erreur connexion Odoo: {e}")
        print("📄 Continuons sans filtre Odoo (tous les PDL F15 seront analysés)")
        df_pdl_odoo = pl.DataFrame({'pdl': [], 'order_name': [], 'marque': []}, schema={'pdl': pl.Utf8, 'order_name': pl.Utf8, 'marque': pl.Utf8})

    # Ajouter des PDL supplémentaires
    pdl_supplementaires = pl.DataFrame({
        'pdl': ['14295224261882', '50070117855585', '50000508594660'],
        'order_name': ['PDL_MANUAL_1', 'PDL_MANUAL_2', 'PDL_MANUAL_3'],
        # 'marque': ['EDN', 'EDN', 'EDN']
    })
    df_pdl_odoo = pl.concat([df_pdl_odoo, pdl_supplementaires]).unique('pdl')
    return (df_pdl_odoo,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Chargement des données pour le pipeline de calcul""")
    return


@app.cell
def _():
    lf_historique = c15().lazy()
    lf_releves = releves_harmonises().lazy()
    return lf_historique, lf_releves


@app.cell
def _():
    mo.md(r"""## Pipeline facturation""")
    return


@app.cell
def _(lf_historique, lf_releves):
    _,_,_,df_facturation = facturation(historique=lf_historique, releves=lf_releves)
    df_facturation
    return (df_facturation,)


@app.cell
def _(df_facturation, df_pdl_odoo, taux_cta):
    df_cta = (
        df_facturation
        .join(df_pdl_odoo, on='pdl', how='inner')
        .with_columns(
            expr_calculer_trimestre().alias('trimestre'),
            (pl.col('turpe_fixe_eur')*taux_cta.value/100).alias('cta_eur')
        )
    )
    df_cta
    return (df_cta,)


@app.cell
def _():
    mo.md(r"""## 📊 Sélection du trimestre et taux CTA""")
    return


@app.cell(hide_code=True)
def _():
    # Input pour le taux de CTA (en %)
    taux_cta = mo.ui.number(
        start=0,
        stop=100,
        step=0.01,
        value=21.93,  # Taux CTA 2025 par défaut
        label="Taux CTA (%)"
    )
    taux_cta
    return (taux_cta,)


@app.cell
def _():
    mo.md(r"""# Accise""")
    return


@app.cell
def _(config):
    with OdooReader(config=config) as _odoo:
        df_lignes = commandes_lignes(_odoo).collect()
    return (df_lignes,)


@app.cell
def _(df_lignes):
    df_lignes
    return


@app.cell
def _(df_lignes):
    # Pipeline complet : agrégation + calcul Accise
    df_accise = pipeline_accise(df_lignes.lazy())
    df_accise
    return (df_accise,)


@app.cell
def _():
    mo.md(r"""# Fusion CTA + Accise pour calcul des marges""")
    return


@app.cell
def _(df_accise, df_cta):
    """
    Fusion des données CTA et Accise pour calculer les marges.

    Convertit mois_annee (texte) en format YYYY-MM pour rejoindre avec mois_consommation.
    """
    # Convertir mois_annee en format YYYY-MM pour la jointure
    df_cta_avec_mois = df_cta.with_columns(
        # Extraire le mois depuis la colonne debut (datetime)
        pl.col('debut').dt.strftime('%Y-%m').alias('mois')
    )

    # Fusion avec df_accise sur order_name et mois
    df_couts = (
        df_cta_avec_mois
        .join(
            df_accise.select([
                'order_name',
                pl.col('mois_consommation').alias('mois'),
                'taux_accise_eur_mwh',
                pl.col('energie_mwh').alias('energie_facturee_mwh'),
                'accise_eur'
            ]),
            on=['order_name', 'mois'],
            how='left'
        )
    )

    df_couts
    return (df_couts,)


@app.cell
def _():
    mo.md(r"""# Facturation détaillée par catégorie""")
    return


@app.cell
def _(df_lignes):
    """
    Transformation des lignes de factures en format pivot pour analyse des marges.

    Pivot sur les catégories (Abonnements, Base, HP, HC) avec quantity, price_unit, price_total
    + calcul du total facturé par mois.
    """
    from electricore.core.pipelines.accise import expr_calculer_mois_consommation

    # Étape 1 : Filtrer sur les catégories d'énergie et abonnements
    df_facturation_filtree = (
        df_lignes
        .filter(pl.col('name_product_category').is_in(['Abonnements', 'Base', 'HP', 'HC']))
        .with_columns(
            expr_calculer_mois_consommation().alias('mois_consommation')
        )
    )

    # Étape 2 : Agréger par order, mois et catégorie
    df_facturation_agg = (
        df_facturation_filtree
        .group_by(['name', 'mois_consommation', 'name_product_category'])
        .agg([
            pl.col('quantity').sum().alias('quantity'),
            pl.col('price_unit').mean().alias('price_unit'),
            pl.col('price_total').sum().alias('price_total')
        ])
    )

    # Étape 3 : Pivoter sur name_product_category
    df_facturation_pivot = (
        df_facturation_agg
        .pivot(
            index=['name', 'mois_consommation'],
            on='name_product_category',
            values=['quantity', 'price_unit', 'price_total']
        )
        # Renommer name → order_name pour cohérence
        .rename({'name': 'order_name'})
    )

    # Étape 4 : Calculer le total facturé (somme horizontale des price_total)
    # Trouver toutes les colonnes qui contiennent "price_total"
    price_total_cols = [col for col in df_facturation_pivot.columns if 'price_total' in col]

    # Calculer le total facturé (gérer le cas où certaines colonnes n'existent pas)
    if price_total_cols:
        # Sommer les colonnes existantes, fill_null(0) pour gérer les valeurs manquantes
        total_expr = pl.sum_horizontal([pl.col(c).fill_null(0) for c in price_total_cols])
        df_facturation_pivot = df_facturation_pivot.with_columns(
            total_expr.alias('total_facture_eur')
        )
    else:
        # Aucune colonne price_total trouvée, créer une colonne à 0
        df_facturation_pivot = df_facturation_pivot.with_columns(
            pl.lit(0.0).alias('total_facture_eur')
        )

    df_facturation_pivot
    return (df_facturation_pivot,)


@app.cell
def _(df_facturation_pivot):
    df_facturation_pivot
    return


@app.cell
def _():
    mo.md(r"""# Fusion des coûts et de la facturation""")
    return


@app.cell
def _(df_couts, df_facturation_pivot):
    """
    Fusion finale : coûts (CTA + Accise) + facturation détaillée.

    Jointure sur order_name et mois_consommation pour obtenir
    une vue complète permettant le calcul des marges.

    Exclusion des lignes avec valeurs aberrantes.
    """
    df_marges = (
        df_couts
        # Exclure ligne avec ref aberrante
        .filter(pl.col('ref_situation_contractuelle') != '833397114')
        # Exclure ligne avec PDL aberrant
        .filter(pl.col('pdl') != '14290738060355')
        .join(
            df_facturation_pivot,
            left_on=['order_name', 'mois'],
            right_on=['order_name', 'mois_consommation'],
            how='left'
        )
    )

    df_marges
    return (df_marges,)


@app.cell
def _():
    mo.md(r"""# Agrégation par souscription (order_name)""")
    return


@app.cell(hide_code=True)
def _(df_marges):
    df_marges_agregees = (
        df_marges
        .sort('order_name')
        .group_by('order_name')
        .agg([
            # Sommes des métriques
            pl.col('nb_jours').sum().alias('nb_jours'),
            pl.col('energie_facturee_mwh').sum().round(3).alias('energie_facturee_mwh'),
            pl.col('accise_eur').sum().round(2).alias('accise_eur'),
            pl.col('turpe_fixe_eur').sum().round(2).alias('turpe_fixe_eur'),
            pl.col('cta_eur').sum().round(2).alias('cta_eur'),
            pl.col('turpe_variable_eur').sum().round(2).alias('turpe_variable_eur'),
            pl.col('total_facture_eur').sum().round(2).alias('total_facture_eur'),

            # Valeurs uniques (sinon null)
            pl.when(pl.col('puissance_moyenne_kva').n_unique() == 1)
              .then(pl.col('puissance_moyenne_kva').first())
              .otherwise(None)
              .alias('puissance_moyenne_kva'),

            pl.when(pl.col('formule_tarifaire_acheminement').n_unique() == 1)
              .then(pl.col('formule_tarifaire_acheminement').first())
              .otherwise(None)
              .alias('formule_tarifaire_acheminement')
        ])
        .with_columns((pl.col('energie_facturee_mwh')*65).round(2).alias('prod_eur'))
        .with_columns((pl.col('accise_eur')
                + pl.col('turpe_fixe_eur')
                + pl.col('cta_eur')
                + pl.col('turpe_variable_eur')
                + pl.col('prod_eur')
                ).round(2).alias('couts_eur'))
        .with_columns((pl.col('total_facture_eur') - pl.col('couts_eur')).round(2).alias('benef_total_eur'))
        .with_columns((pl.col('benef_total_eur') / pl.col('nb_jours') * 30.42).round(2).alias('benef_mensuel_eur'))
    )

    df_marges_agregees
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
