import marimo

__generated_with = "0.16.0"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl
    import pandas as pd
    import sys
    from pathlib import Path
    from datetime import datetime, timezone
    import time
    from typing import Dict, List, Optional, Tuple

    # Ajouter le chemin du projet
    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    # Imports des loaders DuckDB
    from electricore.core.loaders.duckdb_loader import f15, c15, r151, execute_custom_query

    # Imports des pipelines Polars
    from electricore.core.pipelines_polars.energie_polars import (
        pipeline_energie_polars,
        calculer_periodes_energie_polars
    )
    from electricore.core.pipelines_polars.abonnements_polars import (
        pipeline_abonnements,
        calculer_periodes_abonnement
    )
    from electricore.core.pipelines_polars.turpe_polars import (
        load_turpe_rules_polars,
        ajouter_turpe_fixe,
        ajouter_turpe_variable
    )
    from electricore.core.pipelines_polars.perimetre_polars import (
        detecter_points_de_rupture,
        inserer_evenements_facturation
    )

    # Import connecteur Odoo
    from electricore.etl.connectors.odoo import OdooReader


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
    mo.md(
        r"""
    # Validation du calcul TURPE avec les données F15

    Ce notebook compare les montants TURPE calculés par le pipeline ElectriCore
    avec les données de facturation F15 d'Enedis.

    **Objectifs :**

    - ✅ Validation multi-échelle (global, PDL, temporel)
    - ✅ Identification des écarts et leurs causes
    - ✅ Gestion des différences attendues (compteurs non-intelligents, relevés manquants)
    - ✅ Rapport de synthèse interactif
    """
    )
    return


@app.cell
def _():
    mo.md(r"""# Préparation""")
    return


@app.cell
def _():
    mo.md(r"""## Chargement des données F15""")
    return


@app.cell(hide_code=True)
def load_f15_data():
    """Extraction et agrégation des données TURPE depuis F15"""

    mo.md("## 📊 Extraction des données F15 (TURPE facturé)")

    print("🔄 Chargement des données F15...")
    _start_time_f15 = time.time()

    # Requête pour extraire les composantes TURPE depuis F15
    query_f15_turpe = """
    SELECT
        pdl,
        date_facture,
        date_debut,
        date_fin,
        libelle_ev,
        CASE
            WHEN libelle_ev LIKE '%Composante Gestion%' THEN 'Composante Gestion'
            WHEN libelle_ev LIKE '%Composante Comptage%' THEN 'Composante Comptage'
            WHEN libelle_ev LIKE '%Composante Soutirage%' THEN 'Composante Soutirage'
            WHEN libelle_ev LIKE '%Composante de relevé%' THEN 'Composante Relevé'
            WHEN libelle_ev LIKE '%Correctif%' THEN 'Correctif'
            ELSE 'Autres prestations'
        END as type_composante,
        CASE
            -- Priorité aux mentions explicites de "Part fixe" ou "Part variable"
            WHEN libelle_ev LIKE '%Part fixe%' THEN 'Fixe'
            WHEN libelle_ev LIKE '%Part variable%' THEN 'Variable'
            -- Puis classification par composante pour les cas non explicites
            WHEN libelle_ev LIKE '%Composante Gestion%' THEN 'Fixe'
            WHEN libelle_ev LIKE '%Composante Comptage%' THEN 'Fixe'
            WHEN libelle_ev LIKE '%Composante Soutirage%' THEN 'Variable'
            WHEN libelle_ev LIKE '%Composante de relevé%' THEN 'Fixe'
            ELSE 'Autre'
        END as part_turpe,
        CAST(montant_ht AS DOUBLE) as montant_ht,
        CAST(quantite AS DOUBLE) as quantite,
        CAST(prix_unitaire AS DOUBLE) as prix_unitaire,
        unite,
        formule_tarifaire_acheminement
    FROM flux_enedis.flux_f15_detail
    WHERE nature_ev = '01'
    AND montant_ht IS NOT NULL
    """

    # Exécuter la requête
    lf_f15_turpe = execute_custom_query(query_f15_turpe, lazy=True)
    df_f15_turpe = lf_f15_turpe.collect()

    # Statistiques de base
    _load_time_f15 = time.time() - _start_time_f15
    _total_montant = df_f15_turpe.select(pl.col("montant_ht").sum()).item()
    _nb_pdl_uniques = df_f15_turpe.select(pl.col("pdl").n_unique()).item()
    _date_min = df_f15_turpe.select(pl.col("date_debut").min()).item()
    _date_max = df_f15_turpe.select(pl.col("date_fin").max()).item()

    print(f"✅ Données F15 chargées en {_load_time_f15:.1f}s")
    print(f"📊 {len(df_f15_turpe):,} lignes de facturation TURPE")
    print(f"💰 Montant total TURPE F15: {_total_montant:,.2f} €")
    print(f"🏠 {_nb_pdl_uniques} PDL uniques")
    print(f"📅 Période: {_date_min} → {_date_max}")
    df_f15_turpe
    return (df_f15_turpe,)


@app.cell(hide_code=True)
def aggregations_f15(df_f15_turpe):
    """Agrégations des données F15 pour comparaison"""

    mo.md("### Agrégations F15")

    # Agrégation par PDL
    df_f15_par_pdl = (
        df_f15_turpe
        .group_by("pdl")
        .agg([
            pl.col("montant_ht").sum().alias("montant_total_f15"),
            pl.col("date_debut").min().alias("premiere_periode"),
            pl.col("date_fin").max().alias("derniere_periode"),
            pl.col("type_composante").n_unique().alias("nb_composantes")
        ])
        .sort("montant_total_f15", descending=True)
    )

    # Agrégation par mois
    df_f15_par_mois = (
        df_f15_turpe
        .with_columns([
            # Convertir en timezone naive pour éviter les conflits de join
            pl.col("date_debut").str.to_datetime().dt.truncate("1mo").dt.replace_time_zone(None).alias("mois")
        ])
        .group_by("mois")
        .agg([
            pl.col("montant_ht").sum().alias("montant_f15"),
            pl.col("pdl").n_unique().alias("nb_pdl"),
            pl.len().alias("nb_lignes")
        ])
        .sort("mois")
    )

    # Agrégation par type de composante
    df_f15_par_composante = (
        df_f15_turpe
        .group_by(["type_composante", "part_turpe"])
        .agg([
            pl.col("montant_ht").sum().alias("montant_f15"),
            pl.len().alias("nb_lignes")
        ])
        .sort("montant_f15", descending=True)
    )
    return df_f15_par_composante, df_f15_par_mois, df_f15_par_pdl


@app.cell(hide_code=True)
def show_f15_summary(df_f15_par_composante, df_f15_par_mois, df_f15_par_pdl):
    """Affichage des résumés F15"""
    mo.accordion(items={
        'f15 par pdl':df_f15_par_pdl,
        'f15 par mois':df_f15_par_mois,
        'f15 par composante':df_f15_par_composante,
    })
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Séparation turpe fixe/variable""")
    return


@app.cell
def _(df_f15_turpe):
    # Extraction du TURPE fixe depuis F15
    _df_f15_fixe = (
        df_f15_turpe
        .filter(pl.col("part_turpe") == "Fixe")
        # .group_by("pdl")
        # .agg([
        #     pl.col("montant_ht").sum().alias("turpe_fixe_f15")
        # ])
    )

    # Statistiques de base
    _total_montant = _df_f15_fixe.select(pl.col("montant_ht").sum()).item()
    _nb_pdl_uniques = _df_f15_fixe.select(pl.col("pdl").n_unique()).item()
    _date_min = _df_f15_fixe.select(pl.col("date_debut").min()).item()
    _date_max = _df_f15_fixe.select(pl.col("date_fin").max()).item()

    print(f"✅ Données F15 turpe fixe")
    print(f"📊 {len(_df_f15_fixe):,} lignes de facturation TURPE")
    print(f"💰 Montant total TURPE F15: {_total_montant:,.2f} €")
    print(f"🏠 {_nb_pdl_uniques} PDL uniques")
    print(f"📅 Période: {_date_min} → {_date_max}")
    _df_f15_fixe
    return


@app.cell
def _(df_f15_turpe):
    # Extraction du TURPE var depuis F15
    df_f15_variable = (
        df_f15_turpe
        .filter(pl.col("part_turpe") == "Variable")
        # .group_by("pdl")
        # .agg([
        #     pl.col("montant_ht").sum().alias("turpe_variable_f15")
        # ])
    )
    # Statistiques de base
    _total_montant = df_f15_variable.select(pl.col("montant_ht").sum()).item()
    _nb_pdl_uniques = df_f15_variable.select(pl.col("pdl").n_unique()).item()
    _date_min = df_f15_variable.select(pl.col("date_debut").min()).item()
    _date_max = df_f15_variable.select(pl.col("date_fin").max()).item()

    print(f"✅ Données F15 turpe fixe")
    print(f"📊 {len(df_f15_variable):,} lignes de facturation TURPE")
    print(f"💰 Montant total TURPE F15: {_total_montant:,.2f} €")
    print(f"🏠 {_nb_pdl_uniques} PDL uniques")
    print(f"📅 Période: {_date_min} → {_date_max}")
    df_f15_variable
    return (df_f15_variable,)


@app.cell
def _():
    mo.md(r"""## Récupération des pdl EDN""")
    return


@app.cell(hide_code=True)
def load_odoo_perimeter(config):
    """Récupération du périmètre de PDL depuis Odoo"""

    mo.md("## 🏢 Récupération du périmètre PDL (Odoo)")

    # Configuration Odoo (à adapter selon votre configuration)
    try:
        print("🔄 Connexion à Odoo...")
        with OdooReader(config=config) as odoo:
            # Récupération des PDL depuis les commandes Odoo, focus sur C5
            df_pdl_odoo = (
                odoo.query('sale.order',
                    domain=[('x_pdl', '!=', False)],  # Uniquement les commandes avec PDL
                    fields=['name', 'x_pdl', 'partner_id'])
                .filter(pl.col('x_pdl').is_not_null())
                .select([
                    pl.col('x_pdl').str.strip_chars().alias('pdl'),
                    pl.col('name').alias('order_name')
                ])
                .collect()
                .unique('pdl')
            )

        nb_pdl_odoo = len(df_pdl_odoo)
        print(f"✅ {nb_pdl_odoo} PDL récupérés depuis Odoo")

        # Aperçu des PDL
        if nb_pdl_odoo > 0:
            print(f"📊 Exemples PDL: {df_pdl_odoo.select('pdl').to_series().to_list()}")

        df_pdl_odoo = df_pdl_odoo

    except Exception as e:
        print(f"⚠️ Erreur connexion Odoo: {e}")
        print("📄 Continuons sans filtre Odoo (tous les PDL F15 seront analysés)")

        # DataFrame vide si pas de connexion Odoo
        df_pdl_odoo = pl.DataFrame({'pdl': [], 'order_name': []}, schema={'pdl': pl.Utf8, 'order_name': pl.Utf8})
    return (df_pdl_odoo,)


@app.cell
def _():
    mo.md(r"""## Chargement des flux pour pipeline""")
    return


@app.cell(hide_code=True)
def load_pipeline_data():
    """Chargement des données pour le pipeline de calcul TURPE"""

    mo.md("## 🔧 Calcul TURPE via le pipeline")

    print("🔄 Chargement des données pour le pipeline...")
    _start_time_pipeline = time.time()

    # Charger l'historique C15 enrichi
    print("📄 Chargement historique C15...")
    lf_historique = c15().lazy()
    lf_historique_enrichi = inserer_evenements_facturation(
        detecter_points_de_rupture(lf_historique)
    )
    df_historique = lf_historique_enrichi.collect()

    # Charger les relevés R151
    print("📄 Chargement relevés R151...")
    lf_releves = r151().lazy()
    df_releves = lf_releves.collect()

    # Charger les règles TURPE
    print("📄 Chargement règles TURPE...")
    lf_regles_turpe = load_turpe_rules_polars()
    df_regles_turpe = lf_regles_turpe.collect()

    _load_time_pipeline = time.time() - _start_time_pipeline

    print(f"✅ Données pipeline chargées en {_load_time_pipeline:.1f}s")
    print(f"📊 Historique C15: {len(df_historique)} événements")
    print(f"📊 Relevés R151: {len(df_releves)} relevés")
    print(f"📊 Règles TURPE: {len(df_regles_turpe)} règles tarifaires")
    return df_historique, df_releves


@app.cell
def _():
    mo.md(r"""# Analyse fiabilité calcul turpe fixe""")
    return


@app.cell
def _():
    mo.md(r"""## Calcul turpe fixe""")
    return


@app.cell(hide_code=True)
def calculate_turpe_fixe(df_historique):
    """Calcul TURPE fixe uniquement (abonnements)"""

    mo.md("## 🏠 Calcul TURPE Fixe (Abonnements)")

    print("🔄 Calcul des périodes d'abonnement...")
    _start_time_fixe = time.time()

    # Pipeline abonnements complet (inclut calcul + TURPE fixe)
    lf_periodes_abonnement = pipeline_abonnements(pl.LazyFrame(df_historique))
    df_periodes_abonnement = lf_periodes_abonnement.collect()

    # Agrégation du TURPE fixe par PDL
    df_turpe_fixe_pdl = (
        df_periodes_abonnement
        .group_by("pdl")
        .agg([
            pl.col("turpe_fixe").sum().alias("turpe_fixe_total"),
            pl.col("debut").min().alias("date_debut_periode"),
            pl.col("fin").max().alias("date_fin_periode")
        ])
    )

    calc_time_fixe = time.time() - _start_time_fixe

    # Statistiques TURPE fixe
    total_turpe_fixe = df_turpe_fixe_pdl.select(pl.col("turpe_fixe_total").sum()).item()
    nb_pdl_fixe = df_turpe_fixe_pdl.select(pl.col("pdl").n_unique()).item()

    print(f"✅ Pipeline TURPE fixe exécuté en {calc_time_fixe:.1f}s")
    print(f"💰 Montant total TURPE fixe calculé: {total_turpe_fixe:,.2f} €")
    print(f"🏠 {nb_pdl_fixe} PDL traités")
    return (df_turpe_fixe_pdl,)


@app.cell
def _():
    mo.md(r"""## Comparaison F15 vs Calculé - TURPE Fixe""")
    return


@app.cell(hide_code=True)
def compare_turpe_fixe(df_f15_turpe, df_turpe_fixe_pdl):
    """Comparaison détaillée F15 vs Pipeline pour le TURPE fixe"""

    print("🔍 Comparaison F15 vs Pipeline pour TURPE fixe...")

    # Agrégation du TURPE fixe F15 par PDL
    df_f15_fixe_par_pdl = (
        df_f15_turpe
        .filter(pl.col("part_turpe") == "Fixe")
        .group_by("pdl")
        .agg([
            pl.col("montant_ht").sum().alias("turpe_fixe_f15"),
            pl.col("date_debut").min().alias("premiere_periode_f15"),
            pl.col("date_fin").max().alias("derniere_periode_f15"),
            pl.len().alias("nb_lignes_f15")
        ])
    )

    # Jointure complète F15 vs Calculé
    df_comparison_fixe = (
        df_f15_fixe_par_pdl
        .join(df_turpe_fixe_pdl, on="pdl", how="full")
        .with_columns([
            pl.col("turpe_fixe_f15").fill_null(0.0),
            pl.col("turpe_fixe_total").fill_null(0.0)
        ])
        .with_columns([
            (pl.col("turpe_fixe_total") - pl.col("turpe_fixe_f15")).alias("ecart_absolu"),
            pl.when(pl.col("turpe_fixe_f15") != 0.0)
            .then(((pl.col("turpe_fixe_total") - pl.col("turpe_fixe_f15")) / pl.col("turpe_fixe_f15") * 100))
            .otherwise(pl.lit(None))
            .alias("ecart_relatif_pct"),
            # Classification des PDL
            pl.when((pl.col("turpe_fixe_f15") > 0) & (pl.col("turpe_fixe_total") > 0))
            .then(pl.lit("Présent des 2 côtés"))
            .when((pl.col("turpe_fixe_f15") > 0) & (pl.col("turpe_fixe_total") == 0))
            .then(pl.lit("Manquant côté calcul"))
            .when((pl.col("turpe_fixe_f15") == 0) & (pl.col("turpe_fixe_total") > 0))
            .then(pl.lit("En trop côté calcul"))
            .otherwise(pl.lit("Vide des 2 côtés"))
            .alias("statut_pdl")
        ])
        .sort(pl.col("ecart_absolu").abs(), descending=True)
    )

    # Statistiques globales
    total_f15_fixe = df_comparison_fixe.select(pl.col("turpe_fixe_f15").sum()).item()
    total_calcule_fixe = df_comparison_fixe.select(pl.col("turpe_fixe_total").sum()).item()
    ecart_global_fixe = total_calcule_fixe - total_f15_fixe
    _ecart_global_pct = (ecart_global_fixe / total_f15_fixe) * 100 if total_f15_fixe > 0 else 0

    nb_pdl_f15 = df_comparison_fixe.filter(pl.col("turpe_fixe_f15") > 0).select(pl.len()).item()
    nb_pdl_calcule = df_comparison_fixe.filter(pl.col("turpe_fixe_total") > 0).select(pl.len()).item()
    nb_pdl_communs = df_comparison_fixe.filter(
        (pl.col("turpe_fixe_f15") > 0) & (pl.col("turpe_fixe_total") > 0)
    ).select(pl.len()).item()

    taux_couverture = (nb_pdl_calcule / nb_pdl_f15) * 100 if nb_pdl_f15 > 0 else 0

    print(f"📊 RÉSULTATS COMPARAISON TURPE FIXE:")
    print(f"   💰 F15 fixe: {total_f15_fixe:,.2f} € ({nb_pdl_f15} PDL)")
    print(f"   💰 Calculé fixe: {total_calcule_fixe:,.2f} € ({nb_pdl_calcule} PDL)")
    print(f"   ⚖️ Écart global: {ecart_global_fixe:+,.2f} € ({_ecart_global_pct:+.1f}%)")
    print(f"   🎯 Couverture: {nb_pdl_communs}/{nb_pdl_f15} PDL ({taux_couverture:.1f}%)")
    return


@app.cell
def _():
    mo.md(r"""## ⏰ Calcul de prorata temporel""")
    return


@app.cell(hide_code=True)
def calcul_prorata_temporel(df_f15_turpe):
    """Calcul du prorata temporel pour exclure la facturation à échoir"""

    # Date de coupure = fin du mois dernier révolu
    from datetime import date
    import calendar
    _today = date.today()
    if _today.month == 1:
        _date_coupure = date(_today.year - 1, 12, 31)
    else:
        # Dernier jour du mois précédent
        _prev_month = _today.month - 1
        _prev_year = _today.year
        _last_day = calendar.monthrange(_prev_year, _prev_month)[1]
        _date_coupure = date(_prev_year, _prev_month, _last_day)

    print(f"📅 Date de coupure pour prorata: {_date_coupure}")

    # Filtrer TURPE fixe uniquement
    df_f15_fixe = df_f15_turpe.filter(pl.col("part_turpe") == "Fixe")

    # Calcul du prorata pour chaque ligne
    df_prorata = df_f15_fixe.with_columns([
        # Conversion des dates
        pl.col("date_debut").str.to_date().alias("debut_dt"),
        pl.col("date_fin").str.to_date().alias("fin_dt"),
        pl.date(_date_coupure.year, _date_coupure.month, _date_coupure.day).alias("date_coupure_dt")
    ]).with_columns([
        # Calcul des durées
        (pl.col("fin_dt") - pl.col("debut_dt") + pl.duration(days=1)).dt.total_days().alias("duree_totale_jours"),
        # Calcul de la fin effective (min entre fin période et date coupure)
        pl.min_horizontal(pl.col("fin_dt"), pl.col("date_coupure_dt")).alias("fin_effective"),
    ]).with_columns([
        # Calcul des jours avant coupure
        pl.max_horizontal(
            pl.lit(0),
            (pl.col("fin_effective") - pl.col("debut_dt") + pl.duration(days=1)).dt.total_days()
        ).alias("jours_avant_coupure")
    ]).with_columns([
        # Calcul du ratio de prorata
        (pl.col("jours_avant_coupure") / pl.col("duree_totale_jours")).alias("ratio_prorata"),
        # Montant proratisé
        (pl.col("montant_ht") * pl.col("jours_avant_coupure") / pl.col("duree_totale_jours")).alias("montant_proratise")
    ])

    # Classification des périodes
    df_classification = df_prorata.with_columns([
        pl.when(pl.col("debut_dt") > pl.col("date_coupure_dt"))
        .then(pl.lit("Entièrement après coupure"))
        .when(pl.col("fin_dt") <= pl.col("date_coupure_dt"))
        .then(pl.lit("Entièrement avant coupure"))
        .otherwise(pl.lit("Partiellement après coupure"))
        .alias("classification_temporelle")
    ])

    # Statistiques globales
    stats = df_classification.group_by("classification_temporelle").agg([
        pl.len().alias("nb_lignes"),
        pl.col("montant_ht").sum().alias("montant_original"),
        pl.col("montant_proratise").sum().alias("montant_proratise"),
        pl.col("ratio_prorata").mean().alias("ratio_moyen")
    ]).sort("montant_original", descending=True)

    print(f"\n📊 STATISTIQUES DE PRORATA TEMPOREL:")
    print(stats.to_pandas().to_string(index=False, float_format="%.2f"))

    # Totaux
    total_original = df_classification.select(pl.col("montant_ht").sum()).item()
    total_proratise = df_classification.select(pl.col("montant_proratise").sum()).item()
    difference = total_original - total_proratise

    print(f"\n💰 RÉSUMÉ FINANCIER:")
    print(f"   Montant original (F15 fixe): {total_original:,.2f} €")
    print(f"   Montant proratisé: {total_proratise:,.2f} €")
    print(f"   Différence (à échoir): {difference:,.2f} € ({difference/total_original*100:.1f}%)")
    return (df_classification,)


@app.cell
def _(df_classification):
    # Agrégation des montants F15 proratisés par PDL
    df_f15_prorata_pdl = df_classification.group_by("pdl").agg([
        pl.col("montant_proratise").sum().alias("turpe_fixe_f15_prorata")
    ])
    return (df_f15_prorata_pdl,)


@app.cell(hide_code=True)
def turpe_fixe_quality_metrics_prorata(df_f15_prorata_pdl, df_turpe_fixe_pdl):
    """Métriques de qualité TURPE fixe avec montants proratisés"""

    print(f"\n🎯 MÉTRIQUES DE QUALITÉ AVEC PRORATA TEMPOREL")



    # Jointure avec les montants calculés
    df_comparison_prorata = df_f15_prorata_pdl.join(
        df_turpe_fixe_pdl,
        on="pdl",
        how="inner"
    ).with_columns([
        # Calcul des écarts avec prorata
        (pl.col("turpe_fixe_total") - pl.col("turpe_fixe_f15_prorata")).alias("ecart_absolu_prorata"),
        (((pl.col("turpe_fixe_total") - pl.col("turpe_fixe_f15_prorata")) / pl.col("turpe_fixe_f15_prorata")) * 100).alias("ecart_relatif_pct_prorata")
    ])

    # Statistiques globales avec prorata
    total_calcule = df_comparison_prorata.select(pl.col("turpe_fixe_total").sum()).item()
    total_f15_prorata = df_comparison_prorata.select(pl.col("turpe_fixe_f15_prorata").sum()).item()
    ecart_global_prorata = total_calcule - total_f15_prorata
    _nb_pdl_communs_prorata = len(df_comparison_prorata)

    print(f"\n💰 COMPARAISON AVEC PRORATA:")
    print(f"   F15 proratisé: {total_f15_prorata:,.2f} € ({_nb_pdl_communs_prorata} PDL)")
    print(f"   Calculé: {total_calcule:,.2f} €")
    print(f"   Écart global: {ecart_global_prorata:+,.2f} € ({ecart_global_prorata/total_f15_prorata*100:+.1f}%)")

    # Métriques de précision avec prorata
    if _nb_pdl_communs_prorata > 0:
        _nb_precise_1eur_prorata = df_comparison_prorata.filter(pl.col("ecart_absolu_prorata").abs() <= 1.0).select(pl.len()).item()
        _nb_precise_5pct_prorata = df_comparison_prorata.filter(pl.col("ecart_relatif_pct_prorata").abs() <= 5.0).select(pl.len()).item()

        _precision_1eur_prorata = (_nb_precise_1eur_prorata / _nb_pdl_communs_prorata) * 100
        _precision_5pct_prorata = (_nb_precise_5pct_prorata / _nb_pdl_communs_prorata) * 100

        _ecart_moyen_prorata = df_comparison_prorata.select(pl.col("ecart_absolu_prorata").mean()).item()
        _ecart_median_prorata = df_comparison_prorata.select(pl.col("ecart_absolu_prorata").median()).item()

        # Évaluation qualitative avec prorata
        _ecart_global_pct = abs(ecart_global_prorata / total_f15_prorata) * 100 if total_f15_prorata > 0 else 0

        if _ecart_global_pct < 1.0 and _precision_5pct_prorata > 95:
            _evaluation = "🟢 EXCELLENTE"
            _recommandation = "TURPE fixe très fiable avec prorata"
        elif _ecart_global_pct < 2.0 and _precision_5pct_prorata > 90:
            _evaluation = "🟡 BONNE"
            _recommandation = "Quelques ajustements mineurs"
        elif _ecart_global_pct < 5.0:
            _evaluation = "🟠 CORRECTE"
            _recommandation = "Révision des règles tarifaires recommandée"
        else:
            _evaluation = "🔴 À AMÉLIORER"
            _recommandation = "Problèmes majeurs dans le calcul"

        print(f"\n📊 MÉTRIQUES DE PRÉCISION (PRORATA):")
        print(f"   Précision ±1€: {_precision_1eur_prorata:.1f}% ({_nb_precise_1eur_prorata}/{_nb_pdl_communs_prorata} PDL)")
        print(f"   Précision ±5%: {_precision_5pct_prorata:.1f}% ({_nb_precise_5pct_prorata}/{_nb_pdl_communs_prorata} PDL)")
        print(f"   Écart moyen: {_ecart_moyen_prorata:+.2f} € | Écart médian: {_ecart_median_prorata:+.2f} €")
        print(f"   🎯 Évaluation: {_evaluation}")
        print(f"   💡 Recommandation: {_recommandation}")

        df_comparison_prorata_result = df_comparison_prorata
        ecart_global_prorata_result = ecart_global_prorata
    else:
        print("⚠️ Aucun PDL commun trouvé pour la comparaison prorata!")
        df_comparison_prorata_result = None
        ecart_global_prorata_result = 0
    return


@app.cell(hide_code=True)
def analyze_turpe_fixe_gaps_prorata(df_classification, df_turpe_fixe_pdl, df_periodes_abonnement):
    """Analyse des écarts TURPE fixe avec montants proratisés"""

    print("\n🔍 ANALYSE DES ÉCARTS PAR CATÉGORIE (PRORATA):")

    # Agrégation des montants F15 proratisés par PDL
    df_f15_prorata_base = df_classification.group_by("pdl").agg([
        pl.col("montant_proratise").sum().alias("turpe_fixe_f15_prorata"),
        pl.col("date_debut").min().alias("date_arrivee_pdl")  # Première date de facturation
    ])

    # Récupération des métadonnées métier depuis les périodes d'abonnement
    df_metadonnees_pdl = df_periodes_abonnement.group_by("pdl").agg([
        pl.col("formule_tarifaire_acheminement").cast(pl.String).first(),  # FTA du PDL
        pl.col("puissance_souscrite").first()  # Puissance souscrite en kVA
    ])

    # Jointure des montants proratisés avec les métadonnées
    df_f15_prorata_avec_metadonnees = df_f15_prorata_base.join(
        df_metadonnees_pdl,
        on="pdl",
        how="left"  # Garder tous les PDL même sans métadonnées
    )

    # Jointure complète pour analyser tous les PDL
    df_comparison_prorata_full = df_f15_prorata_avec_metadonnees.join(
        df_turpe_fixe_pdl,
        on="pdl",
        how="outer"
    ).with_columns([
        # Gestion des valeurs nulles et statut PDL
        pl.col("turpe_fixe_f15_prorata").fill_null(0),
        pl.col("turpe_fixe_total").fill_null(0),
        pl.when(pl.col("turpe_fixe_f15_prorata").is_null())
        .then(pl.lit("Seulement dans calculé"))
        .when(pl.col("turpe_fixe_total").is_null())
        .then(pl.lit("Seulement dans F15 prorata"))
        .otherwise(pl.lit("Présent des 2 côtés"))
        .alias("statut_pdl")
    ]).with_columns([
        # Calcul des écarts avec prorata
        (pl.col("turpe_fixe_total") - pl.col("turpe_fixe_f15_prorata")).alias("ecart_absolu_prorata"),
        (((pl.col("turpe_fixe_total") - pl.col("turpe_fixe_f15_prorata")) / pl.col("turpe_fixe_f15_prorata")) * 100).alias("ecart_relatif_pct_prorata")
    ])

    # Statistiques par statut PDL avec prorata
    _stats_par_statut_prorata = (
        df_comparison_prorata_full
        .group_by("statut_pdl")
        .agg([
            pl.len().alias("nb_pdl"),
            pl.col("turpe_fixe_f15_prorata").sum().alias("montant_f15_prorata"),
            pl.col("turpe_fixe_total").sum().alias("montant_calcule"),
            pl.col("ecart_absolu_prorata").sum().alias("ecart_total_prorata")
        ])
        .sort("ecart_total_prorata", descending=True)
    )

    for row in _stats_par_statut_prorata.iter_rows(named=True):
        statut = row['statut_pdl']
        nb_pdl = row['nb_pdl']
        montant_f15_prorata = row['montant_f15_prorata']
        montant_calcule = row['montant_calcule']
        ecart = row['ecart_total_prorata']

        print(f"   📋 {statut}: {nb_pdl} PDL")
        print(f"      F15 prorata: {montant_f15_prorata:,.2f} € | Calculé: {montant_calcule:,.2f} € | Écart: {ecart:+,.2f} €")

    # Top 10 des écarts les plus importants avec prorata (PDL communs seulement)
    _top_ecarts_prorata = (
        df_comparison_prorata_full
        .filter(pl.col("statut_pdl") == "Présent des 2 côtés")
        .filter(pl.col("ecart_relatif_pct_prorata").abs() > 0.1)  # Écarts > 0.1%
        .sort("ecart_absolu_prorata", descending=True)
        .select([
            "pdl", "date_arrivee_pdl", "formule_tarifaire_acheminement", "puissance_souscrite",
            "turpe_fixe_f15_prorata", "turpe_fixe_total",
            "ecart_absolu_prorata", "ecart_relatif_pct_prorata"
        ])
    )


    mo.vstack([mo.md(f"\n📈 ÉCARTS SIGNIFICATIFS (PDL communs - PRORATA):"), _top_ecarts_prorata])
    return


@app.cell
def _():
    mo.md(r"""# Analyse fiabilité calcul turpe variable""")
    return


@app.cell
def _():
    mo.md(r"""## Calcul turpe Variable""")
    return


@app.cell(hide_code=True)
def calculate_turpe_variable(df_historique, df_releves):
    """Calcul TURPE variable uniquement (énergies)"""

    mo.md("## ⚡ Calcul TURPE Variable (Énergies)")

    print("🔄 Calcul des périodes d'énergie...")
    _start_time_variable = time.time()

    # Pipeline énergie complet (inclut chronologie + calcul + TURPE variable)
    lf_periodes_energie = pipeline_energie_polars(
        pl.LazyFrame(df_historique),
        pl.LazyFrame(df_releves)
    )
    df_periodes_energie = lf_periodes_energie.collect()

    # Agrégation du TURPE variable par PDL
    df_turpe_variable_pdl = (
        df_periodes_energie
        .group_by("pdl")
        .agg([
            pl.col("turpe_variable").sum().alias("turpe_variable_total")
        ])
    )

    calc_time_variable = time.time() - _start_time_variable

    # Statistiques TURPE variable
    total_turpe_variable = df_turpe_variable_pdl.select(pl.col("turpe_variable_total").sum()).item()
    nb_pdl_variable = df_turpe_variable_pdl.select(pl.col("pdl").n_unique()).item()

    print(f"✅ Pipeline TURPE variable exécuté en {calc_time_variable:.1f}s")
    print(f"💰 Montant total TURPE variable calculé: {total_turpe_variable:,.2f} €")
    print(f"🏠 {nb_pdl_variable} PDL traités (avec données R151)")
    return (df_turpe_variable_pdl,)


if __name__ == "__main__":
    app.run()
