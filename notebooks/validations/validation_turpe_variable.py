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
    from electricore.core.loaders import f15, c15, r151, execute_custom_query

    # Imports des pipelines Polars
    from electricore.core.pipelines.energie import (
        pipeline_energie,
        calculer_periodes_energie
    )
    from electricore.core.pipelines.abonnements import (
        pipeline_abonnements,
        calculer_periodes_abonnement
    )
    from electricore.core.pipelines.turpe import (
        load_turpe_rules,
        ajouter_turpe_fixe,
        ajouter_turpe_variable
    )
    from electricore.core.pipelines.perimetre import (
        detecter_points_de_rupture,
        inserer_evenements_facturation
    )

    # Import connecteur Odoo
    from electricore.core.loaders import OdooReader, query


@app.cell(hide_code=True)
def _():
    """Configuration Odoo depuis .env"""
    import os
    from electricore.config import charger_config_odoo

    try:
        config = charger_config_odoo()
        _env = os.getenv("ODOO_ENV", "test")
        _msg = mo.md(f"**Configuration Odoo chargée** (env: `{_env}`)\n\n- URL: `{config['url']}`\n- Base: `{config['db']}`\n- Utilisateur: `{config['username']}`\n- Mot de passe: `***`")
    except ValueError as e:
        config = {}
        _msg = mo.md(f"⚠️ **Configuration Odoo manquante**\n\n{e}\n\nDéfinissez les variables `ODOO_*` dans `.env`.")
    _msg
    return (config,)


@app.cell(hide_code=True)
def _():
    mo.md(
        r"""
    # Validation du calcul TURPE Variable avec les données F15

    Ce notebook compare les montants TURPE variable calculés par le pipeline ElectriCore
    avec les données de facturation F15 d'Enedis.

    **Spécificités du TURPE Variable :**

    - ✅ Basé sur les consommations réelles par cadran (HP, HC, etc.)
    - ✅ Dépend des données R151 (relevés de comptage)
    - ✅ Nécessite des transformations temporelles pour aligner les périodes
    - ✅ Gestion des PDL sans données de consommation
    - ✅ Entonnoir d'attribution des écarts multiples

    **Défis spécifiques :**

    - 🔴 **Couverture partielle** : Tous les PDL n'ont pas de données R151
    - 🔴 **Décalage temporel** : Facturation Enedis (moiniversaire) vs notre facturation (mensuelle)
    - 🔴 **Trous de données** : PDL nouveaux avec historique incomplet
    """
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Préparation des données""")
    return


@app.cell(hide_code=True)
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
        CAST(date_facture AS DATE) as date_facture,
        CAST(date_debut AS DATE) as date_debut,
        CAST(date_fin AS DATE) as date_fin,
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
def _():
    mo.md(r"""## Extraction du TURPE Variable depuis F15""")
    return


@app.cell(hide_code=True)
def _(df_f15_turpe):
    # Extraction du TURPE variable depuis F15
    df_f15_variable = (
        df_f15_turpe
        .filter(pl.col("part_turpe") == "Variable")
    )

    # Statistiques de base
    _total_montant = df_f15_variable.select(pl.col("montant_ht").sum()).item()
    _nb_pdl_uniques = df_f15_variable.select(pl.col("pdl").n_unique()).item()
    _date_min = df_f15_variable.select(pl.col("date_debut").min()).item()
    _date_max = df_f15_variable.select(pl.col("date_fin").max()).item()

    print(f"✅ Données F15 TURPE Variable extraites")
    print(f"📊 {len(df_f15_variable):,} lignes de facturation")
    print(f"💰 Montant total TURPE Variable F15: {_total_montant:,.2f} €")
    print(f"🏠 {_nb_pdl_uniques} PDL uniques")
    print(f"📅 Période: {_date_min} → {_date_max}")

    # Agrégation par PDL pour comparaison
    df_f15_variable_par_pdl = (
        df_f15_variable
        .group_by("pdl")
        .agg([
            pl.col("montant_ht").sum().alias("turpe_variable_f15"),
            pl.col("date_debut").min().alias("premiere_periode_f15"),
            pl.col("date_fin").max().alias("derniere_periode_f15"),
            pl.len().alias("nb_lignes_f15")
        ])
        .sort("turpe_variable_f15", descending=True)
    )

    df_f15_variable
    return df_f15_variable, df_f15_variable_par_pdl


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
                .unique('pdl')
            )

        nb_pdl_odoo = len(df_pdl_odoo)
        print(f"✅ {nb_pdl_odoo} PDL récupérés depuis Odoo")

        if nb_pdl_odoo > 0:
            print(f"📊 Exemples PDL: {df_pdl_odoo.select('pdl').to_series().to_list()[:5]}")

    except Exception as e:
        print(f"⚠️ Erreur connexion Odoo: {e}")
        print("📄 Continuons sans filtre Odoo (tous les PDL F15 seront analysés)")
        df_pdl_odoo = pl.DataFrame({'pdl': [], 'order_name': []}, schema={'pdl': pl.Utf8, 'order_name': pl.Utf8})
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Chargement des données pour le pipeline de calcul""")
    return


@app.cell(hide_code=True)
def load_pipeline_data():
    """Chargement des données pour le pipeline de calcul TURPE variable"""

    mo.md("## 🔧 Calcul TURPE Variable via le pipeline")

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
    # print("📄 Chargement relevés R151...")
    # lf_releves = r151().lazy()
    from electricore.core.loaders import releves_harmonises
    lf_releves = releves_harmonises().lazy()
    df_releves = lf_releves.collect()

    _load_time_pipeline = time.time() - _start_time_pipeline

    print(f"✅ Données pipeline chargées en {_load_time_pipeline:.1f}s")
    print(f"📊 Historique C15: {len(df_historique)} événements")
    print(f"📊 Relevés R151: {len(df_releves)} relevés")

    # Analyse de couverture R151
    _pdl_historique = set(df_historique.select("pdl").to_series().to_list())
    _pdl_releves = set(df_releves.select("pdl").to_series().to_list())
    _pdl_communs = _pdl_historique.intersection(_pdl_releves)
    _taux_couverture = len(_pdl_communs) / len(_pdl_historique) * 100 if _pdl_historique else 0

    print(f"🎯 Couverture R151: {len(_pdl_communs)}/{len(_pdl_historique)} PDL ({_taux_couverture:.1f}%)")
    print(f"⚠️ PDL sans R151: {len(_pdl_historique) - len(_pdl_communs)} ({100-_taux_couverture:.1f}%)")
    return df_historique, df_releves


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Calcul du TURPE Variable""")
    return


@app.cell(hide_code=True)
def calculate_turpe_variable(df_historique, df_releves):
    """Calcul TURPE variable via le pipeline énergie"""

    mo.md("## ⚡ Calcul TURPE Variable (Pipeline Énergie)")

    print("🔄 Calcul des périodes d'énergie avec TURPE variable...")
    _start_time_variable = time.time()

    # Pipeline énergie complet (inclut chronologie + calcul + TURPE variable)
    lf_periodes_energie = pipeline_energie(
        pl.LazyFrame(df_historique),
        pl.LazyFrame(df_releves)
    )
    df_periodes_energie = lf_periodes_energie.collect()

    # Agrégation du TURPE variable par PDL
    df_turpe_variable_pdl = (
        df_periodes_energie
        .group_by("pdl")
        .agg([
            pl.col("turpe_variable_eur").sum().alias("turpe_variable_calcule"),
            pl.col("debut").min().alias("date_debut_calcule"),
            pl.col("fin").max().alias("date_fin_calcule"),
            pl.len().alias("nb_periodes_calcule")
        ])
        .sort("turpe_variable_calcule", descending=True)
    )

    calc_time_variable = time.time() - _start_time_variable

    # Statistiques TURPE variable calculé
    total_turpe_variable = df_turpe_variable_pdl.select(pl.col("turpe_variable_calcule").sum()).item()
    nb_pdl_variable = df_turpe_variable_pdl.select(pl.col("pdl").n_unique()).item()

    print(f"✅ Pipeline TURPE variable exécuté en {calc_time_variable:.1f}s")
    print(f"💰 Montant total TURPE variable calculé: {total_turpe_variable:,.2f} €")
    print(f"🏠 {nb_pdl_variable} PDL traités (avec données R151)")
    return df_periodes_energie, df_turpe_variable_pdl


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Analyse de couverture des PDL""")
    return


@app.cell(hide_code=True)
def analyze_pdl_coverage(df_f15_variable_par_pdl, df_turpe_variable_pdl):
    """Analyse de la couverture des PDL entre F15 et nos calculs"""

    print("🔍 Analyse de couverture des PDL...")

    # Jointure complète F15 vs Calculé
    df_coverage_analysis = (
        df_f15_variable_par_pdl
        .join(df_turpe_variable_pdl, on="pdl", how="outer")
        .with_columns([
            pl.col("turpe_variable_f15").fill_null(0.0),
            pl.col("turpe_variable_calcule").fill_null(0.0)
        ])
        .with_columns([
            # Classification des PDL selon leur présence
            pl.when((pl.col("turpe_variable_f15") > 0) & (pl.col("turpe_variable_calcule") > 0))
            .then(pl.lit("✅ Présent des 2 côtés"))
            .when((pl.col("turpe_variable_f15") > 0) & (pl.col("turpe_variable_calcule") == 0))
            .then(pl.lit("❌ Manquant côté calcul"))
            .when((pl.col("turpe_variable_f15") == 0) & (pl.col("turpe_variable_calcule") > 0))
            .then(pl.lit("⚠️ En trop côté calcul"))
            .otherwise(pl.lit("⭕ Vide des 2 côtés"))
            .alias("statut_couverture")
        ])
    )

    # Statistiques de couverture
    coverage_stats = (
        df_coverage_analysis
        .group_by("statut_couverture")
        .agg([
            pl.len().alias("nb_pdl"),
            pl.col("turpe_variable_f15").sum().alias("montant_f15"),
            pl.col("turpe_variable_calcule").sum().alias("montant_calcule")
        ])
        .sort("nb_pdl", descending=True)
    )

    print("\n📊 STATISTIQUES DE COUVERTURE PDL:")
    for _row_cov in coverage_stats.iter_rows(named=True):
        _statut = _row_cov['statut_couverture']
        _nb_pdl_cov = _row_cov['nb_pdl']
        _montant_f15 = _row_cov['montant_f15']
        _montant_calcule = _row_cov['montant_calcule']
        print(f"   {_statut}: {_nb_pdl_cov} PDL")
        print(f"      F15: {_montant_f15:,.2f} € | Calculé: {_montant_calcule:,.2f} €")

    # Taux de couverture principal
    _nb_f15_total_cov = df_coverage_analysis.filter(pl.col("turpe_variable_f15") > 0).select(pl.len()).item()
    _nb_calcule_total_cov = df_coverage_analysis.filter(pl.col("turpe_variable_calcule") > 0).select(pl.len()).item()
    _nb_communs_cov = df_coverage_analysis.filter(
        (pl.col("turpe_variable_f15") > 0) & (pl.col("turpe_variable_calcule") > 0)
    ).select(pl.len()).item()

    _taux_couverture = (_nb_communs_cov / _nb_f15_total_cov) * 100 if _nb_f15_total_cov > 0 else 0

    print(f"\n🎯 RÉSUMÉ COUVERTURE:")
    print(f"   PDL F15: {_nb_f15_total_cov}")
    print(f"   PDL calculés: {_nb_calcule_total_cov}")
    print(f"   PDL communs: {_nb_communs_cov}")
    print(f"   Taux de couverture: {_taux_couverture:.1f}%")
    return (coverage_stats,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Transformations pour la comparaison""")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""## Transformation temporelle (Moiniversaire → Mensuel)""")
    return


@app.cell(hide_code=True)
def temporal_transformation_variable(df_f15_variable):
    """Transformation temporelle pour aligner les périodes F15 sur nos périodes mensuelles"""

    # Date de coupure = fin du mois dernier révolu
    _today = date.today()
    if _today.month == 1:
        _date_coupure = date(_today.year - 1, 12, 31)
    else:
        _prev_month = _today.month - 1
        _prev_year = _today.year
        _last_day = calendar.monthrange(_prev_year, _prev_month)[1]
        _date_coupure = date(_prev_year, _prev_month, _last_day)

    print(f"📅 Date de coupure pour transformation temporelle: {_date_coupure}")

    # Calcul du prorata temporel pour chaque ligne F15 Variable
    df_f15_variable_prorata = df_f15_variable.with_columns([
        # Conversion des dates
        pl.date(_date_coupure.year, _date_coupure.month, _date_coupure.day).alias("date_coupure_dt")
    ]).with_columns([
        # Calcul des durées
        (pl.col("date_fin") - pl.col("date_debut") + pl.duration(days=1)).dt.total_days().alias("duree_totale_jours"),
        # Fin effective (min entre fin période et date coupure)
        pl.min_horizontal(pl.col("date_fin"), pl.col("date_coupure_dt")).alias("fin_effective"),
    ]).with_columns([
        # Jours avant coupure
        pl.max_horizontal(
            pl.lit(0),
            (pl.col("fin_effective") - pl.col("date_debut") + pl.duration(days=1)).dt.total_days()
        ).alias("jours_avant_coupure")
    ]).with_columns([
        # Ratio et montant proratisé
        (pl.col("jours_avant_coupure") / pl.col("duree_totale_jours")).alias("ratio_prorata"),
        (pl.col("montant_ht") * pl.col("jours_avant_coupure") / pl.col("duree_totale_jours")).alias("montant_proratise")
    ])

    # Classification temporelle
    df_temporal_analysis = df_f15_variable_prorata.with_columns([
        pl.when(pl.col("date_debut") > pl.col("date_coupure_dt"))
        .then(pl.lit("Entièrement après coupure"))
        .when(pl.col("date_fin") <= pl.col("date_coupure_dt"))
        .then(pl.lit("Entièrement avant coupure"))
        .otherwise(pl.lit("Partiellement après coupure"))
        .alias("classification_temporelle")
    ])

    # Statistiques de transformation temporelle
    temporal_stats = df_temporal_analysis.group_by("classification_temporelle").agg([
        pl.len().alias("nb_lignes"),
        pl.col("montant_ht").sum().alias("montant_original"),
        pl.col("montant_proratise").sum().alias("montant_proratise"),
        pl.col("ratio_prorata").mean().alias("ratio_moyen")
    ]).sort("montant_original", descending=True)

    print(f"\n📊 STATISTIQUES TRANSFORMATION TEMPORELLE:")
    print(temporal_stats.to_pandas().to_string(index=False, float_format="%.2f"))

    # Totaux
    total_original = df_temporal_analysis.select(pl.col("montant_ht").sum()).item()
    total_proratise = df_temporal_analysis.select(pl.col("montant_proratise").sum()).item()
    difference = total_original - total_proratise

    print(f"\n💰 IMPACT TRANSFORMATION TEMPORELLE:")
    print(f"   Montant original F15: {total_original:,.2f} €")
    print(f"   Montant après prorata: {total_proratise:,.2f} €")
    print(f"   Différence (à échoir): {difference:,.2f} € ({difference/total_original*100:.1f}%)")

    # Agrégation par PDL des montants proratisés
    df_f15_variable_prorata_pdl = df_temporal_analysis.group_by("pdl").agg([
        pl.col("montant_proratise").sum().alias("turpe_variable_f15_prorata")
    ])
    return df_f15_variable_prorata_pdl, temporal_stats


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Comparaison F15 vs Calculé avec transformations""")
    return


@app.cell(hide_code=True)
def compare_with_transformations(
    df_f15_variable_prorata_pdl,
    df_turpe_variable_pdl,
):
    """Comparaison des montants après transformations temporelles"""

    print("🔍 Comparaison F15 (après transformations) vs Calculé (PDL avec calculs réels)...")

    # Jointure des PDL communs avec calculs réels uniquement
    df_comparison = (
        df_f15_variable_prorata_pdl
        .join(df_turpe_variable_pdl, on="pdl", how="inner")
        .filter(pl.col("turpe_variable_calcule") > 0)  # Garder uniquement les PDL avec calcul réel
        .with_columns([
            # Calcul des écarts
            (pl.col("turpe_variable_calcule") - pl.col("turpe_variable_f15_prorata")).alias("ecart_absolu"),
            (((pl.col("turpe_variable_calcule") - pl.col("turpe_variable_f15_prorata")) / pl.col("turpe_variable_f15_prorata")) * 100).alias("ecart_relatif_pct")
        ])
        .sort(pl.col("ecart_absolu").abs(), descending=True)
    )

    # Statistiques globales
    nb_pdl_communs = len(df_comparison)
    total_f15_prorata = df_comparison.select(pl.col("turpe_variable_f15_prorata").sum()).item()
    _total_calcule_comp = df_comparison.select(pl.col("turpe_variable_calcule").sum()).item()
    _ecart_global = _total_calcule_comp - total_f15_prorata

    print(f"\n💰 COMPARAISON APRÈS TRANSFORMATIONS:")
    print(f"   F15 (prorata): {total_f15_prorata:,.2f} € ({nb_pdl_communs} PDL)")
    print(f"   Calculé: {_total_calcule_comp:,.2f} €")
    print(f"   Écart global: {_ecart_global:+,.2f} € ({_ecart_global/total_f15_prorata*100:+.1f}%)")

    # Métriques de précision
    if nb_pdl_communs > 0:
        _nb_precise_5eur = df_comparison.filter(pl.col("ecart_absolu").abs() <= 5.0).select(pl.len()).item()
        _nb_precise_10pct = df_comparison.filter(pl.col("ecart_relatif_pct").abs() <= 10.0).select(pl.len()).item()

        _precision_5eur = (_nb_precise_5eur / nb_pdl_communs) * 100
        _precision_10pct = (_nb_precise_10pct / nb_pdl_communs) * 100

        _ecart_moyen = df_comparison.select(pl.col("ecart_absolu").mean()).item()
        _ecart_median = df_comparison.select(pl.col("ecart_absolu").median()).item()

        print(f"\n📊 MÉTRIQUES DE PRÉCISION:")
        print(f"   Précision ±5€: {_precision_5eur:.1f}% ({_nb_precise_5eur}/{nb_pdl_communs} PDL)")
        print(f"   Précision ±10%: {_precision_10pct:.1f}% ({_nb_precise_10pct}/{nb_pdl_communs} PDL)")
        print(f"   Écart moyen: {_ecart_moyen:+.2f} € | Écart médian: {_ecart_median:+.2f} €")
    return (df_comparison,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Entonnoir d'attribution des écarts""")
    return


@app.cell(hide_code=True)
def attribution_funnel_simple(
    df_comparison,
    df_f15_variable_par_pdl,
    df_releves,
    df_turpe_variable_pdl,
):
    """Entonnoir d'attribution simplifié qui exclut les PDL sans R151 du F15"""

    print("🔍 ENTONNOIR D'ATTRIBUTION DES ÉCARTS (SIMPLIFIÉ)")
    print("=" * 55)

    # 0. Montants totaux de départ
    _total_f15_brut = df_f15_variable_par_pdl.select(pl.col("turpe_variable_f15").sum()).item()
    _total_calcule_funnel = df_turpe_variable_pdl.select(pl.col("turpe_variable_calcule").sum()).item()
    _ecart_brut = _total_calcule_funnel - _total_f15_brut

    print(f"📊 NIVEAU 0 - Comparaison brute (avant correction):")
    print(f"   F15 total (tous PDL): {_total_f15_brut:,.2f} €")
    print(f"   Calculé total: {_total_calcule_funnel:,.2f} €")
    print(f"   Écart brut: {_ecart_brut:+,.2f} € ({_ecart_brut/_total_f15_brut*100:+.1f}%)")

    # 1. CORRECTION : Exclure les PDL sans R151 du F15
    _pdl_avec_r151 = df_releves.select("pdl").unique().to_series().to_list()

    # F15 filtré sur les PDL ayant des données R151 uniquement
    df_f15_variable_filtre = (
        df_f15_variable_par_pdl
        .filter(pl.col("pdl").is_in(_pdl_avec_r151))
    )

    _total_f15_filtre = df_f15_variable_filtre.select(pl.col("turpe_variable_f15").sum()).item()
    _nb_pdl_f15_total = len(df_f15_variable_par_pdl)
    _nb_pdl_f15_filtre = len(df_f15_variable_filtre)
    _nb_pdl_exclus = _nb_pdl_f15_total - _nb_pdl_f15_filtre
    _montant_pdl_exclus = _total_f15_brut - _total_f15_filtre

    print(f"\n📊 NIVEAU 1 - Exclusion PDL sans R151:")
    print(f"   PDL F15 total: {_nb_pdl_f15_total}")
    print(f"   PDL F15 avec données R151: {_nb_pdl_f15_filtre}")
    print(f"   PDL exclus (sans R151): {_nb_pdl_exclus} ({_nb_pdl_exclus/_nb_pdl_f15_total*100:.1f}%)")
    print(f"   F15 filtré (PDL avec R151): {_total_f15_filtre:,.2f} €")
    print(f"   Montant PDL exclus: {_montant_pdl_exclus:,.2f} € (compteurs non-communicants)")

    # 2. Comparaison sur périmètre commun après exclusion
    _ecart_apres_exclusion = _total_calcule_funnel - _total_f15_filtre

    print(f"\n📊 NIVEAU 2 - Comparaison sur périmètre commun:")
    print(f"   F15 filtré (PDL avec R151): {_total_f15_filtre:,.2f} €")
    print(f"   Calculé (PDL avec R151): {_total_calcule_funnel:,.2f} €")
    print(f"   Écart après exclusion: {_ecart_apres_exclusion:+,.2f} € ({_ecart_apres_exclusion/_total_f15_filtre*100:+.1f}%)")

    # 3. Impact des transformations temporelles (prorata)
    # Recalculer les montants F15 filtrés mais uniquement sur les PDL avec calculs réels
    _pdl_avec_calculs = set(df_comparison.select("pdl").to_series().to_list())

    _montant_f15_filtre_calculs_reels = (
        df_f15_variable_filtre
        .filter(pl.col("pdl").is_in(list(_pdl_avec_calculs)))
        .select(pl.col("turpe_variable_f15").sum())
        .item()
    )

    _montant_f15_prorata = df_comparison.select(pl.col("turpe_variable_f15_prorata").sum()).item()
    _montant_calcule_communs = df_comparison.select(pl.col("turpe_variable_calcule").sum()).item()
    _impact_prorata = _montant_f15_filtre_calculs_reels - _montant_f15_prorata
    _ecart_final = _montant_calcule_communs - _montant_f15_prorata

    print(f"\n📊 NIVEAU 3 - Impact transformations temporelles:")
    print(f"   F15 filtré (PDL avec calculs): {_montant_f15_filtre_calculs_reels:,.2f} €")
    print(f"   F15 filtré après prorata: {_montant_f15_prorata:,.2f} €")
    print(f"   Impact prorata temporel: {-_impact_prorata:+,.2f} €")
    print(f"   Calculé (PDL avec calculs): {_montant_calcule_communs:,.2f} €")
    print(f"   Écart final: {_ecart_final:+,.2f} € ({_ecart_final/_montant_f15_prorata*100:+.1f}%)")

    # 4. Synthèse finale simplifiée avec cohérence des montants
    _ecart_calculs_reels = _montant_calcule_communs - _montant_f15_filtre_calculs_reels
    _montant_pdl_sans_calculs = _total_f15_filtre - _montant_f15_filtre_calculs_reels

    print(f"\n🎯 SYNTHÈSE D'ATTRIBUTION:")
    print(f"   1. Écart brut initial: {_ecart_brut:+,.2f} €")
    print(f"   2. - PDL sans R151 exclus: {-_montant_pdl_exclus:+,.2f} €")
    print(f"   3. = Écart sur PDL avec R151: {_ecart_apres_exclusion:+,.2f} €")
    print(f"   4. - PDL avec R151 mais sans calcul: {-_montant_pdl_sans_calculs:+,.2f} €")
    print(f"   5. = Écart sur PDL avec calculs: {_ecart_calculs_reels:+,.2f} €")
    print(f"   6. - Impact prorata temporel: {-_impact_prorata:+,.2f} €")
    print(f"   7. = Écart final ajusté: {_ecart_final:+,.2f} €")

    _taux_explication = abs(_montant_pdl_exclus) / abs(_ecart_brut) * 100 if _ecart_brut != 0 else 0
    print(f"   📊 Taux d'explication par exclusion PDL sans R151: {_taux_explication:.1f}%")

    if abs(_ecart_final) < abs(_ecart_brut) * 0.1:  # Si écart final < 10% de l'écart brut
        print(f"   ✅ Écart final acceptable après corrections")
    else:
        print(f"   ⚠️ Écart final significatif - investigation requise")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Visualisations et synthèse""")
    return


@app.cell(hide_code=True)
def _(coverage_stats, df_comparison, temporal_stats):
    """Affichage des résultats principaux"""

    mo.vstack([
        mo.md("## 📊 Statistiques de couverture PDL"),
        coverage_stats,

        mo.md("## ⏰ Impact transformation temporelle"),
        temporal_stats,

        mo.md("## 🔝 Top 10 écarts après transformations"),
        df_comparison.head(10).select([
            "pdl", "turpe_variable_f15_prorata", "turpe_variable_calcule",
            "ecart_absolu", "ecart_relatif_pct"
        ]),

        mo.md("## 📈 Distribution des écarts"),
        df_comparison.select([
            pl.col("ecart_absolu").abs().alias("ecart_abs")
        ]).with_columns([
            pl.when(pl.col("ecart_abs") <= 1.0)
            .then(pl.lit("≤ 1€"))
            .when(pl.col("ecart_abs") <= 5.0)
            .then(pl.lit("1-5€"))
            .when(pl.col("ecart_abs") <= 10.0)
            .then(pl.lit("5-10€"))
            .otherwise(pl.lit("> 10€"))
            .alias("tranche_ecart")
        ]).group_by("tranche_ecart").agg([
            pl.len().alias("nb_pdl"),
            (pl.len() / len(df_comparison) * 100).alias("pourcentage")
        ]).sort("nb_pdl", descending=True)
    ])
    return


@app.cell
def _():
    mo.md(r"""# Validation de l'hypothèse des périodes manquantes""")
    return


@app.cell
def _():
    mo.md(r"""## Analyse des périodes manquantes par PDL""")
    return


@app.cell
def _(df_periodes_energie):
    mo.ui.table(df_periodes_energie)
    return


@app.cell(hide_code=True)
def analyser_periodes_manquantes(df_f15_variable, df_periodes_energie):
    """Analyse des périodes manquantes pour valider l'hypothèse des 3% d'écart"""

    print("🔍 Analyse des périodes manquantes par PDL...")

    # 1. Calculer les statistiques par PDL pour les périodes calculées
    stats_calcules = (
        df_periodes_energie
        .group_by("pdl")
        .agg([
            pl.col("debut").min().alias("premiere_periode_calculee"),
            pl.col("fin").max().alias("derniere_periode_calculee"),
            pl.len().alias("nb_periodes_calculees"),
            pl.col("nb_jours").filter(pl.col("data_complete")).sum().alias("total_jours"),
            pl.col("turpe_variable_eur").sum().alias("turpe_variable_total")
        ])
    )

    # 2. Calculer les statistiques par PDL pour les périodes F15
    stats_f15 = (
        df_f15_variable
        .group_by("pdl")
        .agg([
            pl.col("date_debut").min().alias("premiere_periode_f15"),
            pl.col("date_fin").max().alias("derniere_periode_f15"),
            pl.len().alias("nb_periodes_f15"),
            (pl.col("date_fin").max()- pl.col("date_debut").min()).dt.total_days().cast(pl.Int32).alias("total_jours_f15"),
            pl.col("montant_ht").sum().alias("turpe_f15_total")
        ])
    )

    # 3. Jointure pour comparer les deux sources
    comparaison_periodes = (
        stats_f15
        .join(stats_calcules, on="pdl", how="inner")
        .with_columns([
            # Calcul des jours de décalage en début/fin
            (pl.col("premiere_periode_calculee") - pl.col("premiere_periode_f15")).dt.total_days().alias("decalage_debut_jours"),
            (pl.col("derniere_periode_f15") - pl.col("derniere_periode_calculee")).dt.total_days().alias("decalage_fin_jours"),
            # Ratio de périodes
            (pl.col("total_jours") / pl.col("total_jours_f15")).alias("taux_couverture")
        ])
        .with_columns([
            # Classification des PDL par taux de couverture
            pl.when(pl.col("taux_couverture") >= .95)
            .then(pl.lit("Couverture complète (≥95%)"))
            .when(pl.col("taux_couverture") >= .80)
            .then(pl.lit("Couverture élevée (80-94%)"))
            .when(pl.col("taux_couverture") >= .50)
            .then(pl.lit("Couverture partielle (50-79%)"))
            .otherwise(pl.lit("Couverture faible (<50%)"))
            .alias("categorie_couverture"),

            (pl.col("turpe_f15_total") * pl.col("taux_couverture")).alias("f15_proratise_couverture")
        ])
        .with_columns([
            (pl.col("turpe_variable_total") - pl.col("f15_proratise_couverture")).alias("erreur_prorata")
        ])
    )

    # FILTRE TEMPORAIRE : Exclure PDL avec données aberrantes
    # TODO: Implémenter détection automatique de qualité (voir docs/qualite-donnees-r151.md)
    PDL_ABERRANTS = ["14290738060355"]  # Index aberrants sept 2025 (962 MWh/mois)
    comparaison_periodes = comparaison_periodes.filter(
        ~pl.col("pdl").is_in(PDL_ABERRANTS)
    )

    print(f"⚠️  PDL exclus de la validation : {len(PDL_ABERRANTS)} (données aberrantes)")

    # 4. Statistiques globales
    nb_pdl_total = len(comparaison_periodes)
    stats_par_categorie = (
        comparaison_periodes
        .group_by("categorie_couverture")
        .agg([
            pl.len().alias("nb_pdl"),
            pl.col("taux_couverture").mean().alias("taux_moyen"),
            pl.col("nb_periodes_calculees").sum().alias("periodes_calculees"),
            pl.col("nb_periodes_f15").sum().alias("periodes_f15"),
            pl.col("turpe_variable_total").sum().alias("montant_calcule"),
            pl.col("turpe_f15_total").sum().alias("montant_f15"),
            pl.col("f15_proratise_couverture").sum().alias("montant_f15_proratise")
        ])
        .with_columns([
            (pl.col("nb_pdl") / nb_pdl_total * 100).alias("proportion_pdl_pct"),
            (pl.col("montant_calcule") - pl.col("montant_f15")).alias("ecart_montant"),
            ((pl.col("montant_calcule") - pl.col("montant_f15")) / pl.col("montant_f15") * 100).alias("ecart_pct"),
            (pl.col("montant_calcule") - pl.col("montant_f15_proratise")).alias("ecart_montant_prorata"),
        ])
        .sort("taux_moyen", descending=True)
    )

    print(f"\n📊 ANALYSE COUVERTURE TEMPORELLE:")
    print(f"   Total PDL analysés: {nb_pdl_total}")

    for _row in stats_par_categorie.iter_rows(named=True):
        _cat = _row['categorie_couverture']
        _nb_pdl = _row['nb_pdl']
        _prop_pct = _row['proportion_pdl_pct']
        _taux_moyen = _row['taux_moyen']
        _ecart_pct = _row['ecart_pct']

        print(f"   📋 {_cat}:")
        print(f"      {_nb_pdl} PDL ({_prop_pct:.1f}%) - Couverture moyenne: {_taux_moyen:.1f}%")
        print(f"      Écart TURPE: {_ecart_pct:+.1f}%")
    return comparaison_periodes, stats_par_categorie


@app.cell
def _(comparaison_periodes):
    comparaison_periodes
    return


@app.cell
def _():
    mo.md(r"""## Analyse des erreurs avec prorata de couverture""")
    return


@app.cell(hide_code=True)
def analyse_erreurs_prorata(comparaison_periodes, stats_par_categorie):
    """Analyse détaillée des erreurs avec et sans prorata de couverture"""

    print("📊 ANALYSE DES ERREURS AVEC PRORATA DE COUVERTURE\n")

    # 1. Statistiques globales
    _total_calcule = comparaison_periodes.select(pl.col("turpe_variable_total").sum()).item()
    _total_f15_brut = comparaison_periodes.select(pl.col("turpe_f15_total").sum()).item()
    _total_f15_prorata = comparaison_periodes.select(pl.col("f15_proratise_couverture").sum()).item()

    _erreur_brute = _total_calcule - _total_f15_brut
    _erreur_prorata = _total_calcule - _total_f15_prorata

    print("💰 COMPARAISON GLOBALE:")
    print(f"   Calculé total: {_total_calcule:,.2f} €")
    print(f"   F15 brut: {_total_f15_brut:,.2f} €")
    print(f"   F15 proratisé: {_total_f15_prorata:,.2f} €")
    print(f"   Erreur brute: {_erreur_brute:+,.2f} € ({_erreur_brute/_total_f15_brut*100:+.2f}%)")
    print(f"   Erreur proratisée: {_erreur_prorata:+,.2f} € ({_erreur_prorata/_total_f15_prorata*100:+.2f}%)")

    # 2. Impact du prorata par catégorie de couverture
    print(f"\n📈 IMPACT DU PRORATA PAR CATÉGORIE:")

    stats_erreurs = (
        stats_par_categorie
        .with_columns([
            (pl.col("montant_calcule") - pl.col("montant_f15_proratise")).alias("erreur_prorata_categorie"),
            ((pl.col("montant_calcule") - pl.col("montant_f15_proratise")) / pl.col("montant_f15_proratise") * 100).alias("erreur_prorata_pct")
        ])
        .select([
            "categorie_couverture",
            "nb_pdl",
            "taux_moyen",
            "ecart_pct",
            "erreur_prorata_pct"
        ])
    )

    for _row in stats_erreurs.iter_rows(named=True):
        _cat = _row['categorie_couverture']
        _nb_pdl = _row['nb_pdl']
        _taux = _row['taux_moyen']
        _erreur_brute = _row['ecart_pct']
        _erreur_prorata = _row['erreur_prorata_pct']

        print(f"   📋 {_cat}:")
        print(f"      {_nb_pdl} PDL - Couverture: {_taux*100:.1f}%")
        print(f"      Erreur brute: {_erreur_brute:+.2f}% → Erreur proratisée: {_erreur_prorata:+.2f}%")

    # 3. Distribution des erreurs individuelles
    print(f"\n🎯 DISTRIBUTION DES ERREURS INDIVIDUELLES:")

    # Analyse des erreurs par PDL
    erreurs_stats = (
        comparaison_periodes
        .with_columns([
            (pl.col("erreur_prorata").abs()).alias("erreur_abs"),
            (pl.col("erreur_prorata") / pl.col("f15_proratise_couverture") * 100).alias("erreur_pct")
        ])
        .select([
            pl.col("erreur_abs").mean().alias("erreur_moyenne"),
            pl.col("erreur_abs").median().alias("erreur_mediane"),
            pl.col("erreur_abs").std().alias("erreur_ecart_type"),
            pl.col("erreur_pct").abs().lt(5.0).sum().alias("nb_pdl_erreur_5pct"),
            pl.col("erreur_pct").abs().lt(10.0).sum().alias("nb_pdl_erreur_10pct"),
            pl.len().alias("nb_pdl_total")
        ])
    )

    _stats = erreurs_stats.row(0, named=True)
    _precision_5pct = (_stats['nb_pdl_erreur_5pct'] / _stats['nb_pdl_total']) * 100
    _precision_10pct = (_stats['nb_pdl_erreur_10pct'] / _stats['nb_pdl_total']) * 100

    print(f"   Erreur moyenne: {_stats['erreur_moyenne']:.2f} €")
    print(f"   Erreur médiane: {_stats['erreur_mediane']:.2f} €")
    print(f"   Écart-type: {_stats['erreur_ecart_type']:.2f} €")
    print(f"   Précision ±5%: {_precision_5pct:.1f}% des PDL")
    print(f"   Précision ±10%: {_precision_10pct:.1f}% des PDL")

    # 4. Top écarts après prorata
    print(f"\n🔍 TOP 10 ÉCARTS APRÈS PRORATA:")

    top_ecarts = (
        comparaison_periodes
        .with_columns([
            (pl.col("erreur_prorata") / pl.col("f15_proratise_couverture") * 100).alias("erreur_pct")
        ])
        .filter(pl.col("erreur_pct").abs() > 0.1)
        .sort("erreur_prorata", descending=True)
        .select([
            "pdl",
            "taux_couverture",
            "categorie_couverture",
            "turpe_variable_total",
            "f15_proratise_couverture",
            "erreur_prorata",
            "erreur_pct"
        ])
        .head(10)
    )
    return (top_ecarts,)


@app.cell
def _(top_ecarts):
    mo.md("### Top 10 des écarts après prorata")
    top_ecarts
    return


@app.cell
def _():
    mo.md(r"""## Validation de l'hypothèse des périodes manquantes""")
    return


@app.cell(hide_code=True)
def validation_hypothese_final(comparaison_periodes):
    """Validation finale de l'hypothèse des périodes manquantes"""

    print("🎯 VALIDATION FINALE DE L'HYPOTHÈSE DES PÉRIODES MANQUANTES\n")

    # Calculs globaux
    _total_calcule = comparaison_periodes.select(pl.col("turpe_variable_total").sum()).item()
    _total_f15_brut = comparaison_periodes.select(pl.col("turpe_f15_total").sum()).item()
    _total_f15_prorata = comparaison_periodes.select(pl.col("f15_proratise_couverture").sum()).item()

    _erreur_brute_pct = (_total_calcule - _total_f15_brut) / _total_f15_brut * 100
    _erreur_prorata_pct = (_total_calcule - _total_f15_prorata) / _total_f15_prorata * 100
    _amelioration = abs(_erreur_brute_pct) - abs(_erreur_prorata_pct)

    print("📊 RÉSULTATS:")
    print(f"   Erreur avant ajustement: {_erreur_brute_pct:+.2f}%")
    print(f"   Erreur après prorata couverture: {_erreur_prorata_pct:+.2f}%")
    print(f"   Amélioration: {_amelioration:+.2f} points de %")

    # Validation de l'hypothèse
    print(f"\n🏆 CONCLUSION:")
    if abs(_erreur_prorata_pct) < 1.0:
        _resultat = "✅ HYPOTHÈSE VALIDÉE"
        _explication = "L'ajustement par couverture ramène l'erreur à <1%. Les périodes manquantes expliquent bien l'écart initial."
    elif abs(_erreur_prorata_pct) < 2.0 and _amelioration > 1.0:
        _resultat = "⚠️ HYPOTHÈSE PARTIELLEMENT VALIDÉE"
        _explication = "L'ajustement améliore significativement la précision. Les périodes manquantes sont un facteur important."
    elif _amelioration > 0.5:
        _resultat = "🟡 HYPOTHÈSE PARTIELLEMENT SUPPORTÉE"
        _explication = "L'ajustement améliore la précision mais des écarts résiduels subsistent."
    else:
        _resultat = "❌ HYPOTHÈSE NON VALIDÉE"
        _explication = "L'ajustement n'améliore pas significativement la précision. D'autres facteurs sont en jeu."

    print(f"   {_resultat}")
    print(f"   {_explication}")
    return


if __name__ == "__main__":
    app.run()
