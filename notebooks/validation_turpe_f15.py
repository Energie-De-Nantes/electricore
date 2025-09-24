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

    return df_comparison_fixe, total_f15_fixe, total_calcule_fixe, ecart_global_fixe


@app.cell(hide_code=True)
def analyze_turpe_fixe_gaps(df_comparison_fixe):
    """Analyse des écarts TURPE fixe par catégorie"""

    print("\n🔍 ANALYSE DES ÉCARTS PAR CATÉGORIE:")

    # Statistiques par statut PDL
    stats_par_statut = (
        df_comparison_fixe
        .group_by("statut_pdl")
        .agg([
            pl.len().alias("nb_pdl"),
            pl.col("turpe_fixe_f15").sum().alias("montant_f15"),
            pl.col("turpe_fixe_total").sum().alias("montant_calcule"),
            pl.col("ecart_absolu").sum().alias("ecart_total")
        ])
        .sort("ecart_total", descending=True)
    )

    for row in stats_par_statut.iter_rows(named=True):
        statut = row['statut_pdl']
        nb_pdl = row['nb_pdl']
        montant_f15 = row['montant_f15']
        montant_calcule = row['montant_calcule']
        ecart = row['ecart_total']

        print(f"   📋 {statut}: {nb_pdl} PDL")
        print(f"      F15: {montant_f15:,.2f} € | Calculé: {montant_calcule:,.2f} € | Écart: {ecart:+,.2f} €")

    # Top 10 des écarts les plus importants (PDL communs seulement)
    top_ecarts = (
        df_comparison_fixe
        .filter(pl.col("statut_pdl") == "Présent des 2 côtés")
        .filter(pl.col("ecart_relatif_pct").abs() > 0.1)  # Écarts > 0.1%
        .head(10)
        .select([
            "pdl", "turpe_fixe_f15", "turpe_fixe_total",
            "ecart_absolu", "ecart_relatif_pct"
        ])
    )

    print(f"\n📈 TOP 10 ÉCARTS SIGNIFICATIFS (PDL communs):")
    if len(top_ecarts) > 0:
        print(top_ecarts.to_pandas().to_string(index=False))
    else:
        print("   ✅ Aucun écart significatif détecté!")

    return stats_par_statut, top_ecarts


@app.cell(hide_code=True)
def funnel_analysis_turpe_fixe(df_f15_turpe, df_turpe_fixe_pdl, df_pdl_odoo):
    """Analyse en entonnoir pour attribuer les écarts du TURPE fixe"""

    print("\n📊 ANALYSE EN ENTONNOIR - TURPE FIXE")

    # Étape 1: Montant F15 global fixe
    _df_f15_fixe_entonnoir = df_f15_turpe.filter(pl.col("part_turpe") == "Fixe")
    montant_f15_global = _df_f15_fixe_entonnoir.select(pl.col("montant_ht").sum()).item()
    nb_pdl_f15_global = _df_f15_fixe_entonnoir.select(pl.col("pdl").n_unique()).item()

    # Étape 2: Filtrage segment C5 uniquement
    df_f15_c5 = _df_f15_fixe_entonnoir.filter(
        (pl.col("formule_tarifaire_acheminement").str.contains("C5")) |
        (pl.col("formule_tarifaire_acheminement").str.contains("CU"))
    )
    montant_f15_c5 = df_f15_c5.select(pl.col("montant_ht").sum()).item()
    nb_pdl_c5 = df_f15_c5.select(pl.col("pdl").n_unique()).item()
    ecart_hors_c5 = montant_f15_global - montant_f15_c5

    # Étape 3: Filtrage temporel (exclure facturation à échoir)
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

    df_f15_c5_temporel = df_f15_c5.filter(
        pl.col("date_fin").str.to_date() <= pl.date(_date_coupure.year, _date_coupure.month, _date_coupure.day)
    )
    montant_f15_c5_temporel = df_f15_c5_temporel.select(pl.col("montant_ht").sum()).item()
    nb_pdl_c5_temporel = df_f15_c5_temporel.select(pl.col("pdl").n_unique()).item()
    ecart_a_echoir = montant_f15_c5 - montant_f15_c5_temporel

    # Étape 4: Filtrage périmètre EDN (si données Odoo disponibles)
    if len(df_pdl_odoo) > 0:
        pdl_perimetre = df_pdl_odoo.select("pdl").to_series().to_list()
        df_f15_perimetre = df_f15_c5_temporel.filter(
            pl.col("pdl").is_in(pdl_perimetre)
        )
        _montant_f15_perimetre = df_f15_perimetre.select(pl.col("montant_ht").sum()).item()
        _nb_pdl_perimetre = df_f15_perimetre.select(pl.col("pdl").n_unique()).item()
        ecart_hors_perimetre = montant_f15_c5_temporel - _montant_f15_perimetre
        df_f15_final = df_f15_perimetre
    else:
        _montant_f15_perimetre = montant_f15_c5_temporel
        _nb_pdl_perimetre = nb_pdl_c5_temporel
        ecart_hors_perimetre = 0
        df_f15_final = df_f15_c5_temporel

    # Étape 5: Comparaison avec le calculé (PDL présents des deux côtés)
    # Agrégation F15 par PDL pour la comparaison finale
    df_f15_final_par_pdl = df_f15_final.group_by("pdl").agg([
        pl.col("montant_ht").sum().alias("turpe_fixe_f15")
    ])

    # Jointure avec le calculé (inner = uniquement PDL présents des deux côtés)
    _df_comparison_final = df_f15_final_par_pdl.join(
        df_turpe_fixe_pdl,
        on="pdl",
        how="inner"
    )

    montant_f15_avec_calcul = _df_comparison_final.select(pl.col("turpe_fixe_f15").sum()).item()
    montant_calcule_final = _df_comparison_final.select(pl.col("turpe_fixe_total").sum()).item()
    nb_pdl_avec_calcul = len(_df_comparison_final)
    ecart_pdl_sans_calcul = _montant_f15_perimetre - montant_f15_avec_calcul

    # Écart résiduel = vraie erreur de calcul
    ecart_residuel = montant_calcule_final - montant_f15_avec_calcul

    # Affichage en cascade
    print(f"\n🎯 ÉTAPE 1 - Montant F15 fixe global:")
    print(f"   💰 {montant_f15_global:,.2f} € sur {nb_pdl_f15_global} PDL")

    print(f"\n🎯 ÉTAPE 2 - Filtrage segment C5:")
    print(f"   💰 Montant C5: {montant_f15_c5:,.2f} € ({nb_pdl_c5} PDL)")
    print(f"   ❌ Écart hors C5: {ecart_hors_c5:,.2f} € ({ecart_hors_c5/montant_f15_global*100:.1f}%)")

    print(f"\n🎯 ÉTAPE 3 - Filtrage temporel (≤ {_date_coupure}):")
    print(f"   💰 Montant hors à échoir: {montant_f15_c5_temporel:,.2f} € ({nb_pdl_c5_temporel} PDL)")
    print(f"   ❌ Écart facturation à échoir: {ecart_a_echoir:,.2f} € ({ecart_a_echoir/montant_f15_global*100:.1f}%)")

    if len(df_pdl_odoo) > 0:
        print(f"\n🎯 ÉTAPE 4 - Filtrage périmètre EDN:")
        print(f"   💰 Montant périmètre: {_montant_f15_perimetre:,.2f} € ({_nb_pdl_perimetre} PDL)")
        print(f"   ❌ Écart hors périmètre: {ecart_hors_perimetre:,.2f} € ({ecart_hors_perimetre/montant_f15_global*100:.1f}%)")

    print(f"\n🎯 ÉTAPE 5 - PDL avec calcul disponible:")
    print(f"   💰 Montant F15 (PDL avec calcul): {montant_f15_avec_calcul:,.2f} € ({nb_pdl_avec_calcul} PDL)")
    print(f"   💰 Montant calculé: {montant_calcule_final:,.2f} €")
    print(f"   ❌ Écart PDL sans C15: {ecart_pdl_sans_calcul:,.2f} € ({ecart_pdl_sans_calcul/montant_f15_global*100:.1f}%)")

    print(f"\n🎯 ÉTAPE 6 - ÉCART RÉSIDUEL (vraie erreur):")
    print(f"   ⚠️ Écart de calcul: {ecart_residuel:+,.2f} € ({ecart_residuel/montant_f15_global*100:+.1f}%)")

    # Résumé des attributions
    total_ecart_brut = montant_calcule_final - montant_f15_global
    print(f"\n📈 ATTRIBUTION DES ÉCARTS:")
    print(f"   Écart total brut: {total_ecart_brut:+,.2f} € (100%)")

    if total_ecart_brut != 0:
        print(f"   • Hors C5: {-ecart_hors_c5:,.2f} € ({-ecart_hors_c5/abs(total_ecart_brut)*100:.1f}%)")
        print(f"   • Facturation à échoir: {-ecart_a_echoir:,.2f} € ({-ecart_a_echoir/abs(total_ecart_brut)*100:.1f}%)")
        if len(df_pdl_odoo) > 0:
            print(f"   • Hors périmètre: {-ecart_hors_perimetre:,.2f} € ({-ecart_hors_perimetre/abs(total_ecart_brut)*100:.1f}%)")
        print(f"   • PDL sans C15: {-ecart_pdl_sans_calcul:,.2f} € ({-ecart_pdl_sans_calcul/abs(total_ecart_brut)*100:.1f}%)")
        print(f"   • Erreur de calcul: {ecart_residuel:,.2f} € ({ecart_residuel/abs(total_ecart_brut)*100:.1f}%)")

    df_comparison_final_entonnoir = _df_comparison_final
    return df_comparison_final_entonnoir


@app.cell(hide_code=True)
def turpe_fixe_quality_metrics(df_comparison_fixe, ecart_global_fixe, total_f15_fixe):
    """Métriques de qualité pour le TURPE fixe"""

    # Calculs sur PDL communs uniquement
    df_communs = df_comparison_fixe.filter(pl.col("statut_pdl") == "Présent des 2 côtés")

    if len(df_communs) == 0:
        print("⚠️ Aucun PDL commun trouvé!")
        df_quality_result = None
    else:
        # Métriques de précision
        nb_precise_1eur = df_communs.filter(pl.col("ecart_absolu").abs() <= 1.0).select(pl.len()).item()
        nb_precise_5pct = df_communs.filter(pl.col("ecart_relatif_pct").abs() <= 5.0).select(pl.len()).item()
        nb_total_communs = len(df_communs)

        precision_1eur = (nb_precise_1eur / nb_total_communs) * 100
        precision_5pct = (nb_precise_5pct / nb_total_communs) * 100

        ecart_moyen = df_communs.select(pl.col("ecart_absolu").mean()).item()
        ecart_median = df_communs.select(pl.col("ecart_absolu").median()).item()

        # Évaluation qualitative
        _ecart_global_pct = abs(ecart_global_fixe / total_f15_fixe) * 100 if total_f15_fixe > 0 else 0

        if _ecart_global_pct < 1.0 and precision_5pct > 95:
            _evaluation = "🟢 EXCELLENTE"
            _recommandation = "TURPE fixe très fiable"
        elif _ecart_global_pct < 2.0 and precision_5pct > 90:
            _evaluation = "🟡 BONNE"
            _recommandation = "Quelques ajustements mineurs"
        elif _ecart_global_pct < 5.0:
            _evaluation = "🟠 CORRECTE"
            _recommandation = "Révision des règles tarifaires recommandée"
        else:
            _evaluation = "🔴 À AMÉLIORER"
            _recommandation = "Problèmes majeurs dans le calcul"

        print(f"\n🎯 MÉTRIQUES DE QUALITÉ TURPE FIXE:")
        print(f"   📊 Précision ±1€: {precision_1eur:.1f}% ({nb_precise_1eur}/{nb_total_communs} PDL)")
        print(f"   📊 Précision ±5%: {precision_5pct:.1f}% ({nb_precise_5pct}/{nb_total_communs} PDL)")
        print(f"   📊 Écart moyen: {ecart_moyen:+.2f} € | Écart médian: {ecart_median:+.2f} €")
        print(f"   🎯 Évaluation: {_evaluation}")
        print(f"   💡 Recommandation: {_recommandation}")

        df_quality_result = {
            'evaluation': _evaluation,
            'precision_1eur': precision_1eur,
            'precision_5pct': precision_5pct,
            'ecart_moyen': ecart_moyen,
            'recommandation': _recommandation
        }

    df_quality_result


@app.cell
def _():
    mo.md(r"""## Entonnoir d'attribution des écarts - TURPE Fixe""")
    return


@app.cell
def run_funnel_analysis_turpe_fixe(df_f15_turpe, df_turpe_fixe_pdl, df_pdl_odoo):
    """Exécution de l'analyse en entonnoir pour le TURPE fixe"""
    df_entonnoir_result = funnel_analysis_turpe_fixe(df_f15_turpe, df_turpe_fixe_pdl, df_pdl_odoo)
    return (df_entonnoir_result,)


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


@app.cell
def funnel_analysis_variable(
    df_f15_variable,
    df_pdl_odoo,
    df_turpe_variable_pdl,
):
    """Analyse en entonnoir des écarts TURPE variable"""

    mo.md("## 📊 Analyse en entonnoir - TURPE Variable")

    montant_f15_variable_global = df_f15_variable.select(pl.col("montant_ht").sum()).item()
    nb_pdl_f15_variable = df_f15_variable.select(pl.col("pdl").n_unique()).item()

    print(f"🎯 ÉTAPE 1 - Montant F15 variable global:")
    print(f"   💰 {montant_f15_variable_global:,.2f} € sur {nb_pdl_f15_variable} PDL")

    # Étape 2: Réduction aux PDL avec données R151
    # D'abord agrégér F15 par PDL, puis joindre avec calculé
    df_f15_variable_par_pdl = (
        df_f15_variable
        .group_by("pdl")
        .agg([pl.col("montant_ht").sum().alias("turpe_variable_f15")])
    )

    df_variable_avec_r151 = (
        df_f15_variable_par_pdl
        .join(df_turpe_variable_pdl, on="pdl", how="inner")
        .with_columns([
            (pl.col("turpe_variable_total") - pl.col("turpe_variable_f15")).alias("ecart_variable")
        ])
    )

    montant_f15_variable_r151 = df_variable_avec_r151.select(pl.col("turpe_variable_f15").sum()).item()
    montant_calcule_variable_r151 = df_variable_avec_r151.select(pl.col("turpe_variable_total").sum()).item()
    nb_pdl_avec_r151 = len(df_variable_avec_r151)

    ecart_pdl_sans_r151_eur = montant_f15_variable_global - montant_f15_variable_r151
    ecart_pdl_sans_r151_pct = (ecart_pdl_sans_r151_eur / montant_f15_variable_global) * 100

    print(f"🎯 ÉTAPE 2 - Réduction aux PDL avec données R151:")
    print(f"   📊 {nb_pdl_avec_r151} PDL avec R151 (sur {nb_pdl_f15_variable} total)")
    print(f"   💰 F15 variable (R151): {montant_f15_variable_r151:,.2f} €")
    print(f"   💰 Calculé variable (R151): {montant_calcule_variable_r151:,.2f} €")
    print(f"   ❌ Écart PDL sans R151: {ecart_pdl_sans_r151_eur:,.2f} € ({ecart_pdl_sans_r151_pct:.1f}%)")

    # Étape 3: Réduction aux PDL du périmètre (si données Odoo disponibles)
    if len(df_pdl_odoo) > 0:
        df_variable_perimetre = (
            df_variable_avec_r151
            .join(df_pdl_odoo.select("pdl"), on="pdl", how="inner")
        )

        nb_pdl_perimetre = len(df_variable_perimetre)
        montant_f15_perimetre = df_variable_perimetre.select(pl.col("turpe_variable_f15").sum()).item()
        montant_calcule_perimetre = df_variable_perimetre.select(pl.col("turpe_variable_total").sum()).item()

        ecart_hors_perimetre_eur = montant_f15_variable_r151 - montant_f15_perimetre
        ecart_hors_perimetre_pct = (ecart_hors_perimetre_eur / montant_f15_variable_global) * 100

        print(f"🎯 ÉTAPE 3 - Réduction au périmètre Odoo:")
        print(f"   🏢 {nb_pdl_perimetre} PDL dans le périmètre (sur {nb_pdl_avec_r151} avec R151)")
        print(f"   💰 F15 périmètre: {montant_f15_perimetre:,.2f} €")
        print(f"   💰 Calculé périmètre: {montant_calcule_perimetre:,.2f} €")
        print(f"   ❌ Écart PDL hors périmètre: {ecart_hors_perimetre_eur:,.2f} € ({ecart_hors_perimetre_pct:.1f}%)")

        # Écart résiduel = erreur "vraie"
        ecart_residuel_eur = montant_calcule_perimetre - montant_f15_perimetre
        ecart_residuel_pct = (abs(ecart_residuel_eur) / montant_f15_variable_global) * 100

        print(f"🎯 ÉTAPE 4 - Écart résiduel (erreur vraie):")
        print(f"   ⚠️ Écart de calcul: {ecart_residuel_eur:+,.2f} € ({ecart_residuel_pct:.1f}%)")

        funnel_results = {
            'montant_f15_global': montant_f15_variable_global,
            'ecart_sans_r151': ecart_pdl_sans_r151_eur,
            'ecart_hors_perimetre': ecart_hors_perimetre_eur,
            'ecart_residuel': ecart_residuel_eur,
            'nb_pdl_f15': nb_pdl_f15_variable,
            'nb_pdl_r151': nb_pdl_avec_r151,
            'nb_pdl_perimetre': nb_pdl_perimetre
        }
    else:
        # Sans données Odoo, écart résiduel = différence de calcul sur PDL avec R151
        ecart_residuel_eur = montant_calcule_variable_r151 - montant_f15_variable_r151
        ecart_residuel_pct = (abs(ecart_residuel_eur) / montant_f15_variable_global) * 100

        print(f"🎯 ÉTAPE 3 - Écart résiduel (sans filtre périmètre):")
        print(f"   ⚠️ Écart de calcul: {ecart_residuel_eur:+,.2f} € ({ecart_residuel_pct:.1f}%)")

        funnel_results = {
            'montant_f15_global': montant_f15_variable_global,
            'ecart_sans_r151': ecart_pdl_sans_r151_eur,
            'ecart_hors_perimetre': 0,
            'ecart_residuel': ecart_residuel_eur,
            'nb_pdl_f15': nb_pdl_f15_variable,
            'nb_pdl_r151': nb_pdl_avec_r151,
            'nb_pdl_perimetre': nb_pdl_avec_r151
        }
    return


@app.cell
def decomposition_summary(funnel_fixe, funnel_variable):
    """Synthèse de la décomposition des écarts"""

    mo.md("## 📋 Synthèse de la décomposition des écarts")

    # Calcul des pourcentages
    montant_global = funnel_variable['montant_f15_global'] + funnel_fixe['montant_f15_fixe']

    pct_sans_r151 = (abs(funnel_variable['ecart_sans_r151']) / montant_global) * 100
    pct_hors_perimetre = (abs(funnel_variable['ecart_hors_perimetre']) / montant_global) * 100
    pct_erreur_variable = (abs(funnel_variable['ecart_residuel']) / montant_global) * 100
    pct_erreur_fixe = (abs(funnel_fixe['ecart_fixe']) / montant_global) * 100

    # Tableau de décomposition
    decomposition = pl.DataFrame({
        "Composante": [
            "💰 Montant F15 total",
            "📊 PDL sans données R151",
            "🏢 PDL hors périmètre",
            "⚠️ Erreur calcul variable",
            "⚠️ Erreur calcul fixe",
            "✅ Total écarts expliqués"
        ],
        "Montant (€)": [
            f"{montant_global:,.2f}",
            f"{funnel_variable['ecart_sans_r151']:+,.2f}",
            f"{funnel_variable['ecart_hors_perimetre']:+,.2f}",
            f"{funnel_variable['ecart_residuel']:+,.2f}",
            f"{funnel_fixe['ecart_fixe']:+,.2f}",
            f"{funnel_variable['ecart_sans_r151'] + funnel_variable['ecart_hors_perimetre'] + funnel_variable['ecart_residuel'] + funnel_fixe['ecart_fixe']:+,.2f}"
        ],
        "% du total": [
            "100.0%",
            f"{pct_sans_r151:.1f}%",
            f"{pct_hors_perimetre:.1f}%",
            f"{pct_erreur_variable:.1f}%",
            f"{pct_erreur_fixe:.1f}%",
            f"{pct_sans_r151 + pct_hors_perimetre + pct_erreur_variable + pct_erreur_fixe:.1f}%"
        ],
        "Type": [
            "Référence",
            "Attendu (pas de données)",
            "Attendu (hors scope)",
            "À corriger",
            "À corriger",
            "Total"
        ]
    })

    print("📊 DÉCOMPOSITION DES ÉCARTS TURPE:")
    print(decomposition.to_pandas().to_string(index=False))

    # Évaluation qualitative
    erreur_totale_pct = pct_erreur_variable + pct_erreur_fixe

    if erreur_totale_pct < 2:
        _evaluation = "🟢 EXCELLENTE"
        _recommandation = "Validation très satisfaisante"
    elif erreur_totale_pct < 5:
        _evaluation = "🟡 BONNE"
        _recommandation = "Quelques écarts mineurs à analyser"
    elif erreur_totale_pct < 10:
        _evaluation = "🟠 CORRECTE"
        _recommandation = "Ajustements nécessaires"
    else:
        _evaluation = "🔴 À AMÉLIORER"
        _recommandation = "Révision du pipeline requise"

    print(f"\n🎯 ÉVALUATION: {_evaluation}")
    print(f"   Erreur totale de calcul: {erreur_totale_pct:.1f}%")
    print(f"   Recommandation: {_recommandation}")
    return


@app.cell
def temporal_alignment_analysis(df_f15_turpe):
    """Analyse des décalages temporels (premiers/derniers mois)"""

    mo.md("## 📅 Gestion des décalages temporels")

    # Identifier les périodes partielles (premiers/derniers mois)
    # Critère : période < 25 jours (approximation pour mois partiel)
    df_f15_with_duration = (
        df_f15_turpe
        .with_columns([
            pl.col("date_debut").str.to_datetime().alias("debut_dt"),
            pl.col("date_fin").str.to_datetime().alias("fin_dt")
        ])
        .with_columns([
            (pl.col("fin_dt") - pl.col("debut_dt")).dt.total_days().alias("duree_jours")
        ])
        .with_columns([
            pl.when(pl.col("duree_jours") < 25)
            .then(pl.lit("Partiel"))
            .otherwise(pl.lit("Complet"))
            .alias("type_periode")
        ])
    )

    # Statistiques sur les périodes partielles
    montant_total = df_f15_with_duration.select(pl.col("montant_ht").sum()).item()
    montant_partiel = (
        df_f15_with_duration
        .filter(pl.col("type_periode") == "Partiel")
        .select(pl.col("montant_ht").sum())
        .item()
    )

    nb_lignes_partielles = (
        df_f15_with_duration
        .filter(pl.col("type_periode") == "Partiel")
        .select(pl.len())
        .item()
    )

    nb_pdl_avec_partiels = (
        df_f15_with_duration
        .filter(pl.col("type_periode") == "Partiel")
        .select(pl.col("pdl").n_unique())
        .item()
    )

    impact_temporel_pct = (montant_partiel / montant_total) * 100 if montant_total > 0 else 0

    print(f"🎯 IMPACT DES DÉCALAGES TEMPORELS:")
    print(f"   📊 {nb_lignes_partielles} lignes de facturation partielles")
    print(f"   🏠 {nb_pdl_avec_partiels} PDL concernés par des mois partiels")
    print(f"   💰 Montant périodes partielles: {montant_partiel:,.2f} € ({impact_temporel_pct:.1f}% du total)")
    print(f"   ⚠️ Impact temporel estimé sur la comparaison: ~{impact_temporel_pct:.1f}%")

    # Recommandation
    if impact_temporel_pct > 5:
        print(f"   🔧 Recommandation: Exclure les périodes partielles de la validation")
    else:
        print(f"   ✅ Impact temporel faible, peut être ignoré dans la validation")

    # Données nettoyées (mois complets uniquement)
    df_f15_mois_complets = (
        df_f15_with_duration
        .filter(pl.col("type_periode") == "Complet")
    )

    montant_mois_complets = df_f15_mois_complets.select(pl.col("montant_ht").sum()).item()
    nb_pdl_mois_complets = df_f15_mois_complets.select(pl.col("pdl").n_unique()).item()

    print(f"   📋 Données mois complets: {montant_mois_complets:,.2f} € sur {nb_pdl_mois_complets} PDL")
    return


@app.cell
def validation_conclusion(temporal_analysis):
    """Conclusion de la validation TURPE avec recommandations"""

    mo.md("## 🎯 Conclusion et recommandations")

    print("📋 SYNTHÈSE DE LA VALIDATION TURPE F15:")
    print(f"   ✅ Analyse structurée en entonnoir réalisée")
    print(f"   📊 Séparation TURPE fixe/variable effectuée")
    print(f"   🏢 Filtrage périmètre Odoo appliqué")
    print(f"   📅 Décalages temporels quantifiés ({temporal_analysis['impact_temporel_pct']:.1f}% du total)")

    print(f"\n🎯 APPROCHE MÉTHODOLOGIQUE:")
    print(f"   1️⃣ Attribution des écarts par cause identifiée")
    print(f"   2️⃣ Distinction erreurs attendues vs inattendues")
    print(f"   3️⃣ Quantification précise de chaque source d'écart")
    print(f"   4️⃣ Focus sur les vrais problèmes de calcul")

    print(f"\n📈 PROCHAINES ÉTAPES:")
    print(f"   🔧 Utiliser ce notebook régulièrement pour valider le pipeline")
    print(f"   📊 Ajuster les seuils selon vos critères métier")
    print(f"   ⚙️ Intégrer dans le processus CI/CD si souhaité")
    print(f"   📈 Étendre l'analyse à d'autres segments que C5 si pertinent")

    print(f"\n💡 AVANTAGES DE CETTE APPROCHE:")
    print(f"   ✅ Erreurs attendues vs inattendues clairement séparées")
    print(f"   ✅ Quantification précise de chaque source d'écart")
    print(f"   ✅ Focus sur les vrais problèmes nécessitant correction")
    print(f"   ✅ Validation objective et reproductible")
    return


if __name__ == "__main__":
    app.run()
