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
            WHEN libelle_ev LIKE '%Part fixe%' THEN 'Fixe'
            WHEN libelle_ev LIKE '%Part variable%' THEN 'Variable'
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
    total_montant = df_f15_turpe.select(pl.col("montant_ht").sum()).item()
    nb_pdl_uniques = df_f15_turpe.select(pl.col("pdl").n_unique()).item()
    date_min = df_f15_turpe.select(pl.col("date_debut").min()).item()
    date_max = df_f15_turpe.select(pl.col("date_fin").max()).item()

    print(f"✅ Données F15 chargées en {_load_time_f15:.1f}s")
    print(f"📊 {len(df_f15_turpe):,} lignes de facturation TURPE")
    print(f"💰 Montant total TURPE F15: {total_montant:,.2f} €")
    print(f"🏠 {nb_pdl_uniques} PDL uniques")
    print(f"📅 Période: {date_min} → {date_max}")
    return df_f15_turpe, nb_pdl_uniques, total_montant


@app.cell
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


@app.cell
def show_f15_summary(df_f15_par_composante, df_f15_par_mois, df_f15_par_pdl):
    """Affichage des résumés F15"""

    # Top 10 PDL par montant TURPE
    top_pdl = df_f15_par_pdl.head(10).to_pandas()

    # Évolution mensuelle
    evolution_mensuelle = df_f15_par_mois.to_pandas()

    # Répartition par composante
    repartition_composantes = df_f15_par_composante.to_pandas()
    return


@app.cell
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
def calculate_turpe_pipeline(df_historique, df_releves):
    """Calcul TURPE complet via le pipeline"""

    mo.md("### Exécution du pipeline TURPE")

    print("🔄 Calcul des périodes d'énergie...")
    _start_time_turpe = time.time()

    # Pipeline énergie complet (inclut chronologie + calcul + TURPE variable)
    lf_periodes_energie = pipeline_energie_polars(
        pl.LazyFrame(df_historique),
        pl.LazyFrame(df_releves)
    )
    df_periodes_energie = lf_periodes_energie.collect()

    print("🔄 Calcul des périodes d'abonnement...")

    # Pipeline abonnements complet (inclut calcul + TURPE fixe)
    lf_periodes_abonnement = pipeline_abonnements(pl.LazyFrame(df_historique))
    df_periodes_abonnement = lf_periodes_abonnement.collect()

    print("🔄 Agrégation TURPE pour comparaison...")

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

    # Agrégation du TURPE variable par PDL
    df_turpe_variable_pdl = (
        df_periodes_energie
        .group_by("pdl")
        .agg([
            pl.col("turpe_variable").sum().alias("turpe_variable_total")
        ])
    )

    # Combinaison TURPE fixe + variable par PDL
    df_turpe_calcule = (
        df_turpe_fixe_pdl
        .join(df_turpe_variable_pdl, on="pdl", how="full")
        .with_columns([
            pl.col("turpe_fixe_total").fill_null(0.0),
            pl.col("turpe_variable_total").fill_null(0.0)
        ])
        .with_columns([
            (pl.col("turpe_fixe_total") + pl.col("turpe_variable_total")).alias("turpe_total")
        ])
    )

    calc_time = time.time() - _start_time_turpe

    # Statistiques du calcul TURPE complet (fixe + variable)
    total_turpe_calcule = df_turpe_calcule.select(
        pl.col("turpe_total").sum()
    ).item()

    total_turpe_fixe = df_turpe_calcule.select(
        pl.col("turpe_fixe_total").sum()
    ).item()

    total_turpe_variable = df_turpe_calcule.select(
        pl.col("turpe_variable_total").sum()
    ).item()

    _nb_pdl_calcule_turpe = df_turpe_calcule.select(pl.col("pdl").n_unique()).item()

    print(f"✅ Pipeline TURPE exécuté en {calc_time:.1f}s")
    print(f"💰 Montant total TURPE calculé: {total_turpe_calcule:,.2f} €")
    print(f"   └─ TURPE fixe: {total_turpe_fixe:,.2f} €")
    print(f"   └─ TURPE variable: {total_turpe_variable:,.2f} €")
    print(f"🏠 {_nb_pdl_calcule_turpe} PDL traités")
    return df_turpe_calcule, total_turpe_calcule


@app.cell
def global_comparison(
    df_turpe_calcule,
    nb_pdl_uniques,
    total_montant,
    total_turpe_calcule,
):
    """Comparaison globale F15 vs Pipeline"""

    mo.md("## 📈 Comparaison globale F15 vs Pipeline")

    # Calculs de base
    ecart_absolu = total_turpe_calcule - total_montant
    ecart_relatif = (ecart_absolu / total_montant) * 100 if total_montant != 0 else 0

    _nb_pdl_calcule_global = df_turpe_calcule.select(pl.col("pdl").n_unique()).item()
    pdl_coverage = (_nb_pdl_calcule_global / nb_pdl_uniques) * 100 if nb_pdl_uniques != 0 else 0

    # Statistiques récapitulatives
    stats_globales = {
        "Métrique": [
            "Montant TURPE F15 (€)",
            "Montant TURPE calculé (€)",
            "Écart absolu (€)",
            "Écart relatif (%)",
            "PDL dans F15",
            "PDL calculés",
            "Taux de couverture (%)"
        ],
        "Valeur": [
            f"{total_montant:,.2f}",
            f"{total_turpe_calcule:,.2f}",
            f"{ecart_absolu:+,.2f}",
            f"{ecart_relatif:+.2f}%",
            f"{nb_pdl_uniques:,}",
            f"{_nb_pdl_calcule_global:,}",
            f"{pdl_coverage:.1f}%"
        ]
    }

    df_stats = pl.DataFrame(stats_globales)
    df_stats
    return ecart_relatif, pdl_coverage


@app.cell
def pdl_level_comparison(df_f15_par_pdl, df_turpe_calcule):
    """Comparaison au niveau PDL"""

    mo.md("## 🏠 Comparaison par PDL")

    # Agrégation des montants calculés par PDL (TURPE complet: fixe + variable)
    df_calcule_par_pdl = (
        df_turpe_calcule
        .select([
            "pdl",
            pl.col("turpe_total").alias("montant_total_calcule")
        ])
    )

    # Jointure F15 vs Calculé
    df_comparaison_pdl = (
        df_f15_par_pdl
        .join(df_calcule_par_pdl, on="pdl", how="full")
        .with_columns([
            pl.col("montant_total_f15").fill_null(0.0),
            pl.col("montant_total_calcule").fill_null(0.0)
        ])
        .with_columns([
            (pl.col("montant_total_calcule") - pl.col("montant_total_f15")).alias("ecart_absolu"),
            pl.when(pl.col("montant_total_f15") != 0.0)
            .then(((pl.col("montant_total_calcule") - pl.col("montant_total_f15")) / pl.col("montant_total_f15") * 100))
            .otherwise(pl.lit(None))
            .alias("ecart_relatif_pct")
        ])
        .sort("ecart_absolu", descending=True)
    )

    # PDL avec écarts significatifs (>5%)
    df_ecarts_significatifs = (
        df_comparaison_pdl
        .filter(pl.col("ecart_relatif_pct").abs() > 5.0)
        .select([
            "pdl",
            "montant_total_f15",
            "montant_total_calcule",
            "ecart_absolu",
            "ecart_relatif_pct"
        ])
    )

    # PDL manquants dans le calcul (compteurs non-intelligents probables)
    df_pdl_manquants = (
        df_comparaison_pdl
        .filter((pl.col("montant_total_f15") > 0) & (pl.col("montant_total_calcule") == 0))
        .select(["pdl", "montant_total_f15", "premiere_periode", "derniere_periode"])
        .sort("montant_total_f15", descending=True)
    )
    return df_comparaison_pdl, df_ecarts_significatifs, df_pdl_manquants


@app.cell
def show_pdl_analysis(
    df_comparaison_pdl,
    df_ecarts_significatifs,
    df_pdl_manquants,
):
    """Affichage de l'analyse par PDL"""

    # Statistiques sur les écarts
    stats_ecarts = df_comparaison_pdl.select([
        pl.col("ecart_absolu").mean().alias("ecart_moyen"),
        pl.col("ecart_absolu").median().alias("ecart_median"),
        pl.col("ecart_relatif_pct").mean().alias("ecart_relatif_moyen"),
        pl.len().alias("total_pdl"),
        (pl.col("ecart_relatif_pct").abs() > 5.0).sum().alias("pdl_ecart_significatif"),
        ((pl.col("montant_total_f15") > 0) & (pl.col("montant_total_calcule") == 0)).sum().alias("pdl_manquants")
    ])
    # Tableaux
    mo.vstack([stats_ecarts, df_ecarts_significatifs, df_pdl_manquants])
    return (stats_ecarts,)


@app.cell
def _(stats_ecarts):
    stats_ecarts
    return


@app.cell
def temporal_comparison(df_f15_par_mois, df_turpe_calcule):
    """Comparaison temporelle mensuelle"""

    mo.md("## 📅 Comparaison temporelle")

    # Agrégation mensuelle du calcul pipeline (TURPE complet: fixe + variable)
    df_calcule_par_mois = (
        df_turpe_calcule
        .with_columns([
            # Utiliser la date de début de période pour le regroupement mensuel
            # Convertir en timezone naive pour éviter les conflits de join
            pl.col("date_debut_periode").dt.truncate("1mo").dt.replace_time_zone(None).alias("mois")
        ])
        .group_by("mois")
        .agg([
            pl.col("turpe_total").sum().alias("montant_calcule"),
            pl.col("pdl").n_unique().alias("nb_pdl_calcule")
        ])
        .sort("mois")
    )

    # Jointure temporelle
    df_comparaison_mensuelle = (
        df_f15_par_mois
        .join(df_calcule_par_mois, on="mois", how="full")
        .with_columns([
            pl.col("montant_f15").fill_null(0.0),
            pl.col("montant_calcule").fill_null(0.0),
            pl.col("nb_pdl").fill_null(0),
            pl.col("nb_pdl_calcule").fill_null(0)
        ])
        .with_columns([
            (pl.col("montant_calcule") - pl.col("montant_f15")).alias("ecart_absolu"),
            pl.when(pl.col("montant_f15") != 0.0)
            .then(((pl.col("montant_calcule") - pl.col("montant_f15")) / pl.col("montant_f15") * 100))
            .otherwise(pl.lit(None))
            .alias("ecart_relatif_pct")
        ])
        .sort("mois")
    )
    return (df_comparaison_mensuelle,)


@app.cell(hide_code=True)
def show_temporal_analysis(df_comparaison_mensuelle):
    """Affichage de l'analyse temporelle"""

    # Mois avec les plus gros écarts
    df_ecarts_temporels = (
        df_comparaison_mensuelle
        .filter(pl.col("ecart_relatif_pct").abs() > 10.0)
        .select([
            "mois",
            "montant_f15",
            "montant_calcule",
            "ecart_absolu",
            "ecart_relatif_pct",
            "nb_pdl",
            "nb_pdl_calcule"
        ])
        .sort("ecart_relatif_pct", descending=True)
    )
    df_ecarts_temporels
    return


@app.cell(hide_code=True)
def synthesis_report(
    df_ecarts_significatifs,
    df_pdl_manquants,
    ecart_relatif,
    pdl_coverage,
):
    """Rapport de synthèse final"""

    mo.md("## 📋 Rapport de synthèse")

    # Calcul des métriques de qualité
    nb_ecarts_significatifs = len(df_ecarts_significatifs)
    nb_pdl_manquants = len(df_pdl_manquants)

    # Évaluation globale
    if abs(ecart_relatif) < 2.0 and pdl_coverage > 95.0:
        evaluation = "🟢 EXCELLENTE"
        recommandation = "La validation est très satisfaisante. Écarts dans les limites acceptables."
    elif abs(ecart_relatif) < 5.0 and pdl_coverage > 90.0:
        evaluation = "🟡 BONNE"
        recommandation = "Validation satisfaisante avec quelques écarts à analyser."
    elif abs(ecart_relatif) < 10.0 and pdl_coverage > 80.0:
        evaluation = "🟠 CORRECTE"
        recommandation = "Validation correcte mais nécessite des ajustements."
    else:
        evaluation = "🔴 À AMÉLIORER"
        recommandation = "Écarts importants détectés. Révision du pipeline recommandée."

    # Résumé exécutif
    resume = f"""
    ## Résumé exécutif - Validation TURPE

    ### Évaluation globale : {evaluation}

    **Métriques clés :**
    - Écart relatif global : **{ecart_relatif:+.2f}%**
    - Taux de couverture PDL : **{pdl_coverage:.1f}%**
    - PDL avec écarts >5% : **{nb_ecarts_significatifs}**
    - PDL manquants (compteurs non-intelligents) : **{nb_pdl_manquants}**

    ### Recommandation
    {recommandation}

    ### Actions suggérées
    """

    if nb_pdl_manquants > 0:
        resume += f"- ✅ **{nb_pdl_manquants} PDL manquants identifiés** (compteurs non-intelligents)\n"

    if nb_ecarts_significatifs > 0:
        resume += f"- 🔍 **Analyser les {nb_ecarts_significatifs} PDL avec écarts >5%**\n"

    if abs(ecart_relatif) > 5.0:
        resume += "- ⚡ **Réviser les règles TURPE ou la logique de calcul**\n"

    resume += "- 📊 **Monitoring continu** avec ce notebook\n"
    mo.md(resume)
    return


if __name__ == "__main__":
    app.run()
