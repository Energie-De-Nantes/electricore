import marimo

__generated_with = "0.15.3"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl
    import pandas as pd
    import sys
    from pathlib import Path
    from datetime import datetime, timezone
    import time

    # Ajouter le chemin du projet
    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    # Import des pipelines pandas
    from electricore.core.pipeline_abonnements import (
        pipeline_abonnement as pipeline_pandas,
        generer_periodes_abonnement as genererperiodes_pandas
    )

    # Import des pipelines Polars
    from electricore.core.pipelines.abonnements import (
        pipeline_abonnements as pipeline,
        generer_periodes_abonnement as generer_periodes
    )
    from electricore.core.pipelines.perimetre import detecter_points_de_rupture, inserer_evenements_facturation

    # Import des loaders DuckDB
    from electricore.core.loaders.duckdb_loader import c15


@app.cell
def _():
    mo.md(
        r"""
    # Pipeline Abonnements - Comparaison Pandas vs Polars

    Ce notebook démontre l'équivalence fonctionnelle et les performances
    des pipelines pandas et Polars pour la génération des périodes d'abonnement.
    """
    )
    return


@app.cell
def load_data():
    """Charger les données depuis DuckDB ou données démo"""

    # Essayer de charger depuis DuckDB avec le loader c15
    print("📄 Tentative de chargement depuis DuckDB...")
    lf = c15().lazy()

    # Enrichir avec le pipeline périmètre pour avoir les colonnes d'impact
    lf = inserer_evenements_facturation(detecter_points_de_rupture(lf))
    df_raw = lf.collect()

    if len(df_raw) > 0:
        # Conversion pour pandas avec mapping colonnes
        df_pandas = df_raw.to_pandas()

        # Mapping snake_case → majuscules pour pandas
        column_mapping = {
            'ref_situation_contractuelle': 'Ref_Situation_Contractuelle',
            'date_evenement': 'Date_Evenement',
            'evenement_declencheur': 'Evenement_Declencheur',
            'formule_tarifaire_acheminement': 'Formule_Tarifaire_Acheminement',
            'puissance_souscrite': 'Puissance_Souscrite',
            'segment_clientele': 'Segment_Clientele',
            'etat_contractuel': 'Etat_Contractuel',
            'type_evenement': 'Type_Evenement',
            'type_compteur': 'Type_Compteur',
            'num_compteur': 'Num_Compteur',
            'ref_demandeur': 'Ref_Demandeur',
            'id_affaire': 'Id_Affaire',
            'categorie': 'Categorie',
            'impacte_abonnement': 'impacte_abonnement',
            'impacte_energie': 'impacte_energie',
            'resume_modification': 'resume_modification',
        }

        # Filtrer le mapping pour les colonnes qui existent
        column_mapping = {k: v for k, v in column_mapping.items() if k in df_pandas.columns}

        df_pandas = df_pandas.rename(columns=column_mapping)
        print(f"✅ {len(df_pandas)} lignes chargées depuis DuckDB")
    return df_pandas, df_raw, lf


@app.cell
def _(df_raw):
    df_raw
    return


@app.cell
def _():
    mo.md(r"""# Génération des Périodes d'Abonnement""")
    return


@app.cell
def _():
    colonnes_interessantes = [
        "Ref_Situation_Contractuelle", "mois_annee", "debut_lisible", "fin_lisible",
        "Formule_Tarifaire_Acheminement", "Puissance_Souscrite", "nb_jours"
    ]
    return (colonnes_interessantes,)


@app.cell(hide_code=True)
def pipeline_pandas_abonnements(colonnes_interessantes, df_pandas):
    """Exécuter le pipeline pandas pour les abonnements"""

    print("🐼 Exécution du pipeline PANDAS abonnements...")

    try:
        # Générer les périodes d'abonnement avec pandas
        periodes_pandas = genererperiodes_pandas(df_pandas)

        print(f"✅ {len(periodes_pandas)} périodes générées")

        # Afficher quelques colonnes clés
        if all(col in periodes_pandas.columns for col in colonnes_interessantes):
            display_periodes = periodes_pandas[colonnes_interessantes].head(5)
            print("\n📋 Exemple de périodes (5 premières):")
            print(display_periodes.to_string(index=False))

    except Exception as e:
        print(f"❌ Erreur pipeline pandas: {e}")
        periodes_pandas = pd.DataFrame()  # DataFrame vide en cas d'erreur
    periodes_pandas
    return (periodes_pandas,)


@app.cell(hide_code=True)
def pipeline_abonnements(colonnes_interessantes, lf):
    """Exécuter le pipeline Polars pour les abonnements"""

    print("⚡ Exécution du pipeline POLARS abonnements...")

    # Générer les périodes d'abonnement avec Polars
    periodes_lf = generer_periodes(lf)
    periodes_collect = periodes_lf.collect()

    print(f"✅ {len(periodes_collect)} périodes générées")

    _snake_cols = [col.lower() for col in colonnes_interessantes]
    print(_snake_cols)
    if all(col in periodes_collect.columns for col in _snake_cols):
        _display_periodes = periodes_collect.select(_snake_cols).head(5)
        print("\n📋 Exemple de périodes (5 premières):")
        print(_display_periodes)
    return (periodes_lf,)


@app.cell
def benchmark_performance(df_pandas, lf):
    """Évaluer les performances des deux approches"""

    print("⏱️ BENCHMARK DES PERFORMANCES :")
    print("=" * 40)

    # Benchmark pandas (périodes seulement)
    start = time.perf_counter()
    iterations = 10
    for _ in range(iterations):
        _ = genererperiodes_pandas(df_pandas)
    temps_pandas = (time.perf_counter() - start) / iterations

    # Benchmark Polars
    start = time.perf_counter()
    for _ in range(iterations):
        _ = generer_periodes(lf).collect()
    temps = (time.perf_counter() - start) / iterations

    # Résultats
    acceleration = temps_pandas / temps if temps > 0 else 0

    print(f"🐼 Pandas  : {temps_pandas*1000:.1f}ms")
    print(f"⚡ Polars  : {temps*1000:.1f}ms")
    print(f"🚀 Accélération : {acceleration:.1f}x")

    if acceleration > 1:
        print(f"✅ Polars est {acceleration:.1f}x plus rapide !")
    elif acceleration < 1:
        print(f"⚠️ Pandas est {1/acceleration:.1f}x plus rapide")
    else:
        print("🟰 Performances équivalentes")

    benchmark_results = {"pandas_ms": temps_pandas*1000, "polars_ms": temps*1000, "speedup": acceleration}
    return


@app.cell
def comparaison_periodes(periodes_pandas, periodes_lf):
    """Comparer les résultats des deux pipelines"""

    # Collecter le résultat Polars
    periodes = periodes_lf.collect()

    print("\n🔍 COMPARAISON DES PÉRIODES GÉNÉRÉES :")
    print("=" * 50)

    # Comparer le nombre de périodes
    nb_pandas = len(periodes_pandas)
    nb = len(periodes)

    print(f"📊 Nombre de périodes :")
    print(f"- Pandas : {nb_pandas}")
    print(f"- Polars : {nb}")

    if nb_pandas == nb:
        print("✅ Même nombre de périodes générées")
    else:
        print("❌ Nombre de périodes différent")

    # Statistiques des périodes Polars
    if nb > 0:
        stats = periodes.select([
            pl.col("nb_jours").sum().alias("total_jours"),
            pl.col("nb_jours").mean().alias("jours_moyen"),
            pl.col("puissance_souscrite").mean().alias("puissance_moyenne"),
        ]).to_dicts()[0]

        print(f"\n📈 Statistiques des périodes (Polars) :")
        print(f"- Total jours    : {stats['total_jours']}")
        print(f"- Durée moyenne  : {stats['jours_moyen']:.1f} jours")
        print(f"- Puissance moy. : {stats['puissance_moyenne']:.1f} kVA")

    # Répartition par FTA
    if nb > 0:
        fta_stats = (
            periodes
            .group_by("formule_tarifaire_acheminement")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )

        print(f"\n🏷️ Répartition par FTA :")
        for row in fta_stats.iter_rows(named=True):
            print(f"- {row['formule_tarifaire_acheminement']}: {row['count']} périodes")

    nb_pandas == nb
    return


@app.cell
def _():
    mo.md(r"""# Calcul TURPE Fixe""")
    return


@app.cell(hide_code=True)
def turpe_pandas(periodes_pandas):
    """Appliquer le calcul TURPE avec pandas"""

    print("🐼 Calcul TURPE avec PANDAS...")


    # Appliquer le TURPE complet avec pandas
    from electricore.core.taxes.turpe import load_turpe_rules, ajouter_turpe_fixe as ajouter_turpe_fixe_pandas

    _regles_turpe = load_turpe_rules()
    periodes_avec_turpe_pandas = ajouter_turpe_fixe_pandas(_regles_turpe, periodes_pandas)

    if "turpe_fixe" in periodes_avec_turpe_pandas.columns:
        total_turpe_pandas = periodes_avec_turpe_pandas["turpe_fixe"].sum()
        turpe_moyen_pandas = periodes_avec_turpe_pandas["turpe_fixe"].mean()

        print(f"✅ TURPE calculé pour {len(periodes_avec_turpe_pandas)} périodes")
        print(f"💰 Total TURPE : {total_turpe_pandas :.2f}€")
        print(f"📊 TURPE moyen : {turpe_moyen_pandas :.2f}€")
    else:
        print("⚠️ Colonne turpe_fixe non trouvée")
        total_turpe_pandas = turpe_moyen_pandas = 0
    return total_turpe_pandas, turpe_moyen_pandas


@app.cell(hide_code=True)
def turpe(periodes_lf):
    """Appliquer le calcul TURPE avec Polars"""

    print("⚡ Calcul TURPE avec POLARS...")

    try:
        # Appliquer le TURPE avec Polars
        from electricore.core.pipelines.turpe import ajouter_turpe_fixe as ajouter_turpe_fixe

        periodes_avec_turpe_lf = ajouter_turpe_fixe(periodes_lf)
        periodes_avec_turpe = periodes_avec_turpe_lf.collect()

        if "turpe_fixe" in periodes_avec_turpe.columns:
            stats_turpe = (
                periodes_avec_turpe
                .select([
                    pl.col("turpe_fixe").sum().alias("total"),
                    pl.col("turpe_fixe").mean().alias("moyen"),
                ])
                .to_dicts()[0]
            )

            total_turpe = stats_turpe["total"]
            turpe_moyen = stats_turpe["moyen"]

            print(f"✅ TURPE calculé pour {len(periodes_avec_turpe)} périodes")
            print(f"💰 Total TURPE : {total_turpe:.2f}€")
            print(f"📊 TURPE moyen : {turpe_moyen:.2f}€")
        else:
            print("⚠️ Colonne turpe_fixe non trouvée")
            total_turpe = turpe_moyen = 0

    except Exception as e:
        print(f"❌ Erreur TURPE Polars: {e}")
        periodes_avec_turpe = periodes_lf.collect()
        total_turpe = turpe_moyen = 0
    return total_turpe, turpe_moyen


@app.cell
def comparaison_turpe(
    total_turpe_pandas,
    total_turpe,
    turpe_moyen_pandas,
    turpe_moyen,
):
    """Comparer les résultats TURPE"""

    print("\n💰 COMPARAISON DES CALCULS TURPE :")
    print("=" * 50)

    print(f"📊 TURPE Total :")
    print(f"- Pandas : {total_turpe_pandas:.2f}€")
    print(f"- Polars : {total_turpe:.2f}€")

    if abs(total_turpe_pandas - total_turpe) < 0.01:
        print("✅ Montants totaux identiques")
    else:
        diff = abs(total_turpe_pandas - total_turpe)
        print(f"❌ Différence : {diff:.2f}€")

    print(f"\n📈 TURPE Moyen :")
    print(f"- Pandas : {turpe_moyen_pandas:.2f}€")
    print(f"- Polars : {turpe_moyen:.2f}€")

    if abs(turpe_moyen_pandas - turpe_moyen) < 0.01:
        print("✅ Montants moyens identiques")
    else:
        diff = abs(turpe_moyen_pandas - turpe_moyen)
        print(f"❌ Différence : {diff:.2f}€")

    turpe_equivalent = abs(total_turpe_pandas - total_turpe) < 0.01
    return


if __name__ == "__main__":
    app.run()
