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
    from electricore.core.pipeline_energie import (
        reconstituer_chronologie_relevés as reconstituer_pandas,
        calculer_periodes_energie as calculer_energie_pandas
    )

    # Import des pipelines Polars
    from electricore.core.pipelines_polars.energie_polars import (
        pipeline_energie_polars,
        reconstituer_chronologie_releves_polars,
        calculer_periodes_energie_polars
    )
    from electricore.core.pipelines_polars.perimetre_polars import (
        detecter_points_de_rupture,
        inserer_evenements_facturation
    )

    # Import des loaders DuckDB
    from electricore.core.loaders.duckdb_loader import c15, r151


@app.cell
def _():
    mo.md(
        r"""
    # Pipeline Énergie - Comparaison Pandas vs Polars

    Ce notebook démontre l'équivalence fonctionnelle et les performances
    des pipelines pandas et Polars pour le calcul des périodes d'énergie.
    """
    )
    return


@app.cell(hide_code=True)
def load_data():
    """Charger les données depuis DuckDB"""

    print("📄 Chargement des données depuis DuckDB...")

    # Charger l'historique C15
    _lf_historique = c15().lazy()
    lf_historique_enrichi = inserer_evenements_facturation(
        detecter_points_de_rupture(_lf_historique)
    )
    df_historique = lf_historique_enrichi.collect()

    # Charger les relevés R151
    lf_releves = r151().lazy()
    df_releves = lf_releves.collect()

    print(f"✅ Historique C15: {len(df_historique)} événements")
    print(f"✅ Relevés R151: {len(df_releves)} relevés")

    # Conversion pour pandas avec mapping colonnes complet
    _column_mapping = {
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
        'impacte_energie': 'impacte_energie',
        'impacte_abonnement': 'impacte_abonnement',
        'resume_modification': 'resume_modification',
        # Index avant (noms exacts des colonnes)
        'avant_base': 'Avant_BASE',
        'avant_hp': 'Avant_HP',
        'avant_hc': 'Avant_HC',
        'avant_hch': 'Avant_HCH',
        'avant_hph': 'Avant_HPH',
        'avant_hpb': 'Avant_HPB',
        'avant_hcb': 'Avant_HCB',
        'avant_id_calendrier_distributeur': 'Avant_Id_Calendrier_Distributeur',
        # Index après (noms exacts des colonnes avec accent)
        'apres_base': 'Après_BASE',
        'apres_hp': 'Après_HP',
        'apres_hc': 'Après_HC',
        'apres_hch': 'Après_HCH',
        'apres_hph': 'Après_HPH',
        'apres_hpb': 'Après_HPB',
        'apres_hcb': 'Après_HCB',
        'apres_id_calendrier_distributeur': 'Après_Id_Calendrier_Distributeur'
    }

    # Filtrer le mapping pour les colonnes qui existent
    _column_mapping_filtered = {k: v for k, v in _column_mapping.items() if k in df_historique.columns}
    df_historique_pandas = df_historique.to_pandas().rename(columns=_column_mapping_filtered)

    # Conversion relevés pour pandas (noms polars → noms pandas avec accents)
    _releves_mapping = {
        'pdl': 'pdl',  # Garder pdl en minuscule comme attendu par pandas
        'date_releve': 'Date_Releve',  # polars: date_releve → pandas: Date_Releve
        'base': 'BASE',
        'hp': 'HP',
        'hc': 'HC',
        'hch': 'HCH',
        'hph': 'HPH',
        'hpb': 'HPB',
        'hcb': 'HCB',
        'ref_situation_contractuelle': 'Ref_Situation_Contractuelle',
        'formule_tarifaire_acheminement': 'Formule_Tarifaire_Acheminement',
        'id_calendrier_distributeur': 'Id_Calendrier_Distributeur',
        'source': 'Source',
        'unite': 'Unité',  # polars: unite → pandas: Unité (avec accent)
        'precision': 'Précision',  # polars: precision → pandas: Précision (avec accent)
        'ordre_index': 'ordre_index',
        'id_affaire': 'id_affaire',
        'id_calendrier_fournisseur': 'id_calendrier_fournisseur'
    }
    _releves_mapping_filtered = {k: v for k, v in _releves_mapping.items() if k in df_releves.columns}
    df_releves_pandas = df_releves.to_pandas().rename(columns=_releves_mapping_filtered)

    # Filtrer les calendriers invalides pour le pipeline pandas
    if 'Id_Calendrier_Distributeur' in df_releves_pandas.columns:
        _before_filter = len(df_releves_pandas)
        df_releves_pandas = df_releves_pandas[
            df_releves_pandas['Id_Calendrier_Distributeur'].isin(['DI000001', 'DI000002', 'DI000003'])
        ]
        _after_filter = len(df_releves_pandas)
        print(f"🔧 Filtrage calendriers: {_before_filter} → {_after_filter} relevés")
    return (
        df_historique,
        df_historique_pandas,
        df_releves_pandas,
        lf_historique_enrichi,
        lf_releves,
    )


@app.cell
def _(df_historique):
    df_historique.head()
    return


@app.cell
def _():
    mo.md(r"""# Calcul des Périodes d'Énergie""")
    return


@app.cell
def _():
    colonnes_interessantes = [
        "ref_situation_contractuelle", "debut_lisible", "fin_lisible",
        "base_energie", "hp_energie", "hc_energie", "nb_jours"
    ]
    return (colonnes_interessantes,)


@app.cell(hide_code=True)
def pipeline_pandas_energie(
    colonnes_interessantes,
    df_historique_pandas,
    df_releves_pandas,
):
    """Exécuter le pipeline pandas pour l'énergie"""

    print("🐼 Exécution du pipeline PANDAS énergie...")

    try:
        # Vérifier les colonnes disponibles
        print(f"📋 Colonnes historique: {sorted(df_historique_pandas.columns.tolist())}")
        print(f"📋 Colonnes relevés: {sorted(df_releves_pandas.columns.tolist())}")

        # Filtrer les événements qui impactent l'énergie
        if 'impacte_energie' in df_historique_pandas.columns:
            _evt_energie = df_historique_pandas[df_historique_pandas['impacte_energie'] == True]
            print(f"📊 Événements énergie: {len(_evt_energie)}")
        else:
            print("⚠️ Colonne impacte_energie manquante, utilisation de tous les événements")
            _evt_energie = df_historique_pandas

        if len(_evt_energie) == 0:
            print("⚠️ Aucun événement impactant l'énergie trouvé")
            periodes_pandas = pd.DataFrame()
        else:
            # Reconstituer la chronologie des relevés
            _chronologie = reconstituer_pandas(df_releves_pandas, _evt_energie)

            # Calculer les périodes d'énergie
            periodes_pandas = calculer_energie_pandas(_chronologie)

        print(f"✅ {len(periodes_pandas)} périodes générées")

        # Afficher quelques colonnes clés
        if len(periodes_pandas) > 0:
            _display_cols = [col.lower() for col in colonnes_interessantes if col.lower() in periodes_pandas.columns]
            if _display_cols:
                _display_periodes = periodes_pandas[_display_cols].head(5)
                print("\n📋 Exemple de périodes (5 premières):")
                print(_display_periodes.to_string(index=False))

    except Exception as e:
        print(f"❌ Erreur pipeline pandas: {e}")
        periodes_pandas = pd.DataFrame()
    return (periodes_pandas,)


@app.cell(hide_code=True)
def pipeline_polars_energie(
    colonnes_interessantes,
    lf_historique_enrichi,
    lf_releves,
):
    """Exécuter le pipeline Polars pour l'énergie"""

    print("⚡ Exécution du pipeline POLARS énergie...")

    # Calculer les périodes d'énergie avec Polars
    periodes_polars_lf = pipeline_energie_polars(lf_historique_enrichi, lf_releves)
    periodes_polars_collect = periodes_polars_lf.collect()

    print(f"✅ {len(periodes_polars_collect)} périodes générées")

    if len(periodes_polars_collect) > 0:
        _display_cols = [col for col in colonnes_interessantes if col in periodes_polars_collect.columns]
        if _display_cols:
            _display_periodes = periodes_polars_collect.select(_display_cols).head(5)
            print("\n📋 Exemple de périodes (5 premières):")
            print(_display_periodes)
    return periodes_polars_collect, periodes_polars_lf


@app.cell
def _(periodes_polars_collect):
    periodes_polars_collect
    return


@app.cell
def _(periodes_polars_collect):
    periodes_polars_collect.filter(pl.col('pdl') == '14287988313383')
    return


@app.cell
def benchmark_performance(
    df_historique_pandas,
    df_releves_pandas,
    lf_historique_enrichi,
    lf_releves,
):
    """Évaluer les performances des deux approches"""

    print("⏱️ BENCHMARK DES PERFORMANCES :")
    print("=" * 40)

    # Benchmark pandas
    start = time.perf_counter()
    iterations = 5
    for _ in range(iterations):
        _evt_energie = df_historique_pandas[df_historique_pandas['impacte_energie'] == True]
        _chronologie = reconstituer_pandas(df_releves_pandas, _evt_energie)
        _ = calculer_energie_pandas(_chronologie)
    temps_pandas = (time.perf_counter() - start) / iterations

    # Benchmark Polars
    start = time.perf_counter()
    for _ in range(iterations):
        _ = pipeline_energie_polars(lf_historique_enrichi, lf_releves).collect()
    temps_polars = (time.perf_counter() - start) / iterations

    # Résultats
    acceleration = temps_pandas / temps_polars if temps_polars > 0 else 0

    print(f"🐼 Pandas  : {temps_pandas*1000:.1f}ms")
    print(f"⚡ Polars  : {temps_polars*1000:.1f}ms")
    print(f"🚀 Accélération : {acceleration:.1f}x")

    if acceleration > 1:
        print(f"✅ Polars est {acceleration:.1f}x plus rapide !")
    elif acceleration < 1:
        print(f"⚠️ Pandas est {1/acceleration:.1f}x plus rapide")
    else:
        print("🟰 Performances équivalentes")
    return


@app.cell
def comparaison_periodes(periodes_pandas, periodes_polars_lf):
    """Comparer les résultats des deux pipelines"""

    # Collecter le résultat Polars
    periodes_polars = periodes_polars_lf.collect()

    print("\n🔍 COMPARAISON DES PÉRIODES D'ÉNERGIE :")
    print("=" * 50)

    # Comparer le nombre de périodes
    nb_pandas = len(periodes_pandas)
    nb_polars = len(periodes_polars)

    print(f"📊 Nombre de périodes :")
    print(f"- Pandas : {nb_pandas}")
    print(f"- Polars : {nb_polars}")

    if nb_pandas == nb_polars:
        print("✅ Même nombre de périodes générées")
    else:
        print("❌ Nombre de périodes différent")

    # Statistiques des énergies Polars
    if nb_polars > 0:
        _cadrans_energie = [col for col in periodes_polars.columns if col.endswith('_energie')]
        if _cadrans_energie:
            _stats_polars = periodes_polars.select([
                pl.col("nb_jours").sum().alias("total_jours"),
                pl.col("nb_jours").mean().alias("jours_moyen"),
                *[pl.col(cadran).sum().alias(f"total_{cadran}") for cadran in _cadrans_energie if cadran in periodes_polars.columns]
            ]).to_dicts()[0]

            print(f"\n📈 Statistiques des périodes (Polars) :")
            print(f"- Total jours    : {_stats_polars['total_jours']}")
            print(f"- Durée moyenne  : {_stats_polars['jours_moyen']:.1f} jours")

            for cadran in _cadrans_energie:
                if f"total_{cadran}" in _stats_polars:
                    print(f"- Total {cadran}: {_stats_polars[f'total_{cadran}']:.0f} kWh")
    return


@app.cell
def _():
    mo.md(r"""# Calcul TURPE Variable (optionnel)""")
    return


@app.cell(hide_code=True)
def turpe_variable_polars(periodes_polars_lf):
    """Calcul TURPE variable avec Polars (déjà inclus dans le pipeline)"""

    print("⚡ TURPE variable déjà inclus dans le pipeline Polars...")

    periodes_avec_turpe = periodes_polars_lf.collect()

    if "turpe_variable" in periodes_avec_turpe.columns:
        _stats_turpe = (
            periodes_avec_turpe
            .select([
                pl.col("turpe_variable").sum().alias("total"),
                pl.col("turpe_variable").mean().alias("moyen"),
            ])
            .to_dicts()[0]
        )

        total_turpe = _stats_turpe["total"]
        turpe_moyen = _stats_turpe["moyen"]

        print(f"✅ TURPE variable calculé pour {len(periodes_avec_turpe)} périodes")
        print(f"💰 Total TURPE variable : {total_turpe:.2f}€")
        print(f"📊 TURPE variable moyen : {turpe_moyen:.2f}€")
    else:
        print("⚠️ Colonne turpe_variable non trouvée")
        total_turpe = turpe_moyen = 0
    return


if __name__ == "__main__":
    app.run()
