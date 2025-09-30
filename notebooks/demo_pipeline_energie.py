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
    from electricore.core.pipelines.energie import (
        pipeline_energie,
        reconstituer_chronologie_releves,
        calculer_periodes_energie
    )
    from electricore.core.pipelines.perimetre import (
        detecter_points_de_rupture,
        inserer_evenements_facturation
    )

    # Import des loaders DuckDB
    from electricore.core.loaders import c15, r151, f15


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
def pipeline_energie(
    colonnes_interessantes,
    lf_historique_enrichi,
    lf_releves,
):
    """Exécuter le pipeline Polars pour l'énergie"""

    print("⚡ Exécution du pipeline POLARS énergie...")

    # Calculer les périodes d'énergie avec Polars
    periodes_lf = pipeline_energie(lf_historique_enrichi, lf_releves)
    periodes_collect = periodes_lf.collect()

    print(f"✅ {len(periodes_collect)} périodes générées")

    if len(periodes_collect) > 0:
        _display_cols = [col for col in colonnes_interessantes if col in periodes_collect.columns]
        if _display_cols:
            _display_periodes = periodes_collect.select(_display_cols).head(5)
            print("\n📋 Exemple de périodes (5 premières):")
            print(_display_periodes)
    return periodes_collect, periodes_lf


@app.cell
def _(periodes_collect):
    periodes_collect
    return


@app.cell
def _(periodes_collect):
    periodes_collect.filter(pl.col('pdl') == '14287988313383')
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
        _ = pipeline_energie(lf_historique_enrichi, lf_releves).collect()
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
    return


@app.cell
def comparaison_periodes(periodes_pandas, periodes_lf):
    """Comparer les résultats des deux pipelines"""

    # Collecter le résultat Polars
    periodes = periodes_lf.collect()

    print("\n🔍 COMPARAISON DES PÉRIODES D'ÉNERGIE :")
    print("=" * 60)

    # Comparer le nombre de périodes
    nb_pandas = len(periodes_pandas)
    nb = len(periodes)

    print(f"📊 Nombre de périodes :")
    print(f"- Pandas : {nb_pandas:,}")
    print(f"- Polars : {nb:,}")

    if nb_pandas == nb:
        print("✅ Même nombre de périodes générées")
    else:
        diff = abs(nb_pandas - nb)
        print(f"⚠️  Différence: {diff:,} périodes")

    # Comparaison détaillée des énergies calculées
    print(f"\n🔋 COMPARAISON DES ÉNERGIES CALCULÉES :")
    print("-" * 50)

    cadrans_energie = ["base_energie", "hp_energie", "hc_energie"]

    for cadran in cadrans_energie:
        if cadran in periodes_pandas.columns and cadran in periodes.columns:
            # Statistiques pandas
            pandas_non_null = periodes_pandas[cadran].dropna()
            polars_non_null = periodes[cadran].drop_nulls().to_pandas()

            if len(pandas_non_null) > 0 and len(polars_non_null) > 0:
                pandas_sum = pandas_non_null.sum()
                polars_sum = polars_non_null.sum()
                pandas_mean = pandas_non_null.mean()
                polars_mean = polars_non_null.mean()
                pandas_count = len(pandas_non_null)
                polars_count = len(polars_non_null)

                print(f"\n📈 {cadran.upper().replace('_', ' ')}:")
                print(f"  🐼 Pandas  : {pandas_count:,} périodes, somme={pandas_sum:,.0f} kWh, moyenne={pandas_mean:.1f} kWh")
                print(f"  ⚡ Polars  : {polars_count:,} périodes, somme={polars_sum:,.0f} kWh, moyenne={polars_mean:.1f} kWh")

                if pandas_sum > 0:
                    _diff_relative = abs(pandas_sum - polars_sum) / pandas_sum * 100
                    if _diff_relative < 0.1:
                        print(f"  ✅ Différence: {_diff_relative:.3f}% (excellente)")
                    elif _diff_relative < 1:
                        print(f"  ✅ Différence: {_diff_relative:.2f}% (très bonne)")
                    elif _diff_relative < 5:
                        print(f"  ⚠️  Différence: {_diff_relative:.2f}% (acceptable)")
                    else:
                        print(f"  ❌ Différence: {_diff_relative:.2f}% (significative)")

    # Comparaison des flags de qualité
    print(f"\n🏷️  COMPARAISON FLAGS QUALITÉ :")
    print("-" * 50)

    flags_qualite = ["data_complete", "periode_irreguliere"]

    for flag in flags_qualite:
        if flag in periodes_pandas.columns and flag in periodes.columns:
            pandas_true = periodes_pandas[flag].sum()
            polars_true = periodes[flag].sum()
            pandas_ratio = pandas_true / len(periodes_pandas) * 100 if len(periodes_pandas) > 0 else 0
            polars_ratio = polars_true / len(periodes) * 100 if len(periodes) > 0 else 0

            print(f"\n🏁 {flag.replace('_', ' ').title()}:")
            print(f"  🐼 Pandas  : {pandas_true:,} périodes ({pandas_ratio:.1f}%)")
            print(f"  ⚡ Polars  : {polars_true:,} périodes ({polars_ratio:.1f}%)")

            if pandas_true == polars_true:
                print(f"  ✅ Identique")
            else:
                diff = abs(pandas_true - polars_true)
                print(f"  ⚠️  Différence: {diff:,} périodes")
    return


@app.cell
def _():
    mo.md(r"""# Calcul TURPE Variable (optionnel)""")
    return


@app.cell(hide_code=True)
def calcul_turpe_pandas(periodes_pandas):
    """Calcul TURPE variable avec pandas - utilise directement calculer_turpe_variable"""
    from electricore.core.taxes.turpe import calculer_turpe_variable, load_turpe_rules

    print("🐼 Calcul TURPE variable avec pandas...")

    if len(periodes_pandas) == 0:
        print("⚠️ Pas de données pandas pour calculer le TURPE")
        turpe_variable_pandas = pd.Series(dtype=float)
    else:
        try:
            # Charger les règles TURPE
            regles_turpe = load_turpe_rules()
            print(f"✅ {len(regles_turpe)} règles TURPE chargées")

            # Préparer les données : convertir snake_case → PascalCase pour compatibilité
            _data = periodes_pandas.copy()

            # Mapping colonnes essentielles
            if 'formule_tarifaire_acheminement' in _data.columns:
                _data = _data.rename(columns={'formule_tarifaire_acheminement': 'Formule_Tarifaire_Acheminement'})

            # Colonnes énergie
            for old, new in [
                ('base_energie', 'BASE_energie'),
                ('hp_energie', 'HP_energie'),
                ('hc_energie', 'HC_energie'),
                ('hph_energie', 'HPH_energie'),
                ('hpb_energie', 'HPB_energie'),
                ('hch_energie', 'HCH_energie'),
                ('hcb_energie', 'HCB_energie')
            ]:
                if old in _data.columns:
                    _data = _data.rename(columns={old: new})

            # Créer colonne debut si manquante
            if 'debut' not in _data.columns and 'debut_lisible' in _data.columns:
                _data['debut'] = pd.to_datetime(_data['debut_lisible']).dt.tz_localize('Europe/Paris')

            # Calculer TURPE variable
            turpe_variable_pandas = calculer_turpe_variable(regles_turpe, _data)

            total = turpe_variable_pandas.sum()
            print(f"✅ TURPE variable calculé: {total:.2f}€ total")

        except Exception as e:
            print(f"❌ Erreur calcul TURPE pandas: {e}")
            turpe_variable_pandas = pd.Series(dtype=float)
    return (turpe_variable_pandas,)


@app.cell(hide_code=True)
def calcul_turpe(periodes_lf):
    """Calcul TURPE variable avec Polars (déjà inclus dans le pipeline)"""

    print("⚡ TURPE variable déjà inclus dans le pipeline Polars...")

    periodes_avec_turpe = periodes_lf.collect()

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

        turpe_variable = periodes_avec_turpe["turpe_variable"].to_pandas()
    else:
        print("⚠️ Colonne turpe_variable non trouvée")
        turpe_variable = pd.Series(dtype=float)
    return (turpe_variable,)


@app.cell
def comparaison_turpe_variable(turpe_variable_pandas, turpe_variable):
    """Comparer les calculs TURPE variable entre pandas et Polars"""

    print("\n💰 COMPARAISON TURPE VARIABLE :")
    print("=" * 60)

    # Statistiques pandas
    if len(turpe_variable_pandas) > 0:
        total_pandas = turpe_variable_pandas.sum()
        moyen_pandas = turpe_variable_pandas.mean()
        median_pandas = turpe_variable_pandas.median()
        count_pandas = len(turpe_variable_pandas)
    else:
        total_pandas = moyen_pandas = median_pandas = count_pandas = 0

    # Statistiques Polars
    if len(turpe_variable) > 0:
        total = turpe_variable.sum()
        moyen = turpe_variable.mean()
        median = turpe_variable.median()
        count = len(turpe_variable)
    else:
        total = moyen = median = count = 0

    print(f"📊 Statistiques TURPE variable :")
    print(f"  🐼 Pandas  : {count_pandas:,} périodes")
    print(f"     Total   : {total_pandas:,.2f}€")
    print(f"     Moyenne : {moyen_pandas:.2f}€")
    print(f"     Médiane : {median_pandas:.2f}€")

    print(f"  ⚡ Polars  : {count:,} périodes")
    print(f"     Total   : {total:,.2f}€")
    print(f"     Moyenne : {moyen:.2f}€")
    print(f"     Médiane : {median:.2f}€")

    # Comparaison
    if total_pandas > 0 and total > 0:
        diff_absolue = abs(total_pandas - total)
        _diff_relative_turpe = diff_absolue / total_pandas * 100

        print(f"\n🔍 Analyse des différences :")
        print(f"  Différence absolue : {diff_absolue:.2f}€")
        print(f"  Différence relative : {_diff_relative_turpe:.3f}%")

        if _diff_relative_turpe < 0.01:
            print(f"  ✅ Excellente concordance (< 0.01%)")
        elif _diff_relative_turpe < 0.1:
            print(f"  ✅ Très bonne concordance (< 0.1%)")
        elif _diff_relative_turpe < 1:
            print(f"  ✅ Bonne concordance (< 1%)")
        elif _diff_relative_turpe < 5:
            print(f"  ⚠️  Différence acceptable (< 5%)")
        else:
            print(f"  ❌ Différence significative (≥ 5%)")
    elif total_pandas == 0 and total == 0:
        print(f"  ✅ Aucun TURPE calculé dans les deux cas")
    else:
        print(f"  ❌ Un seul pipeline a calculé du TURPE")
    return


@app.cell
def _():
    mo.md(r"""# Vérif Turpe""")
    return


@app.cell
def _():
    f15().collect()
    return


if __name__ == "__main__":
    app.run()
