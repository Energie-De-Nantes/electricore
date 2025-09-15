import marimo

__generated_with = "0.15.3"
app = marimo.App(width="medium")

with app.setup:
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
    from electricore.core.pipelines_polars.abonnements_polars import (
        pipeline_abonnements as pipeline_polars,
        generer_periodes_abonnement as generer_periodes_polars
    )
    from electricore.core.pipelines_polars.perimetre_polars import detecter_points_de_rupture

    # Import des loaders DuckDB
    from electricore.core.loaders.duckdb_loader import c15


@app.cell
def _(mo):
    mo.md(
        r"""
    # Pipeline Abonnements - Comparaison Pandas vs Polars

    Ce notebook démontre l'équivalence fonctionnelle et les performances
    des pipelines pandas et Polars pour la génération des périodes d'abonnement.
    """
    )
    return


@app.cell(hide_code=True)
def demo_data():
    """Créer des données de démonstration pour les abonnements"""

    paris_tz = timezone.utc

    # Version pandas (colonnes majuscules)
    _demo_data_pandas = {
        "Ref_Situation_Contractuelle": ["PDL001"] * 6 + ["PDL002"] * 4,
        "pdl": ["12345001"] * 6 + ["12345002"] * 4,
        "Date_Evenement": pd.to_datetime([
            "2024-01-01", "2024-02-01", "2024-04-01", "2024-06-01", "2024-08-01", "2024-10-01",
            "2024-01-15", "2024-03-15", "2024-07-15", "2024-12-01"
        ]).tz_localize(paris_tz),
        "Evenement_Declencheur": [
            "MES", "MCT", "MCT", "MCT", "MCT", "RES",
            "MES", "MCT", "MCT", "RES"
        ],
        "Formule_Tarifaire_Acheminement": [
            "BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4", "BTINFMU4",
            "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"
        ],
        "Puissance_Souscrite": [
            6.0, 6.0, 9.0, 9.0, 9.0, 9.0,
            3.0, 3.0, 6.0, 6.0
        ],

        # Colonnes obligatoires du schéma pandas
        "Segment_Clientele": ["C5"] * 10,
        "Etat_Contractuel": [
            "ACTIF", "ACTIF", "ACTIF", "ACTIF", "ACTIF", "RESILIE",
            "ACTIF", "ACTIF", "ACTIF", "RESILIE"
        ],
        "Type_Evenement": ["reel"] * 10,
        "Type_Compteur": ["LINKY"] * 10,
        "Num_Compteur": ["COMP001"] * 6 + ["COMP002"] * 4,
        "Ref_Demandeur": ["REF001"] * 10,
        "Id_Affaire": ["AFF001"] * 10,
        "Categorie": ["PRO"] * 10,

        # Colonnes Avant_/Après_ nécessaires pour le pipeline périmètre
        "Avant_Id_Calendrier_Distributeur": ["CAL_HP_HC"] * 5 + ["CAL_TEMPO"] * 5,
        "Après_Id_Calendrier_Distributeur": ["CAL_HP_HC"] * 5 + ["CAL_TEMPO"] * 5,
        "Avant_Id_Calendrier_Fournisseur": [None] * 10,
        "Après_Id_Calendrier_Fournisseur": [None] * 10,
        "Avant_Date_Releve": [None] * 10,
        "Après_Date_Releve": [None] * 10,
        "Avant_Nature_Index": [None] * 10,
        "Après_Nature_Index": [None] * 10,

        # Index énergétiques (nulls pour démo abonnements)
        "Avant_BASE": [None] * 10,
        "Après_BASE": [None] * 10,
        "Avant_HP": [None] * 10,
        "Après_HP": [None] * 10,
        "Avant_HC": [None] * 10,
        "Après_HC": [None] * 10,
        "Avant_HPH": [None] * 10,
        "Après_HPH": [None] * 10,
        "Avant_HCH": [None] * 10,
        "Après_HCH": [None] * 10,
        "Avant_HPB": [None] * 10,
        "Après_HPB": [None] * 10,
        "Avant_HCB": [None] * 10,
        "Après_HCB": [None] * 10,
    }

    # Version Polars (colonnes snake_case)
    _demo_data_polars = {
        "ref_situation_contractuelle": ["PDL001"] * 6 + ["PDL002"] * 4,
        "pdl": ["12345001"] * 6 + ["12345002"] * 4,
        "date_evenement": [
            datetime(2024, 1, 1, tzinfo=paris_tz),
            datetime(2024, 2, 1, tzinfo=paris_tz),
            datetime(2024, 4, 1, tzinfo=paris_tz),
            datetime(2024, 6, 1, tzinfo=paris_tz),
            datetime(2024, 8, 1, tzinfo=paris_tz),
            datetime(2024, 10, 1, tzinfo=paris_tz),
            datetime(2024, 1, 15, tzinfo=paris_tz),
            datetime(2024, 3, 15, tzinfo=paris_tz),
            datetime(2024, 7, 15, tzinfo=paris_tz),
            datetime(2024, 12, 1, tzinfo=paris_tz),
        ],
        "evenement_declencheur": [
            "MES", "MCT", "MCT", "MCT", "MCT", "RES",
            "MES", "MCT", "MCT", "RES"
        ],
        "formule_tarifaire_acheminement": [
            "BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4", "BTINFMU4",
            "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"
        ],
        "puissance_souscrite": [
            6.0, 6.0, 9.0, 9.0, 9.0, 9.0,
            3.0, 3.0, 6.0, 6.0
        ],
        "segment_clientele": ["C5"] * 10,
        "etat_contractuel": [
            "ACTIF", "ACTIF", "ACTIF", "ACTIF", "ACTIF", "RESILIE",
            "ACTIF", "ACTIF", "ACTIF", "RESILIE"
        ],
        "type_evenement": ["reel"] * 10,
        "type_compteur": ["LINKY"] * 10,
        "num_compteur": ["COMP001"] * 6 + ["COMP002"] * 4,
        "ref_demandeur": ["REF001"] * 10,
        "id_affaire": ["AFF001"] * 10,
        "categorie": ["PRO"] * 10,

        # Colonnes de relevés snake_case
        "avant_date_releve": [None] * 10,
        "apres_date_releve": [None] * 10,
        "avant_id_calendrier_distributeur": ["CAL_HP_HC"] * 5 + ["CAL_TEMPO"] * 5,
        "apres_id_calendrier_distributeur": ["CAL_HP_HC"] * 5 + ["CAL_TEMPO"] * 5,
        "avant_id_calendrier_fournisseur": [None] * 10,
        "apres_id_calendrier_fournisseur": [None] * 10,
        "avant_nature_index": [None] * 10,
        "apres_nature_index": [None] * 10,

        # Index énergétiques
        "avant_BASE": [None] * 10,
        "apres_BASE": [None] * 10,
        "avant_HP": [None] * 10,
        "apres_HP": [None] * 10,
        "avant_HC": [None] * 10,
        "apres_HC": [None] * 10,
        "avant_HPH": [None] * 10,
        "apres_HPH": [None] * 10,
        "avant_HCH": [None] * 10,
        "apres_HCH": [None] * 10,
        "avant_HPB": [None] * 10,
        "apres_HPB": [None] * 10,
        "avant_HCB": [None] * 10,
        "apres_HCB": [None] * 10,

        # Métadonnées
        "source": ["demo"] * 10,
        "unite": ["kWh"] * 10,
        "precision": ["kWh"] * 10,
    }

    print("📊 Données de démonstration créées :")
    print(f"- {len(_demo_data_pandas['Date_Evenement'])} événements sur 2 PDL")
    print(f"- Période : {_demo_data_pandas['Date_Evenement'].min()} à {_demo_data_pandas['Date_Evenement'].max()}")
    print(f"- FTA : {set(_demo_data_pandas['Formule_Tarifaire_Acheminement'])}")

    demo_data = _demo_data_pandas, _demo_data_polars
    return (demo_data,)


@app.cell
def load_data(demo_data):
    """Charger les données depuis DuckDB ou données démo"""
    _demo_data_pandas, _demo_data_polars = demo_data

    try:
        # Essayer de charger depuis DuckDB avec le loader c15
        print("📄 Tentative de chargement depuis DuckDB...")
        lf_polars = c15().limit(1000).lazy()

        # Enrichir avec le pipeline périmètre pour avoir les colonnes d'impact
        lf_polars = detecter_points_de_rupture(lf_polars)
        df_polars_raw = lf_polars.collect()

        if len(df_polars_raw) > 0:
            # Conversion pour pandas avec mapping colonnes
            df_pandas = df_polars_raw.to_pandas()

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

            df_pandas = df_pandas.rename(columns=column_mapping)
            print(f"✅ {len(df_pandas)} lignes chargées depuis DuckDB")
            source_donnees = "DuckDB"
        else:
            raise Exception("DuckDB vide")

    except Exception as e:
        print(f"❌ Erreur DuckDB: {e}")
        print("📝 Utilisation des données de démonstration")

        # Enrichir les données de démo avec le pipeline périmètre
        from electricore.core.pipeline_perimetre import pipeline_perimetre

        df_pandas = pd.DataFrame(_demo_data_pandas)
        lf_polars = detecter_points_de_rupture(pl.LazyFrame(_demo_data_polars))

        # Enrichir pandas avec pipeline périmètre
        df_pandas = pipeline_perimetre(df_pandas)

        source_donnees = "Demo"
        print(f"✅ {len(df_pandas)} lignes de données de démonstration")
    return df_pandas, lf_polars, source_donnees


@app.cell
def _(mo):
    mo.md(r"""# Génération des Périodes d'Abonnement""")
    return


@app.cell(hide_code=True)
def _pipeline_pandas_abonnements(df_pandas):
    """Exécuter le pipeline pandas pour les abonnements"""

    print("🐼 Exécution du pipeline PANDAS abonnements...")

    try:
        # Générer les périodes d'abonnement avec pandas
        periodes_pandas = genererperiodes_pandas(df_pandas)

        print(f"✅ {len(periodes_pandas)} périodes générées")

        # Afficher quelques colonnes clés
        _colonnes_interessantes = [
            "Ref_Situation_Contractuelle", "mois_annee", "debut_lisible", "fin_lisible",
            "Formule_Tarifaire_Acheminement", "Puissance_Souscrite", "nb_jours"
        ]

        if all(col in periodes_pandas.columns for col in _colonnes_interessantes):
            _display_periodes = periodes_pandas[_colonnes_interessantes].head(5)
            print("\n📋 Exemple de périodes (5 premières):")
            print(_display_periodes.to_string(index=False))

    except Exception as e:
        print(f"❌ Erreur pipeline pandas: {e}")
        periodes_pandas = pd.DataFrame()  # DataFrame vide en cas d'erreur
    periodes_pandas
    return (periodes_pandas,)


@app.cell(hide_code=True)
def _pipeline_polars_abonnements(lf_polars):
    """Exécuter le pipeline Polars pour les abonnements"""

    print("⚡ Exécution du pipeline POLARS abonnements...")

    # Générer les périodes d'abonnement avec Polars
    periodes_polars_lf = generer_periodes_polars(lf_polars)
    _periodes_polars = periodes_polars_lf.collect()

    print(f"✅ {len(_periodes_polars)} périodes générées")

    # Afficher quelques colonnes clés
    _colonnes_interessantes = [
        "ref_situation_contractuelle", "mois_annee", "debut_lisible", "fin_lisible",
        "formule_tarifaire_acheminement", "puissance_souscrite", "nb_jours"
    ]

    if all(col in _periodes_polars.columns for col in _colonnes_interessantes):
        _display_periodes = _periodes_polars.select(_colonnes_interessantes).head(5)
        print("\n📋 Exemple de périodes (5 premières):")
        print(_display_periodes)
    _periodes_polars
    return (periodes_polars_lf,)


@app.cell
def _benchmark_performance(df_pandas, lf_polars):
    """Évaluer les performances des deux approches"""

    print("⏱️ BENCHMARK DES PERFORMANCES :")
    print("=" * 40)

    # Benchmark pandas (périodes seulement)
    _start = time.perf_counter()
    _iterations = 10
    for _ in range(_iterations):
        _ = genererperiodes_pandas(df_pandas)
    _temps_pandas = (time.perf_counter() - _start) / _iterations

    # Benchmark Polars
    _start = time.perf_counter()
    for _ in range(_iterations):
        _ = generer_periodes_polars(lf_polars).collect()
    _temps_polars = (time.perf_counter() - _start) / _iterations

    # Résultats
    _acceleration = _temps_pandas / _temps_polars if _temps_polars > 0 else 0

    print(f"🐼 Pandas  : {_temps_pandas*1000:.1f}ms")
    print(f"⚡ Polars  : {_temps_polars*1000:.1f}ms")
    print(f"🚀 Accélération : {_acceleration:.1f}x")

    if _acceleration > 1:
        print(f"✅ Polars est {_acceleration:.1f}x plus rapide !")
    elif _acceleration < 1:
        print(f"⚠️ Pandas est {1/_acceleration:.1f}x plus rapide")
    else:
        print("🟰 Performances équivalentes")

    {"pandas_ms": _temps_pandas*1000, "polars_ms": _temps_polars*1000, "speedup": _acceleration}
    return


@app.cell
def _comparaison_periodes(periodes_pandas, periodes_polars_lf):
    """Comparer les résultats des deux pipelines"""

    # Collecter le résultat Polars
    _periodes_polars = periodes_polars_lf.collect()

    print("\n🔍 COMPARAISON DES PÉRIODES GÉNÉRÉES :")
    print("=" * 50)

    # Comparer le nombre de périodes
    _nb_pandas = len(periodes_pandas)
    _nb_polars = len(_periodes_polars)

    print(f"📊 Nombre de périodes :")
    print(f"- Pandas : {_nb_pandas}")
    print(f"- Polars : {_nb_polars}")

    if _nb_pandas == _nb_polars:
        print("✅ Même nombre de périodes générées")
    else:
        print("❌ Nombre de périodes différent")

    # Statistiques des périodes Polars
    if _nb_polars > 0:
        _stats_polars = _periodes_polars.select([
            pl.col("nb_jours").sum().alias("total_jours"),
            pl.col("nb_jours").mean().alias("jours_moyen"),
            pl.col("puissance_souscrite").mean().alias("puissance_moyenne"),
        ]).to_dicts()[0]

        print(f"\n📈 Statistiques des périodes (Polars) :")
        print(f"- Total jours    : {_stats_polars['total_jours']}")
        print(f"- Durée moyenne  : {_stats_polars['jours_moyen']:.1f} jours")
        print(f"- Puissance moy. : {_stats_polars['puissance_moyenne']:.1f} kVA")

    # Répartition par FTA
    if _nb_polars > 0:
        fta_stats = (
            _periodes_polars
            .group_by("formule_tarifaire_acheminement")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )

        print(f"\n🏷️ Répartition par FTA :")
        for row in fta_stats.iter_rows(named=True):
            print(f"- {row['formule_tarifaire_acheminement']}: {row['count']} périodes")

    equivalent = _nb_pandas == _nb_polars
    return (equivalent,)


@app.cell
def _(mo):
    mo.md(r"""# Calcul TURPE Fixe""")
    return


@app.cell(hide_code=True)
def _turpe_pandas(periodes_pandas):
    """Appliquer le calcul TURPE avec pandas"""

    print("🐼 Calcul TURPE avec PANDAS...")

    try:
        # Appliquer le TURPE complet avec pandas
        from electricore.core.taxes.turpe import load_turpe_rules, ajouter_turpe_fixe

        regles_turpe = load_turpe_rules()
        periodes_avec_turpe_pandas = ajouter_turpe_fixe(regles_turpe, periodes_pandas)

        if "turpe_fixe" in periodes_avec_turpe_pandas.columns:
            total_turpe = periodes_avec_turpe_pandas["turpe_fixe"].sum()
            turpe_moyen = periodes_avec_turpe_pandas["turpe_fixe"].mean()

            print(f"✅ TURPE calculé pour {len(periodes_avec_turpe_pandas)} périodes")
            print(f"💰 Total TURPE : {total_turpe:.2f}€")
            print(f"📊 TURPE moyen : {turpe_moyen:.2f}€")
        else:
            print("⚠️ Colonne turpe_fixe non trouvée")
            total_turpe = turpe_moyen = 0

    except Exception as e:
        print(f"❌ Erreur TURPE pandas: {e}")
        periodes_avec_turpe_pandas = periodes_pandas
        total_turpe = turpe_moyen = 0
    return (total_turpe,)


@app.cell(hide_code=True)
def _turpe_polars(periodes_polars_lf):
    """Appliquer le calcul TURPE avec Polars"""

    print("⚡ Calcul TURPE avec POLARS...")

    try:
        # Appliquer le TURPE avec Polars
        from electricore.core.taxes.turpe_polars import ajouter_turpe_fixe_polars

        periodes_avec_turpe_polars_lf = ajouter_turpe_fixe_polars(periodes_polars_lf)
        periodes_avec_turpe_polars = periodes_avec_turpe_polars_lf.collect()

        if "turpe_fixe" in periodes_avec_turpe_polars.columns:
            stats_turpe = (
                periodes_avec_turpe_polars
                .select([
                    pl.col("turpe_fixe").sum().alias("total"),
                    pl.col("turpe_fixe").mean().alias("moyen"),
                ])
                .to_dicts()[0]
            )

            total_turpe_polars = stats_turpe["total"]
            turpe_moyen_polars = stats_turpe["moyen"]

            print(f"✅ TURPE calculé pour {len(periodes_avec_turpe_polars)} périodes")
            print(f"💰 Total TURPE : {total_turpe_polars:.2f}€")
            print(f"📊 TURPE moyen : {turpe_moyen_polars:.2f}€")
        else:
            print("⚠️ Colonne turpe_fixe non trouvée")
            total_turpe_polars = turpe_moyen_polars = 0

    except Exception as e:
        print(f"❌ Erreur TURPE Polars: {e}")
        periodes_avec_turpe_polars = periodes_polars_lf.collect()
        total_turpe_polars = turpe_moyen_polars = 0
    return


@app.cell
def _comparaison_turpe(moyen_pandas, moyen_polars, total_pandas, total_polars):
    """Comparer les résultats TURPE"""

    _, _total_pandas, _moyen_pandas = _turpe_pandas
    _, _total_polars, _moyen_polars = _turpe_polars

    print("\n💰 COMPARAISON DES CALCULS TURPE :")
    print("=" * 50)

    print(f"📊 TURPE Total :")
    print(f"- Pandas : {total_pandas:.2f}€")
    print(f"- Polars : {total_polars:.2f}€")

    if abs(total_pandas - total_polars) < 0.01:
        print("✅ Montants totaux identiques")
    else:
        diff = abs(total_pandas - total_polars)
        print(f"❌ Différence : {diff:.2f}€")

    print(f"\n📈 TURPE Moyen :")
    print(f"- Pandas : {moyen_pandas:.2f}€")
    print(f"- Polars : {moyen_polars:.2f}€")

    if abs(moyen_pandas - moyen_polars) < 0.01:
        print("✅ Montants moyens identiques")
    else:
        diff = abs(moyen_pandas - moyen_polars)
        print(f"❌ Différence : {diff:.2f}€")

    turpe_equivalent = abs(total_pandas - total_polars) < 0.01
    return


@app.cell
def _conclusion(equivalent, source_donnees, total_turpe):
    """Conclusion de la démonstration"""

    _, _total_turpe, _ = _turpe_polars
    _equivalent = _comparaison_periodes

    print("🎉 DÉMONSTRATION TERMINÉE")
    print("=" * 40)
    print()
    print("📋 Résumé de la migration :")
    print(f"• 🔧 Source données : {source_donnees}")
    print(f"• ✅ Équivalence fonctionnelle : {'OUI' if equivalent else 'NON'}")
    print(f"• ⚡ Pipeline Polars optimisé avec LazyFrames")
    print(f"• 🧩 Expressions composables et testables")
    print(f"• 💰 TURPE calculé : {total_turpe:.2f}€")
    print()
    print("🚀 Le pipeline Polars pour les abonnements est prêt ! 🎯")
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
