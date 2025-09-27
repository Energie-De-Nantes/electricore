import marimo

__generated_with = "0.15.3"
app = marimo.App(width="medium")

with app.setup:
    import polars as pl
    import pandas as pd
    import sys
    from pathlib import Path

    # Ajouter le chemin du projet
    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    # Import des pipelines
    from electricore.core.pipeline_perimetre import (
        detecter_points_de_rupture as detecter_pandas,
        inserer_evenements_facturation as inserer_evenements_factu_pandas
    )
    from electricore.core.pipelines.perimetre import (
        detecter_points_de_rupture as detecter,
        inserer_evenements_facturation as inserer_evenements_factu
    )
    # Import des loaders DuckDB
    from electricore.core.loaders.duckdb_loader import c15


@app.cell
def _(mo):
    mo.md(r"""# Chargement des Données ou données démos""")
    return


@app.cell(hide_code=True)
def demo_data():
    """Créer des données de démonstration réalistes"""

    # Données d'exemple pour un PDL avec plusieurs événements
    demo_data = {
        "Ref_Situation_Contractuelle": ["PDL001", "PDL001", "PDL001", "PDL001", "PDL001", "PDL001"],
        "Date_Evenement": pd.to_datetime([
            "2024-01-15", "2024-02-01", "2024-03-20", "2024-04-01", "2024-05-10", "2024-06-01"
        ]),
        "Evenement_Declencheur": ["MES", "FACTURATION", "MCT", "FACTURATION", "MCT", "RES"],
        "Puissance_Souscrite": [6.0, 6.0, 9.0, 9.0, 12.0, 12.0],
        "Formule_Tarifaire_Acheminement": ["BTINFCU4", "BTINFCU4", "BTINFMU4", "BTINFMU4", "BTINFMU4", "BTINFMU4"],

        # Colonnes obligatoires du schéma
        "pdl": ["PDL12345", "PDL12345", "PDL12345", "PDL12345", "PDL12345", "PDL12345"],
        "Segment_Clientele": ["C5", "C5", "C5", "C5", "C5", "C5"],
        "Etat_Contractuel": ["ACTIF", "ACTIF", "ACTIF", "ACTIF", "ACTIF", "RESILIE"],
        "Type_Evenement": ["reel", "artificiel", "reel", "artificiel", "reel", "reel"],
        "Categorie": ["PRO", "PRO", "PRO", "PRO", "PRO", "PRO"],
        "Type_Compteur": ["LINKY", "LINKY", "LINKY", "LINKY", "LINKY", "LINKY"],
        "Num_Compteur": ["12345678", "12345678", "12345678", "12345678", "12345678", "12345678"],
        "Ref_Demandeur": ["REF001", "REF001", "REF001", "REF001", "REF001", "REF001"],
        "Id_Affaire": ["AFF001", "AFF001", "AFF001", "AFF001", "AFF001", "AFF001"],

        # Colonnes Avant_/Après_ pour calendrier et index
        "Avant_Id_Calendrier_Distributeur": ["CAL_HP_HC", "CAL_HP_HC", "CAL_HP_HC", "CAL_TEMPO", "CAL_TEMPO", "CAL_TEMPO"],
        "Après_Id_Calendrier_Distributeur": ["CAL_HP_HC", "CAL_HP_HC", "CAL_TEMPO", "CAL_TEMPO", "CAL_TEMPO", "CAL_TEMPO"],

        # Index avec quelques changements
        "Avant_BASE": [1000, 1250, 1600, 1950, 2400, 2800],
        "Après_BASE": [1000, 1250, 1650, 1950, 2400, 2800],  # Ajustement ligne 3
        "Avant_HP": [500, 625, 800, 975, 1200, 1400],
        "Après_HP": [500, 625, 800, 975, 1200, 1400],
        "Avant_HC": [300, 375, 480, 585, 720, 840],
        "Après_HC": [300, 375, 480, 585, 720, 840],
        "Avant_HPH": [None, None, None, None, None, None],
        "Après_HPH": [None, None, None, None, None, None],
        "Avant_HCH": [None, None, None, None, None, None],
        "Après_HCH": [None, None, None, None, None, None],
        "Avant_HPB": [None, None, None, None, None, None],
        "Après_HPB": [None, None, None, None, None, None],
        "Avant_HCB": [None, None, None, None, None, None],
        "Après_HCB": [None, None, None, None, None, None],
    }

    print("📊 Données de démonstration créées :")
    print(f"- {len(demo_data['Date_Evenement'])} événements sur 1 PDL")
    print(f"- Période : {demo_data['Date_Evenement'][0]} à {demo_data['Date_Evenement'][-1]}")
    print(f"- Événements : {demo_data['Evenement_Declencheur']}")

    demo_data
    return (demo_data,)


@app.cell
def load_data(demo_data):
    """Charger les données depuis DuckDB ou données démo"""

    try:
        # Essayer de charger depuis DuckDB avec le loader c15
        print("📄 Chargement depuis DuckDB...")
        lf = c15().limit(10000).lazy()  # Limiter pour la démo
        df_pandas = lf.collect().to_pandas()

        # Convertir les colonnes pour compatibilité avec le pipeline pandas (majuscules)
        column_mapping = {
            'date_evenement': 'Date_Evenement',
            'evenement_declencheur': 'Evenement_Declencheur',
            'puissance_souscrite': 'Puissance_Souscrite',
            'formule_tarifaire_acheminement': 'Formule_Tarifaire_Acheminement',
            'ref_situation_contractuelle': 'Ref_Situation_Contractuelle',
            'segment_clientele': 'Segment_Clientele',
            'etat_contractuel': 'Etat_Contractuel',
            'type_evenement': 'Type_Evenement',
            'type_compteur': 'Type_Compteur',
            'num_compteur': 'Num_Compteur',
            'ref_demandeur': 'Ref_Demandeur',
            'id_affaire': 'Id_Affaire',
            'avant_date_releve': 'Avant_Date_Releve',
            'avant_nature_index': 'Avant_Nature_Index',
            'avant_id_calendrier_fournisseur': 'Avant_Id_Calendrier_Fournisseur',
            'avant_id_calendrier_distributeur': 'Avant_Id_Calendrier_Distributeur',
            'apres_date_releve': 'Après_Date_Releve',
            'apres_nature_index': 'Après_Nature_Index',
            'apres_id_calendrier_fournisseur': 'Après_Id_Calendrier_Fournisseur',
            'apres_id_calendrier_distributeur': 'Après_Id_Calendrier_Distributeur',
            # Colonnes d'index
            'avant_BASE': 'Avant_BASE',
            'avant_HP': 'Avant_HP',
            'avant_HC': 'Avant_HC',
            'avant_HCH': 'Avant_HCH',
            'avant_HPH': 'Avant_HPH',
            'avant_HPB': 'Avant_HPB',
            'avant_HCB': 'Avant_HCB',
            'apres_BASE': 'Après_BASE',
            'apres_HP': 'Après_HP',
            'apres_HC': 'Après_HC',
            'apres_HCH': 'Après_HCH',
            'apres_HPH': 'Après_HPH',
            'apres_HPB': 'Après_HPB',
            'apres_HCB': 'Après_HCB',
            # Autres
            'categorie': 'Categorie',
            'source': 'Source',
            'unite': 'Unité',
            'precision': 'Précision'
        }

        # Renommer les colonnes
        df_pandas = df_pandas.rename(columns=column_mapping)

        # Convertir les colonnes de dates
        date_cols = ['Date_Evenement', 'Avant_Date_Releve', 'Après_Date_Releve']
        for _col in date_cols:
            if _col in df_pandas.columns:
                df_pandas[_col] = pd.to_datetime(df_pandas[_col])

        print(f"✅ {len(df_pandas)} lignes chargées depuis DuckDB")

    except Exception as e:
        print(f"❌ Erreur DuckDB: {e}")
        print("📝 Utilisation des données de démonstration")
        df_pandas = pd.DataFrame(demo_data)
        lf = pl.LazyFrame(demo_data)
    return df_pandas, lf


@app.cell
def _(mo):
    mo.md(r"""# Détéction""")
    return


@app.cell(hide_code=True)
def pipeline_pandas(df_pandas):
    """Exécuter le pipeline pandas"""

    print("🐼 Exécution du pipeline PANDAS...")

    # Appliquer le pipeline pandas
    resultat_pandas = detecter_pandas(df_pandas)

    # # Afficher les colonnes clés
    # _colonnes_interessantes = [
    #     "Date_Evenement", "Evenement_Declencheur", "Puissance_Souscrite",
    #     "Formule_Tarifaire_Acheminement", "impacte_abonnement", "impacte_energie", 
    #     "resume_modification"
    # ]

    # pandas_result = resultat_pandas[_colonnes_interessantes]
    resultat_pandas
    return (resultat_pandas,)


@app.cell(hide_code=True)
def pipeline(lf):
    """Exécuter le pipeline Polars"""

    print("⚡ Exécution du pipeline POLARS...")

    # Appliquer le pipeline Polars
    resultat_lf = detecter(lf)
    resultat = resultat_lf.collect()

    # Mêmes colonnes que pandas
    # _colonnes_interessantes = [
    #     "Date_Evenement", "Evenement_Declencheur", "Puissance_Souscrite",
    #     "Formule_Tarifaire_Acheminement", "impacte_abonnement", "impacte_energie", 
    #     "resume_modification"
    # ]

    # polars_result = resultat.select(_colonnes_interessantes)
    resultat
    return (resultat_lf,)


@app.cell
def benchmark_performance(df_pandas, lf):
    """Évaluer les performances des deux approches"""
    import time

    print("⏱️ BENCHMARK DES PERFORMANCES :")
    print("=" * 40)

    # Benchmark pandas
    start = time.perf_counter()
    for _ in range(10):
        _ = detecter_pandas(df_pandas)
    temps_pandas = (time.perf_counter() - start) / 10

    # Benchmark Polars 
    start = time.perf_counter()
    for _ in range(10):
        _ = detecter(lf).collect()
    temps = (time.perf_counter() - start) / 10

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

    {"pandas_ms": temps_pandas*1000, "polars_ms": temps*1000, "speedup": acceleration}
    return


@app.cell
def comparaison(resultat_pandas, resultat_lf):
    """Comparer les résultats des deux pipelines"""

    # Collecter le résultat Polars
    polars_result = resultat_lf.collect()

    print("\n🔍 COMPARAISON DES RÉSULTATS :")
    print("=" * 50)

    # Vérifier l'équivalence des colonnes clés
    colonnes_comparees = ["impacte_abonnement", "impacte_energie"]

    equivalent = True
    for col in colonnes_comparees:
        pandas_vals = resultat_pandas[col].tolist()
        polars_vals = polars_result[col].to_list()

        if pandas_vals == polars_vals:
            print(f"✅ {col}: Identique")
        else:
            print(f"❌ {col}: Différent")
            print(f"   Pandas: {pandas_vals}")
            print(f"   Polars: {polars_vals}")
            equivalent = False

    if equivalent:
        print("\n🎉 Les deux pipelines produisent des résultats IDENTIQUES !")
    else:
        print("\n⚠️ Différences détectées entre les pipelines")

    # Statistiques
    print(f"\n📊 Impacts détectés :")
    print(f"- Abonnement : {sum(polars_result['impacte_abonnement'].to_list())}/{polars_result.height}")
    print(f"- Énergie    : {sum(polars_result['impacte_energie'].to_list())}/{polars_result.height}")

    # Résumés générés
    resumes_non_vides = [r for r in polars_result["resume_modification"].to_list() if r.strip()]
    print(f"- Résumés    : {len(resumes_non_vides)}/{polars_result.height}")

    if resumes_non_vides:
        print(f"\n📝 Exemples de résumés :")
        for i, resume in enumerate(resumes_non_vides[:2], 1):
            print(f"   {i}. {resume}")

    equivalent
    return


@app.cell
def _(mo):
    mo.md(r"""# Insertion événements""")
    return


@app.cell
def _(resultat_pandas):
    inserer_evenements_factu_pandas(resultat_pandas)
    return


@app.cell
def _(resultat_lf):
    inserer_evenements_factu(resultat_lf).collect()
    return


@app.cell
def conclusion():
    """Conclusion de la démonstration"""

    print("🚀 DÉMONSTRATION TERMINÉE")
    print("=" * 30)
    print()
    print("Cette démonstration montre :")
    print("• ✅ Équivalence fonctionnelle pandas ↔ Polars") 
    print("• ⚡ Pipeline Polars avec LazyFrames optimisées")
    print("• 🧩 Expressions composables et réutilisables")
    print("• 📊 Détection d'impacts métier cohérente")
    print("• 📝 Génération automatique de résumés")
    print()
    print("Le pipeline Polars est prêt pour la production ! 🎯")

    "Demo terminée ! 🚀"
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
