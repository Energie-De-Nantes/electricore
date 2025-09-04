import marimo

__generated_with = "0.15.2"
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
    from electricore.core.pipeline_perimetre import detecter_points_de_rupture as detecter_pandas
    from electricore.core.pipelines_polars.perimetre_polars import detecter_points_de_rupture as detecter_polars


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
def load_parquet(demo_data):
    """Charger les données parquet"""

    export_dir = Path.home() / "data" / "export_flux"
    historique_files = list(export_dir.glob("**/historique*.parquet"))

    if historique_files:
        latest_file = max(historique_files, key=lambda x: x.stat().st_mtime)
        print(f"📄 Chargement : {latest_file.name}")
        df_pandas = pd.read_parquet(latest_file)
        lf_polars = pl.scan_parquet(latest_file)
        print(f"✅ {len(df_pandas)} lignes chargées")
    
    else:
        print("❌ Aucun fichier parquet trouvé")
        df_pandas = pd.DataFrame(demo_data)
        lf_polars = pl.LazyFrame(demo_data)
    return df_pandas, lf_polars


@app.cell(hide_code=True)
def pipeline_pandas(df_pandas):
    """Exécuter le pipeline pandas"""

    print("🐼 Exécution du pipeline PANDAS...")

    # Appliquer le pipeline pandas
    resultat_pandas = detecter_pandas(df_pandas)

    # Afficher les colonnes clés
    _colonnes_interessantes = [
        "Date_Evenement", "Evenement_Declencheur", "Puissance_Souscrite",
        "Formule_Tarifaire_Acheminement", "impacte_abonnement", "impacte_energie", 
        "resume_modification"
    ]

    pandas_result = resultat_pandas[_colonnes_interessantes]
    pandas_result
    return (pandas_result,)


@app.cell(hide_code=True)
def pipeline_polars(lf_polars):
    """Exécuter le pipeline Polars"""

    print("⚡ Exécution du pipeline POLARS...")

    # Appliquer le pipeline Polars
    resultat_polars_lf = detecter_polars(lf_polars)
    resultat_polars = resultat_polars_lf.collect()

    # Mêmes colonnes que pandas
    _colonnes_interessantes = [
        "Date_Evenement", "Evenement_Declencheur", "Puissance_Souscrite",
        "Formule_Tarifaire_Acheminement", "impacte_abonnement", "impacte_energie", 
        "resume_modification"
    ]

    polars_result = resultat_polars.select(_colonnes_interessantes)
    polars_result
    return (polars_result,)


@app.cell
def benchmark_performance(df_pandas, lf_polars):
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
        _ = detecter_polars(lf_polars).collect()
    temps_polars = (time.perf_counter() - start) / 10
    
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
    
    {"pandas_ms": temps_pandas*1000, "polars_ms": temps_polars*1000, "speedup": acceleration}


@app.cell
def comparaison(pandas_result, polars_result):
    """Comparer les résultats des deux pipelines"""

    print("\n🔍 COMPARAISON DES RÉSULTATS :")
    print("=" * 50)

    # Vérifier l'équivalence des colonnes clés
    colonnes_comparees = ["impacte_abonnement", "impacte_energie"]

    equivalent = True
    for col in colonnes_comparees:
        pandas_vals = pandas_result[col].tolist()
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


if __name__ == "__main__":
    app.run()
