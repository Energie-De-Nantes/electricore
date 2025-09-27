import marimo

__generated_with = "0.16.0"
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
    from electricore.core.loaders.duckdb_loader import f15, c15, r151, execute_custom_query

    from electricore.core.pipelines import facturation


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Préparation des données""")
    return


@app.cell(hide_code=True)
def load_pipeline_data():
    """Chargement des données pour le pipeline de calcul TURPE variable"""

    mo.md("## 🔧 Calcul TURPE Variable via le pipeline")

    print("🔄 Chargement des données pour le pipeline...")

    # Charger l'historique C15 enrichi
    print("📄 Chargement historique C15...")
    historique_lf = c15().lazy()

    # Charger les relevés R151
    print("📄 Chargement relevés R151...")
    releves_lf = r151().lazy()
    return historique_lf, releves_lf


@app.cell(hide_code=True)
def _():
    mo.md(r"""# Calcul du TURPE Variable""")
    return


@app.cell(hide_code=True)
def calculate_turpe_variable(historique_lf, releves_lf):
    # Pipeline complet
    result = facturation(historique_lf, releves_lf)

    # Accéder aux résultats
    print("Historique enrichi:", result.historique_enrichi.collect().shape)
    print("Abonnements:", result.abonnements.collect().shape)
    print("Énergie:", result.energie.collect().shape)
    print("Facturation:", result.facturation.shape)  # Déjà collecté

    # Ou unpacking
    hist, abo, ener, fact = result
    return (fact,)


@app.cell
def _(fact):
    fact
    return


if __name__ == "__main__":
    app.run()
