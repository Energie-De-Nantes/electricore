import marimo

__generated_with = "0.15.2"
app = marimo.App()


@app.cell
def _():
    """
    # 🔍 Exploration des bases DuckDB générées par dlt

    Ce notebook permet d'explorer les bases de données créées automatiquement par dlt
    à partir de vos flux Enedis convertis XML → JSON.
    """
    import marimo as mo
    import duckdb
    import pandas as pd
    from pathlib import Path

    # Interface pour sélectionner la base à analyser
    db_files = list(Path(".").glob("*.duckdb"))
    db_options = {f.name: str(f) for f in db_files}

    if not db_options:
        mo.md("❌ **Aucune base DuckDB trouvée**  \nLancez d'abord `python demo_dlt_test.py`")
    else:
        selected_db = mo.ui.dropdown(
            options=db_options,
            value=list(db_options.keys())[0] if db_options else None,
            label="🗄️ **Sélectionnez une base DuckDB :**"
        )
        mo.vstack([
            mo.md("## 📊 Sélection de la base"),
            selected_db
        ])
    return db_options, duckdb, mo, selected_db


@app.cell
def _(duckdb):
    DATABASE_URL = "/home/virgile/workspace/electricore/electricore/etl/flux_enedis_unified.duckdb"
    engine = duckdb.connect(DATABASE_URL, read_only=True)
    return (engine,)


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM flux_enedis.flux_c15 LIMIT 100
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM flux_enedis.flux_r151
        """,
        engine=engine
    )
    return


@app.cell
def _(db_options, duckdb, mo, selected_db):
    # Connexion à la base sélectionnée
    if not db_options or selected_db.value is None:
        mo.stop(mo.md("Sélectionnez d'abord une base DuckDB"))

    conn = duckdb.connect(selected_db.value)

    # Récupération des tables
    tables_query = "SHOW TABLES"
    tables_df = conn.execute(tables_query).df()

    # Filtrer les tables système dlt
    user_tables = tables_df[~tables_df['name'].str.startswith('_dlt')]

    mo.md(f"""
    ## 📋 Base : `{selected_db.value}`

    **Tables utilisateur : {len(user_tables)}**  
    **Tables système dlt : {len(tables_df) - len(user_tables)}**
    """)
    return conn, user_tables


@app.cell
def _(mo, user_tables):
    # Interface pour sélectionner une table
    if len(user_tables) == 0:
        mo.md("❌ Aucune table utilisateur trouvée")


    table_options = {row['name']: row['name'] for _, row in user_tables.iterrows()}
    table_selector = mo.ui.dropdown(
        options=table_options,
        value=list(table_options.keys())[0],
        label="📊 **Sélectionnez une table à explorer :**"
    )
    mo.vstack([
        mo.md("## 🔍 Exploration des tables"),
        table_selector,
        mo.md("**Tables disponibles :**"),
        mo.ui.table(user_tables, selection=None)
    ])
    return (table_selector,)


@app.cell
def _(conn, mo, table_selector):
    # Analyse de la table sélectionnée
    if table_selector is None or table_selector.value is None:
        mo.stop(mo.md("Sélectionnez une table à explorer"))

    selected_table = table_selector.value

    # Schéma de la table
    schema_query = f"DESCRIBE {selected_table}"
    schema_df = conn.execute(schema_query).df()

    # Nombre d'enregistrements
    count_query = f"SELECT COUNT(*) as total FROM {selected_table}"
    count_result = conn.execute(count_query).fetchone()[0]

    # Échantillon de données
    sample_query = f"SELECT * FROM {selected_table} LIMIT 5"
    sample_df = conn.execute(sample_query).df()

    mo.md(f"""
    ## 📊 Table : `{selected_table}`

    **Nombre d'enregistrements :** {count_result:,}  
    **Nombre de colonnes :** {len(schema_df)}
    """)
    return sample_df, schema_df, selected_table


@app.cell
def _(mo, schema_df):
    # Affichage du schéma
    mo.vstack([
        mo.md("### 🏗️ Schéma de la table"),
        mo.ui.table(schema_df, selection=None)
    ])
    return


@app.cell
def _(mo, sample_df):
    # Affichage de l'échantillon
    mo.vstack([
        mo.md("### 📋 Échantillon de données (5 premières lignes)"),
        mo.ui.table(sample_df, selection=None)
    ])
    return


@app.cell
def _(mo, selected_table):
    # Requêtes d'analyse avancée
    mo.md("### 🔧 Requêtes SQL personnalisées")

    # Exemples de requêtes prédéfinies
    if "prm" in selected_table.lower():
        suggested_queries = {
            "Comptage par segment clientèle": f"""
                SELECT segment_clientele, COUNT(*) as nombre
                FROM {selected_table} 
                GROUP BY segment_clientele 
                ORDER BY nombre DESC
            """,
            "PRM par type de compteur": f"""
                SELECT dispositif_de_comptage__compteur__type as type_compteur, 
                       COUNT(*) as nombre
                FROM {selected_table} 
                GROUP BY dispositif_de_comptage__compteur__type
            """,
            "Distribution des puissances souscrites": f"""
                SELECT situation_contractuelle__structure_tarifaire__puissance_souscrite as puissance,
                       COUNT(*) as nombre
                FROM {selected_table} 
                GROUP BY situation_contractuelle__structure_tarifaire__puissance_souscrite
                ORDER BY CAST(puissance AS INTEGER)
            """
        }
    elif "classe_temporelle" in selected_table.lower():
        suggested_queries = {
            "Valeurs par classe temporelle": f"""
                SELECT id_classe_temporelle, 
                       COUNT(*) as nombre_mesures,
                       AVG(CAST(valeur AS DOUBLE)) as valeur_moyenne
                FROM {selected_table} 
                WHERE valeur IS NOT NULL
                GROUP BY id_classe_temporelle
                ORDER BY id_classe_temporelle
            """,
            "Distribution des valeurs": f"""
                SELECT id_classe_temporelle,
                       MIN(CAST(valeur AS DOUBLE)) as min_val,
                       MAX(CAST(valeur AS DOUBLE)) as max_val,
                       COUNT(*) as count
                FROM {selected_table}
                WHERE valeur IS NOT NULL
                GROUP BY id_classe_temporelle
            """
        }
    else:
        suggested_queries = {
            "Aperçu général": f"SELECT * FROM {selected_table} LIMIT 10",
            "Comptage total": f"SELECT COUNT(*) FROM {selected_table}"
        }

    query_selector = mo.ui.dropdown(
        options=suggested_queries,
        value=list(suggested_queries.values())[0],
        label="💡 **Requêtes suggérées :**"
    )

    custom_query = mo.ui.text_area(
        value=list(suggested_queries.values())[0],
        label="✏️ **Ou tapez votre requête SQL :**",
        rows=4
    )

    mo.vstack([query_selector, custom_query])
    return (custom_query,)


@app.cell
def _(conn, custom_query, mo):
    # Exécution de la requête
    if custom_query.value.strip():
        try:
            result_df = conn.execute(custom_query.value).df()

            mo.vstack([
                mo.md("### 📈 Résultats"),
                mo.md(f"**{len(result_df)} lignes retournées**"),
                mo.ui.table(result_df, selection=None) if len(result_df) <= 100 else mo.md(f"⚠️ Trop de résultats ({len(result_df)} lignes). Limitez votre requête."),
                mo.md("#### 📊 Statistiques rapides") if len(result_df) > 0 and result_df.select_dtypes(include=['number']).shape[1] > 0 else None,
                mo.ui.table(result_df.describe(), selection=None) if len(result_df) > 0 and result_df.select_dtypes(include=['number']).shape[1] > 0 else None
            ])
        except Exception as e:
            mo.md(f"❌ **Erreur SQL :** `{str(e)}`")
    else:
        mo.md("Tapez une requête SQL pour voir les résultats")
    return


@app.cell
def _(mo):
    # Guide d'utilisation
    mo.md("""
    ---
    ## 📚 Guide d'exploration

    ### 🎯 Tables principales détectées par dlt :

    - **`flux_data`** : Table racine contenant les métadonnées des flux
    - **`flux_data__flux_enedis__prm`** : Données des Point de Raccordement au réseau de Mesure (PRM)
    - **`flux_data__...__classe_temporelle_distributeur`** : Données de consommation par plage horaire

    ### 🔗 Relations automatiques :
    - **`_dlt_parent_id`** : Clé de liaison vers la table parent
    - **`_dlt_list_idx`** : Index dans les listes imbriquées
    - **`_dlt_id`** : Identifiant unique de chaque enregistrement

    ### 💡 Conseils d'analyse :
    1. Explorez d'abord la table racine pour comprendre la structure
    2. Utilisez les jointures via `_dlt_parent_id` pour reconstituer les données
    3. Les colonnes avec `__` représentent des champs imbriqués du XML original

    ### 🚀 Prochaines étapes :
    - Testez les requêtes suggérées selon le type de table
    - Explorez les relations entre tables
    - Identifiez les patterns dans vos données Enedis !
    """)
    return


if __name__ == "__main__":
    app.run()
