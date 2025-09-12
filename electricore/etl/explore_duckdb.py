import marimo

__generated_with = "0.15.2"
app = marimo.App()


@app.cell
def _():
    import duckdb
    DATABASE_URL = "/home/virgile/workspace/electricore/electricore/etl/enedis_data.duckdb"
    engine = duckdb.connect(DATABASE_URL, read_only=True)
    return (engine,)


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM enedis_full_production.flux_c15 LIMIT 100
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM enedis_full_production.flux_r64 LIMIT 100
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM enedis_multi.flux_r64 LIMIT 100
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM enedis_multi.flux_r64 LIMIT 100
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM enedis_multi.flux_r15_acc LIMIT 100
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM enedis_multi.flux_r15 LIMIT 100
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM enedis_multi._dlt_loads LIMIT 100
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM enedis_multi.r15
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM enedis_multi._dlt_loads LIMIT 100
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM enedis_multi._dlt_pipeline_state LIMIT 100
        """,
        engine=engine
    )
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


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
