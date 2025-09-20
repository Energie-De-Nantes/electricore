import marimo

__generated_with = "0.15.3"
app = marimo.App()


@app.cell
def _():
    import duckdb
    DATABASE_URL = "/home/virgile/workspace/electricore/electricore/etl/flux_enedis.duckdb"
    engine = duckdb.connect(DATABASE_URL, read_only=True)
    return (engine,)


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT
            *
        FROM
            enedis_production.flux_r64
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM enedis_production.flux_r151 LIMIT 100
        """,
        engine=engine
    )
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


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
