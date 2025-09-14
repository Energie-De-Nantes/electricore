import marimo

__generated_with = "0.15.3"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from pathlib import Path

    # Import des modules ElectriCore

    from electricore.core.loaders.duckdb_loader import (
        load_historique_perimetre,
        load_releves,
    )
    return Path, load_historique_perimetre, load_releves, mo


@app.cell(hide_code=True)
def _(Path, mo):

    db_path = Path("electricore/etl/flux_enedis.duckdb")
    db_exists = db_path.exists()

    mo.md(f"""
    ## 📊 Base DuckDB

    **Chemin** : `{db_path}`
    **Status** : {'✅ Trouvée' if db_exists else '❌ Non trouvée'}
    """)
    return (db_path,)


@app.cell
def _(db_path, load_historique_perimetre):
    load_historique_perimetre(database_path=db_path, valider=True).collect()
    return


@app.cell
def _(db_path, load_releves):
    releves = load_releves(database_path=db_path, valider=True).collect()
    releves
    return


@app.cell
def _():
    from electricore.core.loaders.duckdb_loader import get_available_tables

    # Vérifier la connectivité
    tables = get_available_tables()
    print(f"Tables disponibles: {len(tables)}")
    tables
    return


if __name__ == "__main__":
    app.run()
