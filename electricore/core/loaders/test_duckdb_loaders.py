"""
Notebook Marimo simple pour tester les loaders DuckDB ElectriCore.
"""

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
        execute_custom_query,
        get_available_tables
    )
    from electricore.core.pipeline_perimetre import pipeline_perimetre_polars

    return (
        execute_custom_query,
        get_available_tables,
        load_historique_perimetre,
        mo,
        pipeline_perimetre_polars,
        pl,
    )


@app.cell
def _(mo, Path):
    _db_path = Path("electricore/etl/flux_enedis.duckdb")
    _db_exists = _db_path.exists()
    mo.md(f"""
    ## 📊 Base DuckDB

    **Chemin** : `{_db_path}`
    **Status** : {'✅ Trouvée' if _db_exists else '❌ Non trouvée'}
    """)
    return _db_path, _db_exists


@app.cell
def _(get_available_tables, mo):
    if not _modules_ok or not _db_exists:
        mo.md("⚠️ Configuration requise non disponible")
    else:
        # Lister les tables
        try:
            _tables = get_available_tables()
            _table_list = "\n".join([f"- `{table}`" for table in _tables[:10]])

            mo.md(f"""
            ## 🗂️ Tables disponibles ({len(_tables)} total)

            {_table_list}
            """)
        except Exception as e:
            mo.md(f"❌ Erreur lors de la lecture des tables : {e}")
    return


@app.cell
def _(mo):
    # Widgets de test
    _pdl_filter = mo.ui.text(
        placeholder="Entrez un PDL (optionnel)",
        label="Filtrer par PDL"
    )

    _date_filter = mo.ui.text(
        placeholder="YYYY-MM-DD",
        value="2024-09-01",
        label="Date minimum"
    )

    _limit = mo.ui.slider(
        start=1, stop=100, value=10,
        label="Nombre max de lignes"
    )

    mo.md("## ⚙️ Paramètres de test")
    return


@app.cell
def _(mo):
    mo.hstack([_pdl_filter, _date_filter, _limit])
    return


@app.cell
def _(load_historique_perimetre, mo):
    # Test du loader historique
    if not _modules_ok or not _db_exists:
        _result_historique = None
        mo.md("⚠️ Test impossible")
    else:
        try:
            _filters = {}
            if _date_filter.value:
                _filters["Date_Evenement"] = f">= '{_date_filter.value}'"
            if _pdl_filter.value:
                _filters["pdl"] = f"'{_pdl_filter.value}'"

            _lf_historique = load_historique_perimetre(
                filters=_filters if _filters else None,
                limit=_limit.value,
                valider=False
            )

            _result_historique = _lf_historique.collect()

            mo.md(f"""
            ## 📋 Résultat Historique Périmètre

            **Lignes** : {len(_result_historique)}
            **Colonnes** : {len(_result_historique.columns)}
            **Filtres appliqués** : {_filters if _filters else 'Aucun'}
            """)

        except Exception as e:
            _result_historique = None
            mo.md(f"❌ Erreur : {e}")
    return


@app.cell
def _(mo):
    # Afficher l'échantillon d'historique
    if _result_historique is not None and len(_result_historique) > 0:
        _cols_importantes = ["Date_Evenement", "pdl", "Evenement_Declencheur", "Type_Evenement"]
        _cols_disponibles = [col for col in _cols_importantes if col in _result_historique.columns]

        _echantillon = _result_historique.select(_cols_disponibles).head(5)

        mo.ui.table(_echantillon.to_pandas(), selection=None)
    else:
        mo.md("Aucune donnée d'historique à afficher")
    return


@app.cell
def _(mo, pipeline_perimetre_polars, pl):
    # Test du pipeline Polars moderne
    if _result_historique is not None and len(_result_historique) > 0:
        try:
            _lf_pipeline = pipeline_perimetre_polars(
                source="duckdb",
                filters=_filters if _filters else None,
                limit=min(_limit.value, 20)  # Limiter pour le test
            )

            _result_pipeline = _lf_pipeline.collect()

            # Statistiques sur les impacts
            _nb_impacte_abonnement = _result_pipeline.filter(pl.col("impacte_abonnement")).height
            _nb_impacte_energie = _result_pipeline.filter(pl.col("impacte_energie")).height

            mo.md(f"""
            ## 🔄 Résultat Pipeline Polars

            **Lignes traitées** : {len(_result_pipeline)}
            **Événements impactant abonnement** : {_nb_impacte_abonnement}
            **Événements impactant énergie** : {_nb_impacte_energie}
            """)

        except Exception as e:
            _result_pipeline = None
            mo.md(f"❌ Erreur pipeline : {e}")
    else:
        _result_pipeline = None
        mo.md("⚠️ Pas de données pour tester le pipeline")
    return


@app.cell
def _(mo):
    # Afficher les résultats du pipeline
    if _result_pipeline is not None and len(_result_pipeline) > 0:
        _cols_pipeline = ["Date_Evenement", "pdl", "Evenement_Declencheur", "impacte_abonnement", "impacte_energie", "resume_modification"]
        _cols_pipeline_dispo = [col for col in _cols_pipeline if col in _result_pipeline.columns]

        _echantillon_pipeline = _result_pipeline.select(_cols_pipeline_dispo).head(5)

        mo.ui.table(_echantillon_pipeline.to_pandas(), selection=None)
    else:
        mo.md("Aucune donnée de pipeline à afficher")
    return


@app.cell
def _(mo):
    # Test d'une requête personnalisée
    _requete = mo.ui.text_area(
        value="""SELECT
    pdl,
    COUNT(*) as nb_evenements,
    MIN(date_evenement) as premier_evenement,
    MAX(date_evenement) as dernier_evenement
    FROM enedis_production.flux_c15
    GROUP BY pdl
    ORDER BY nb_evenements DESC
    LIMIT 10""",
        label="Requête SQL personnalisée"
    )

    mo.md("## 🔧 Requête Personnalisée")
    return


@app.cell
def _():
    _requete
    return


@app.cell
def _(execute_custom_query, mo):
    # Exécuter la requête personnalisée
    if not _modules_ok or not _db_exists:
        mo.md("⚠️ Requête impossible")
    else:
        try:
            _result_custom = execute_custom_query(_requete.value, lazy=False)

            mo.md(f"""
            ## 📊 Résultat Requête

            **Lignes** : {len(_result_custom)}
            **Colonnes** : {len(_result_custom.columns)}
            """)

        except Exception as e:
            _result_custom = None
            mo.md(f"❌ Erreur requête : {e}")
    return


@app.cell
def _(mo):
    # Afficher le résultat de la requête personnalisée
    if '_result_custom' in locals() and _result_custom is not None and len(_result_custom) > 0:
        mo.ui.table(_result_custom.to_pandas(), selection=None)
    else:
        mo.md("Aucun résultat de requête à afficher")
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 🎯 Conclusion

    Ce notebook vous permet de tester rapidement les loaders DuckDB et le pipeline Polars moderne d'ElectriCore.

    **Fonctionnalités testées** :
    - ✅ Chargement historique périmètre depuis DuckDB
    - ✅ Pipeline Polars avec expressions composables
    - ✅ Détection des impacts (abonnement/énergie)
    - ✅ Requêtes SQL personnalisées

    **Pour aller plus loin** :
    - Testez d'autres filtres et paramètres
    - Explorez les autres loaders (`load_releves`)
    - Développez vos propres expressions Polars
    """
    )
    return


if __name__ == "__main__":
    app.run()
