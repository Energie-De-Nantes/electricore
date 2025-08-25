import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    # Imports pour le pipeline complet
    from pathlib import Path
    import pandas as pd
    import numpy as np

    # ElectriFlux - Extract
    from electriflux.simple_reader import process_flux

    # ElectriCore - Transform
    from electricore.inputs.flux import lire_flux_c15, lire_flux_r151

    # ElectriCore - Process : Pipeline pur + orchestration moderne
    from electricore.core.pipeline_abonnements import pipeline_abonnement
    from electricore.core.pipeline_energie import pipeline_energie
    from electricore.core.orchestration import calculer_energie

    # Debugging
    from icecream import ic

    return Path, calculer_energie, lire_flux_c15, lire_flux_r151, pipeline_energie, process_flux


@app.cell(hide_code=True)
def _(Path, mo):
    # Configuration des chemins de données
    data_path = Path('~/data/flux_enedis').expanduser()
    c15_path = data_path / 'C15'
    r151_path = data_path / 'R151'

    _status_message = mo.md(f"""
    ## Configuration des chemins

    - **Répertoire principal**: `{data_path}`
    - **Flux C15**: `{c15_path}` {'✅' if c15_path.exists() else '❌ (non trouvé)'}
    - **Flux R151**: `{r151_path}` {'✅' if r151_path.exists() else '❌ (non trouvé)'}
    """)

    _status_message
    return c15_path, r151_path


@app.cell(hide_code=True)
def _(c15_path, mo, process_flux, r151_path):
    # Étape 1: Extract - Chargement des données brutes avec ElectriFlux
    raw_c15, raw_r151, _extract_status = None, None, None

    try:
        raw_c15 = process_flux('C15', c15_path)
        raw_r151 = process_flux('R151', r151_path)
        _extract_status = mo.md(f"""
        ## 📁 **Extract - Données brutes chargées**

        - **C15 (Contrats)**: {len(raw_c15)} lignes, {len(raw_c15.columns)} colonnes
        - **R151 (Relevés)**: {len(raw_r151)} lignes, {len(raw_r151.columns)} colonnes
        """)
    except Exception as e:
        _extract_status = mo.md(f"❌ **Erreur lors du chargement**: {str(e)}")

    _extract_status
    return raw_c15, raw_r151


@app.cell(hide_code=True)
def _(lire_flux_c15, lire_flux_r151, mo, raw_c15, raw_r151):
    # Étape 2: Transform - Conversion vers les modèles Pandera
    historique, releves, transform_status, transform_success = None, None, None, False

    if raw_c15 is not None and raw_r151 is not None:
        try:
            # Transformation C15 → HistoriquePérimètre
            historique = lire_flux_c15(raw_c15)

            # Transformation R151 → RelevéIndex
            releves = lire_flux_r151(raw_r151)

            _transform_status = mo.md(f"""
            ## 🔄 **Transform - Données typées**

            - **HistoriquePérimètre**: {len(historique)} lignes validées ✅
            - **RelevéIndex**: {len(releves)} lignes validées ✅

            Les données respectent les schémas Pandera.
            """)
            transform_success = True
        except Exception as e:
            _transform_status = mo.md(f"❌ **Erreur de transformation**: {str(e)}")
    else:
        _transform_status = mo.md("⏭️ Étape Transform ignorée (données brutes manquantes)")

    _transform_status
    return historique, releves, transform_success


@app.cell
def _(historique, mo):
    # Inspection de l'historique du périmètre
    _historique_display = (
        mo.vstack([mo.md("### Historique du Périmètre (sample)"), historique]) 
        if historique is not None 
        else mo.md("❌ Historique non disponible")
    )
    _historique_display
    return


@app.cell
def _(mo, releves):
    # Inspection des relevés
    _releves_display = (
        mo.vstack([mo.md("### Relevés Index (sample)"), releves]) 
        if releves is not None 
        else mo.md("❌ Relevés non disponibles")
    )
    _releves_display
    return


@app.cell(hide_code=True)
def _(historique, mo, pipeline_energie, releves, transform_success):
    # Étape 3: Process - Exécution du pipeline_energie
    periodes_energie, pipeline_status, pipeline_success = None, None, False

    if transform_success and historique is not None and releves is not None:
        try:
            periodes_energie = pipeline_energie(historique, releves)
            _pipeline_status = mo.md(f"""
            ## ⚡ **Process - Pipeline Énergies**

            - **Périodes d'énergie calculées**: {len(periodes_energie)} lignes ✅
            - **Colonnes**: {len(periodes_energie.columns)}

            Le pipeline s'est exécuté avec succès !
            """)
            pipeline_success = True
        except Exception as e:
            _pipeline_status = mo.md(f"❌ **Erreur pipeline_energie**: {str(e)}")
    else:
        _pipeline_status = mo.md("⏭️ Pipeline ignoré (données transformées manquantes)")

    _pipeline_status
    return (periodes_energie,)


@app.cell(hide_code=True)
def _(mo, periodes_energie):
    # Résultats du pipeline
    _periodes_display = (
        mo.vstack([mo.md("### Périodes d'Énergie (résultat final)"), periodes_energie]) 
        if periodes_energie is not None 
        else mo.md("❌ Périodes d'énergie non disponibles")
    )
    _periodes_display
    return


@app.cell(hide_code=True)
def _(mo, periodes_energie):
    # Validation et métriques de qualité
    quality_metrics = None

    if periodes_energie is not None:
        # Statistiques de base
        total_periodes = len(periodes_energie)
        periodes_completes = periodes_energie['data_complete'].sum() if 'data_complete' in periodes_energie.columns else "N/A"
        periodes_irregulieres = periodes_energie['periode_irreguliere'].sum() if 'periode_irreguliere' in periodes_energie.columns else "N/A"

        # PDLs uniques
        pdls_uniques = periodes_energie['pdl'].nunique() if 'pdl' in periodes_energie.columns else "N/A"

        _quality_metrics = mo.md(f"""
        ## 📊 **Métriques de Qualité**

        - **Total des périodes**: {total_periodes}
        - **PDL uniques**: {pdls_uniques}
        - **Données complètes**: {periodes_completes}
        - **Périodes irrégulières**: {periodes_irregulieres}
        """)
    else:
        _quality_metrics = mo.md("⏭️ Métriques non disponibles")

    _quality_metrics
    return


@app.cell(hide_code=True)
def _(mo, periodes_energie):
    # Colonnes disponibles pour inspection
    columns_info = None

    if periodes_energie is not None:
        colonnes = list(periodes_energie.columns)
        _columns_info = mo.md(f"""
        ### Colonnes disponibles
        {', '.join(colonnes)}
        """)
    else:
        _columns_info = mo.md("❌ Pas de colonnes à afficher")

    _columns_info
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
    ## 🔧 **Zone de Debug Interactive**

    Utilisez les cellules ci-dessous pour explorer les données plus en détail.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    # Interface pour sélectionner un PDL spécifique à inspecter
    pdl_input = mo.ui.text(placeholder="Entrez un PDL à inspecter", label="PDL à analyser")
    pdl_input
    return (pdl_input,)


@app.cell(hide_code=True)
def _(mo, pdl_input, periodes_energie):
    # Analyse détaillée d'un PDL spécifique
    pdl_analysis = None

    if periodes_energie is not None and pdl_input.value:
        pdl_data = periodes_energie[periodes_energie['pdl'] == pdl_input.value]
        if len(pdl_data) > 0:
            _pdl_analysis = mo.vstack([
                mo.md(f"### Analyse détaillée du PDL: {pdl_input.value}"),
                pdl_data
            ])
        else:
            _pdl_analysis = mo.md(f"❌ Aucune donnée trouvée pour le PDL: {pdl_input.value}")
    else:
        _pdl_analysis = mo.md("💡 Entrez un PDL ci-dessus pour voir l'analyse détaillée")

    _pdl_analysis
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
    ## 📝 **Notes de Développement**

    - Le notebook charge les données depuis `~/data/flux_enedis/`
    - Pipeline: ElectriFlux (extract) → ElectriCore/inputs (transform) → ElectriCore/core (process)
    - Réactivité Marimo: toute modification en amont met à jour les résultats en aval
    - Utilisez la zone de debug pour inspecter des cas spécifiques

    **Prochaines étapes**: Améliorer la validation, ajouter des visualisations temporelles
    """
    )
    return


if __name__ == "__main__":
    app.run()
