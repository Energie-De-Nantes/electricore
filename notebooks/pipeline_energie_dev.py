import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    mo.md(
        r"""
        # Pipeline Énergies - Développement et Validation

        Ce notebook permet de développer et valider interactivement le pipeline `pipeline_energie` d'ElectriCore.

        ## Architecture ETL
        1. **Extract** (ElectriFlux): XML → raw DataFrames
        2. **Transform** (ElectriCore/inputs): raw DataFrames → Pandera models  
        3. **Process** (ElectriCore/core): business logic sur données typées
        """
    )
    return (mo,)


@app.cell
def _(mo):
    # Imports pour le pipeline complet
    from pathlib import Path
    import pandas as pd
    import numpy as np

    # ElectriFlux - Extract
    from electriflux.simple_reader import process_flux

    # ElectriCore - Transform
    from electricore.inputs.flux import lire_flux_c15, lire_flux_r151

    # ElectriCore - Process
    from electricore.core.services import pipeline_energie

    # Debugging
    from icecream import ic

    mo.md("✅ **Imports réalisés avec succès**")
    return Path, lire_flux_c15, lire_flux_r151, pipeline_energie, process_flux


@app.cell(hide_code=True)
def _(Path, mo):
    # Configuration des chemins de données
    data_path = Path('~/data/flux_enedis').expanduser()
    c15_path = data_path / 'C15'
    r151_path = data_path / 'R151'

    mo.md(f"""
    ## Configuration des chemins

    - **Répertoire principal**: `{data_path}`
    - **Flux C15**: `{c15_path}` {'✅' if c15_path.exists() else '❌ (non trouvé)'}
    - **Flux R151**: `{r151_path}` {'✅' if r151_path.exists() else '❌ (non trouvé)'}
    """)
    return c15_path, r151_path


@app.cell(hide_code=True)
def _(c15_path, mo, process_flux, r151_path):
    # Étape 1: Extract - Chargement des données brutes avec ElectriFlux
    try:
        raw_c15 = process_flux('C15', c15_path)
        raw_r151 = process_flux('R151', r151_path)

        mo.md(f"""
        ## 📁 **Extract - Données brutes chargées**

        - **C15 (Contrats)**: {len(raw_c15)} lignes, {len(raw_c15.columns)} colonnes
        - **R151 (Relevés)**: {len(raw_r151)} lignes, {len(raw_r151.columns)} colonnes
        """)
    except Exception as e:
        raw_c15 = None
        raw_r151 = None
        mo.md(f"❌ **Erreur lors du chargement**: {str(e)}")

    return raw_c15, raw_r151


@app.cell
def _(mo, raw_c15):
    # Inspection des données C15
    if raw_c15 is not None:
        mo.md("### Aperçu des données C15")
        display_c15 = raw_c15.head()
    else:
        display_c15 = None
        mo.md("❌ Données C15 non disponibles")

    display_c15
    return


@app.cell
def _(mo, raw_r151):
    # Inspection des données R151
    if raw_r151 is not None:
        mo.md("### Aperçu des données R151")
        display_r151 = raw_r151.head()
    else:
        display_r151 = None
        mo.md("❌ Données R151 non disponibles")

    display_r151
    return


@app.cell
def _(lire_flux_c15, lire_flux_r151, mo, raw_c15, raw_r151):
    # Étape 2: Transform - Conversion vers les modèles Pandera
    if raw_c15 is not None and raw_r151 is not None:
        try:
            # Transformation C15 → HistoriquePérimètre
            historique = lire_flux_c15(raw_c15)

            # Transformation R151 → RelevéIndex
            releves = lire_flux_r151(raw_r151)

            mo.md(f"""
            ## 🔄 **Transform - Données typées**

            - **HistoriquePérimètre**: {len(historique)} lignes validées ✅
            - **RelevéIndex**: {len(releves)} lignes validées ✅

            Les données respectent les schémas Pandera.
            """)
            transform_success = True
        except Exception as e:
            historique = None
            releves = None
            transform_success = False
            mo.md(f"❌ **Erreur de transformation**: {str(e)}")
    else:
        historique = None
        releves = None
        transform_success = False
        mo.md("⏭️ Étape Transform ignorée (données brutes manquantes)")

    return historique, releves, transform_success


@app.cell
def _(historique, mo):
    # Inspection de l'historique du périmètre
    if historique is not None:
        mo.md("### Historique du Périmètre (sample)")
        display_historique = historique.head()
    else:
        display_historique = None
        mo.md("❌ Historique non disponible")

    display_historique
    return


@app.cell
def _(mo, releves):
    # Inspection des relevés
    if releves is not None:
        mo.md("### Relevés Index (sample)")
        display_releves = releves.head()
    else:
        display_releves = None
        mo.md("❌ Relevés non disponibles")

    display_releves
    return


@app.cell
def _(historique, mo, pipeline_energie, releves, transform_success):
    # Étape 3: Process - Exécution du pipeline_energie
    if transform_success and historique is not None and releves is not None:
        try:
            periodes_energie = pipeline_energie(historique, releves)

            mo.md(f"""
            ## ⚡ **Process - Pipeline Énergies**

            - **Périodes d'énergie calculées**: {len(periodes_energie)} lignes ✅
            - **Colonnes**: {len(periodes_energie.columns)}

            Le pipeline s'est exécuté avec succès !
            """)
            pipeline_success = True
        except Exception as e:
            periodes_energie = None
            pipeline_success = False
            mo.md(f"❌ **Erreur pipeline_energie**: {str(e)}")
    else:
        periodes_energie = None
        pipeline_success = False
        mo.md("⏭️ Pipeline ignoré (données transformées manquantes)")

    return (periodes_energie,)


@app.cell
def _(mo, periodes_energie):
    # Résultats du pipeline
    if periodes_energie is not None:
        mo.md("### Périodes d'Énergie (résultat final)")
        display_periodes = periodes_energie.head(10)
    else:
        display_periodes = None
        mo.md("❌ Périodes d'énergie non disponibles")

    display_periodes
    return


@app.cell
def _(mo, periodes_energie):
    # Validation et métriques de qualité
    if periodes_energie is not None:
        # Statistiques de base
        total_periodes = len(periodes_energie)
        periodes_completes = periodes_energie['data_complete'].sum() if 'data_complete' in periodes_energie.columns else "N/A"
        periodes_irregulieres = periodes_energie['periode_irreguliere'].sum() if 'periode_irreguliere' in periodes_energie.columns else "N/A"

        # PDLs uniques
        pdls_uniques = periodes_energie['pdl'].nunique() if 'pdl' in periodes_energie.columns else "N/A"

        mo.md(f"""
        ## 📊 **Métriques de Qualité**

        - **Total des périodes**: {total_periodes}
        - **PDL uniques**: {pdls_uniques}
        - **Données complètes**: {periodes_completes}
        - **Périodes irrégulières**: {periodes_irregulieres}
        """)
    else:
        mo.md("⏭️ Métriques non disponibles")
    return


@app.cell
def _(mo, periodes_energie):
    # Colonnes disponibles pour inspection
    if periodes_energie is not None:
        colonnes = list(periodes_energie.columns)
        mo.md(f"""
        ### Colonnes disponibles
        {', '.join(colonnes)}
        """)
    else:
        colonnes = []
        mo.md("❌ Pas de colonnes à afficher")

    return


@app.cell
def _(mo):
    mo.md(
        """
    ## 🔧 **Zone de Debug Interactive**

    Utilisez les cellules ci-dessous pour explorer les données plus en détail.
    """
    )
    return


@app.cell
def _(mo):
    # Interface pour sélectionner un PDL spécifique à inspecter
    pdl_input = mo.ui.text(placeholder="Entrez un PDL à inspecter", label="PDL à analyser")
    pdl_input
    return (pdl_input,)


@app.cell
def _(mo, pdl_input, periodes_energie):
    # Analyse détaillée d'un PDL spécifique
    if periodes_energie is not None and pdl_input.value:
        pdl_data = periodes_energie[periodes_energie['pdl'] == pdl_input.value]
        if len(pdl_data) > 0:
            mo.md(f"### Analyse détaillée du PDL: {pdl_input.value}")
            display_pdl = pdl_data
        else:
            mo.md(f"❌ Aucune donnée trouvée pour le PDL: {pdl_input.value}")
            display_pdl = None
    else:
        display_pdl = None
        mo.md("💡 Entrez un PDL ci-dessus pour voir l'analyse détaillée")

    display_pdl
    return


@app.cell
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
