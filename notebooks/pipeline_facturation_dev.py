import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell
def import_marimo():
    import marimo as mo
    return (mo,)


@app.cell
def import_dependencies():
    # Imports pour le pipeline de facturation complet
    from pathlib import Path
    import pandas as pd
    import numpy as np
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    # ElectriFlux - Extract
    from electriflux.simple_reader import process_flux

    # ElectriCore - Transform
    from electricore.inputs.flux import lire_flux_c15, lire_flux_r151

    # ElectriCore - Process : Orchestration de facturation (nouvelle architecture)
    from electricore.core.orchestration import facturation

    # Debugging
    from icecream import ic
    import traceback

    return Path, facturation, lire_flux_c15, lire_flux_r151, process_flux, px


@app.cell(hide_code=True)
def configure_data_paths(Path, mo):
    # Configuration des chemins de données
    data_path = Path('~/data/flux_enedis').expanduser()
    c15_path = data_path / 'C15'
    r151_path = data_path / 'R151'

    _status_message = mo.md(f"""
    ## Configuration des chemins de données

    - **Répertoire principal**: `{data_path}`
    - **Flux C15 (contrats)**: `{c15_path}` {'✅' if c15_path.exists() else '❌ (non trouvé)'}
    - **Flux R151 (relevés)**: `{r151_path}` {'✅' if r151_path.exists() else '❌ (non trouvé)'}

    Ce notebook teste le nouveau pipeline de **méta-périodes mensuelles** avec les vraies données.
    """)

    _status_message
    return c15_path, r151_path


@app.cell(hide_code=True)
def extract_raw_data(c15_path, mo, process_flux, r151_path):
    # Étape 1: Extract - Chargement des données brutes avec ElectriFlux
    raw_c15, raw_r151, _extract_status = None, None, None

    try:
        raw_c15 = process_flux('C15', c15_path)
        raw_r151 = process_flux('R151', r151_path)
        _extract_status = mo.md(f"""
        ## 📁 **Extract - Données brutes chargées**

        - **C15 (Contrats)**: {len(raw_c15):,} lignes, {len(raw_c15.columns)} colonnes
        - **R151 (Relevés)**: {len(raw_r151):,} lignes, {len(raw_r151.columns)} colonnes
        """)
    except Exception as e:
        _extract_status = mo.md(f"❌ **Erreur lors du chargement**: {str(e)}")

    _extract_status
    return raw_c15, raw_r151


@app.cell(hide_code=True)
def transform_data_models(
    lire_flux_c15,
    lire_flux_r151,
    mo,
    raw_c15,
    raw_r151,
):
    # Étape 2: Transform - Conversion vers les modèles Pandera
    historique, releves, _transform_status, transform_success = None, None, None, False

    if raw_c15 is not None and raw_r151 is not None:
        try:
            # Transformation C15 → HistoriquePérimètre
            historique = lire_flux_c15(raw_c15)

            # Transformation R151 → RelevéIndex
            releves = lire_flux_r151(raw_r151)

            _transform_status = mo.md(f"""
            ## 🔄 **Transform - Données typées et validées**

            - **HistoriquePérimètre**: {len(historique):,} lignes validées ✅
            - **RelevéIndex**: {len(releves):,} lignes validées ✅

            Les données respectent les schémas Pandera.
            """)
            transform_success = True
        except Exception as e:
            _transform_status = mo.md(f"❌ **Erreur de transformation**: {str(e)}")
    else:
        _transform_status = mo.md("⏭️ Étape Transform ignorée (données brutes manquantes)")

    _transform_status
    return historique, releves


@app.cell(hide_code=True)
def pipeline_facturation_primary(facturation, historique, releves):
    # Utilisation de l'orchestration complète qui retourne un ResultatFacturation
    resultat_facturation = facturation(historique, releves)
    meta_periodes_primary = resultat_facturation.facturation
    return meta_periodes_primary, resultat_facturation


@app.cell(hide_code=True)
def display_meta_periodes_results(meta_periodes_primary, mo):
    # Affichage des résultats de facturation
    _meta_display = (
        mo.vstack([
            mo.md("### Méta-périodes de Facturation (échantillon)"), 
            meta_periodes_primary
        ]) 
        if meta_periodes_primary is not None 
        else mo.md("❌ Méta-périodes non disponibles")
    )
    _meta_display
    return


@app.cell
def _(resultat_facturation):
    resultat_facturation.energie
    return


@app.cell
def _(resultat_facturation):
    resultat_facturation.abonnements
    return


@app.cell(hide_code=True)
def global_metrics(meta_periodes_primary, mo):
    # Métriques globales
    if meta_periodes_primary is not None:
        # Statistiques de base
        total_meta = len(meta_periodes_primary)
        pdls_uniques = meta_periodes_primary['pdl'].nunique()
        mois_uniques = meta_periodes_primary['mois_annee'].nunique()
        fta_uniques = meta_periodes_primary['Formule_Tarifaire_Acheminement'].nunique()

        # Cas avec changements intra-mois
        avec_changements = meta_periodes_primary['has_changement'].sum()

        _metrics_globales = mo.md(f"""
        ## 📊 **Métriques Globales**

        - **Total méta-périodes**: {total_meta:,}
        - **PDL uniques**: {pdls_uniques:,}
        - **Mois couverts**: {mois_uniques}
        - **Formules tarifaires (FTA)**: {fta_uniques}
        - **Périodes avec changements intra-mois**: {avec_changements:,} ({avec_changements/total_meta*100:.1f}%)
        """)
    else:
        _metrics_globales = mo.md("⏭️ Métriques globales non disponibles")

    _metrics_globales
    return


@app.cell(hide_code=True)
def analyze_aggregations(meta_periodes_primary, mo):
    # Analyse des agrégations
    if meta_periodes_primary is not None:
        # Distribution des sous-périodes
        avg_sous_periodes_abo = meta_periodes_primary['nb_sous_periodes_abo'].mean()
        avg_sous_periodes_energie = meta_periodes_primary['nb_sous_periodes_energie'].mean()
        max_sous_periodes_abo = meta_periodes_primary['nb_sous_periodes_abo'].max()
        max_sous_periodes_energie = meta_periodes_primary['nb_sous_periodes_energie'].max()

        # Puissance moyenne
        puissance_min = meta_periodes_primary['puissance_moyenne'].min()
        puissance_max = meta_periodes_primary['puissance_moyenne'].max()
        puissance_avg = meta_periodes_primary['puissance_moyenne'].mean()

        _analyse_agregation = mo.md(f"""
        ## 🔧 **Analyse des Agrégations**

        ### Sous-périodes agrégées
        - **Abonnements** - Moyenne: {avg_sous_periodes_abo:.2f}, Maximum: {max_sous_periodes_abo}
        - **Énergies** - Moyenne: {avg_sous_periodes_energie:.2f}, Maximum: {max_sous_periodes_energie}

        ### Puissances moyennes pondérées
        - **Min**: {puissance_min:.2f} kVA
        - **Moyenne**: {puissance_avg:.2f} kVA  
        - **Max**: {puissance_max:.2f} kVA
        """)
    else:
        _analyse_agregation = mo.md("⏭️ Analyse des agrégations non disponible")

    _analyse_agregation
    return


@app.cell(hide_code=True)
def financial_analysis_turpe(meta_periodes_primary, mo):
    def _():
        # Analyse financière TURPE
        if meta_periodes_primary is not None and 'turpe_fixe' in meta_periodes_primary.columns and 'turpe_variable' in meta_periodes_primary.columns:
            # Montants TURPE
            turpe_fixe_total = meta_periodes_primary['turpe_fixe'].sum()
            turpe_variable_total = meta_periodes_primary['turpe_variable'].sum()
            turpe_total = turpe_fixe_total + turpe_variable_total
            ratio_fixe_variable = turpe_fixe_total / turpe_variable_total if turpe_variable_total > 0 else float('inf')

            # Top 10 PDL par montant TURPE
            meta_periodes_avec_total = meta_periodes_primary.assign(
                turpe_total=meta_periodes_primary['turpe_fixe'] + meta_periodes_primary['turpe_variable']
            )
            top_pdl_turpe = (
                meta_periodes_avec_total
                .groupby('pdl')['turpe_total']
                .sum()
                .sort_values(ascending=False)
                .head(10)
            )

            _analyse_financiere = mo.vstack([
                mo.md(f"""
                ## 💰 **Analyse Financière TURPE**

                ### Montants totaux
                - **TURPE Fixe**: {turpe_fixe_total:,.2f} €
                - **TURPE Variable**: {turpe_variable_total:,.2f} €
                - **TURPE Total**: {turpe_total:,.2f} €
                - **Ratio Fixe/Variable**: {ratio_fixe_variable:.2f}

                ### Top 10 PDL par montant TURPE total
                """),
                top_pdl_turpe.to_frame('TURPE Total (€)')
            ])
        else:
            _analyse_financiere = mo.md("⏭️ Analyse financière non disponible (colonnes TURPE manquantes)")
        return _analyse_financiere


    _()
    return


@app.cell(hide_code=True)
def energy_analysis(meta_periodes_primary, mo):
    # Analyse énergétique
    if meta_periodes_primary is not None:
        # Colonnes d'énergie disponibles
        _colonnes_energie = [_col for _col in ['BASE_energie', 'HP_energie', 'HC_energie'] if _col in meta_periodes_primary.columns]

        if _colonnes_energie:
            # Calcul des totaux d'énergie
            totaux_energie = {}
            for _col in _colonnes_energie:
                total = meta_periodes_primary[_col].sum() if meta_periodes_primary[_col].notna().any() else 0
                totaux_energie[_col.replace('_energie', '')] = total

            # Consommation totale
            consommation_totale = sum(totaux_energie.values())

            # Top 10 PDL par consommation
            meta_avec_total_energie = meta_periodes_primary.copy()
            meta_avec_total_energie['energie_totale'] = sum(
                meta_periodes_primary[_col].fillna(0) for _col in _colonnes_energie
            )

            top_pdl_energie = (
                meta_avec_total_energie
                .groupby('pdl')['energie_totale']
                .sum()
                .sort_values(ascending=False)
                .head(10)
            )

            # Format des totaux pour affichage
            totaux_formatted = []
            for type_energie, total in totaux_energie.items():
                if total > 0:
                    totaux_formatted.append(f"- **{type_energie}**: {total:,.0f} kWh")

            _analyse_energetique = mo.vstack([
                mo.md(f"""
                ## ⚡ **Analyse Énergétique**

                ### Consommations totales
                {chr(10).join(totaux_formatted)}
                - **Total**: {consommation_totale:,.0f} kWh

                ### Top 10 PDL par consommation totale
                """),
                top_pdl_energie.to_frame('Consommation (kWh)')
            ])
        else:
            _analyse_energetique = mo.md("⏭️ Analyse énergétique non disponible (colonnes énergie manquantes)")
    else:
        _analyse_energetique = mo.md("⏭️ Analyse énergétique non disponible")

    _analyse_energetique
    return


@app.cell(hide_code=True)
def turpe_visualizations(meta_periodes_primary, mo, px):
    # Visualisations interactives
    if meta_periodes_primary is not None and 'turpe_fixe' in meta_periodes_primary.columns and 'turpe_variable' in meta_periodes_primary.columns:
        # Graphique temporel des montants TURPE par mois
        turpe_mensuel = (
            meta_periodes_primary
            .assign(turpe_total=meta_periodes_primary['turpe_fixe'] + meta_periodes_primary['turpe_variable'])
            .groupby('mois_annee')
            .agg({
                'turpe_fixe': 'sum',
                'turpe_variable': 'sum', 
                'turpe_total': 'sum'
            })
            .reset_index()
        )

        fig_turpe_temps = px.line(
            turpe_mensuel, 
            x='mois_annee', 
            y=['turpe_fixe', 'turpe_variable', 'turpe_total'],
            title="Évolution mensuelle des montants TURPE",
            labels={'value': 'Montant (€)', 'mois_annee': 'Mois'},
            height=400
        )
        fig_turpe_temps.update_layout(legend_title_text='Type TURPE')

        _viz_turpe = fig_turpe_temps
    else:
        _viz_turpe = mo.md("⏭️ Visualisation TURPE non disponible")

    _viz_turpe
    return


@app.cell(hide_code=True)
def power_distribution_visualization(meta_periodes_primary, mo, px):
    # Distribution des puissances moyennes par FTA
    if meta_periodes_primary is not None:
        fig_puissance_fta = px.box(
            meta_periodes_primary,
            x='Formule_Tarifaire_Acheminement',
            y='puissance_moyenne',
            title="Distribution des puissances moyennes par Formule Tarifaire",
            labels={'puissance_moyenne': 'Puissance Moyenne (kVA)', 'Formule_Tarifaire_Acheminement': 'FTA'},
            height=400
        )
        fig_puissance_fta.update_layout(xaxis_tickangle=-45)

        _viz_puissance = fig_puissance_fta
    else:
        _viz_puissance = mo.md("⏭️ Visualisation puissances non disponible")

    _viz_puissance
    return


@app.cell(hide_code=True)
def energy_turpe_scatter(meta_periodes_primary, mo, px):
    # Scatter plot énergie vs TURPE
    if (meta_periodes_primary is not None and 'turpe_fixe' in meta_periodes_primary.columns 
        and any(_col in meta_periodes_primary.columns for _col in ['BASE_energie', 'HP_energie', 'HC_energie'])):

        # Calculer énergie totale et TURPE total
        _colonnes_energie = [_col for _col in ['BASE_energie', 'HP_energie', 'HC_energie'] if _col in meta_periodes_primary.columns]
        meta_viz = meta_periodes_primary.copy()
        meta_viz['energie_totale'] = sum(meta_periodes_primary[_col].fillna(0) for _col in _colonnes_energie)
        meta_viz['turpe_total'] = meta_periodes_primary['turpe_fixe'] + meta_periodes_primary['turpe_variable']

        # Filtrer les valeurs nulles et aberrantes
        meta_viz_clean = meta_viz[
            (meta_viz['energie_totale'] > 0) & 
            (meta_viz['turpe_total'] > 0) &
            (meta_viz['energie_totale'] < meta_viz['energie_totale'].quantile(0.99))
        ]

        fig_scatter = px.scatter(
            meta_viz_clean,
            x='energie_totale',
            y='turpe_total',
            color='Formule_Tarifaire_Acheminement',
            title="Relation Énergie vs TURPE Total",
            labels={'energie_totale': 'Énergie Totale (kWh)', 'turpe_total': 'TURPE Total (€)'},
            height=400,
            hover_data=['pdl', 'mois_annee']
        )

        _viz_scatter = fig_scatter
    else:
        _viz_scatter = mo.md("⏭️ Visualisation énergie/TURPE non disponible")

    _viz_scatter
    return


@app.cell(hide_code=True)
def synthesis_tables(meta_periodes_primary, mo):
    # Tableaux de synthèse
    if meta_periodes_primary is not None:
        # Synthèse par mois
        synthese_mensuelle = (
            meta_periodes_primary
            .groupby('mois_annee')
            .agg({
                'pdl': 'nunique',
                'puissance_moyenne': 'mean',
                'turpe_fixe': 'sum',
                'turpe_variable': 'sum',
                'has_changement': 'sum', 
                'HP_energie': 'sum',
                'HC_energie': 'sum',
                'BASE_energie': 'sum',
            })
            .round(2)
            .rename(columns={
                'pdl': 'Nb PDL',
                'puissance_moyenne': 'Puissance Moy (kVA)',
                'turpe_fixe': 'TURPE Fixe (€)',
                'turpe_variable': 'TURPE Var (€)',
                'has_changement': 'Nb Changements'
            })
        )

        # Synthèse par FTA
        synthese_fta = (
            meta_periodes_primary
            .groupby('Formule_Tarifaire_Acheminement')
            .agg({
                'pdl': 'nunique',
                'puissance_moyenne': 'mean',
                'turpe_fixe': 'sum',
                'turpe_variable': 'sum',
                'nb_jours': 'sum',
                'HP_energie': 'sum',
                'HC_energie': 'sum',
                'BASE_energie': 'sum',
            })
            .round(2)
            .rename(columns={
                'pdl': 'Nb PDL',
                'puissance_moyenne': 'Puissance Moy (kVA)',
                'turpe_fixe': 'TURPE Fixe (€)',
                'turpe_variable': 'TURPE Var (€)',
                'nb_jours': 'Total Jours'
            })
        )

        _syntheses = mo.vstack([
            mo.md("## 📋 **Tableaux de Synthèse**"),
            mo.md("### Synthèse par Mois"),
            synthese_mensuelle,
            mo.md("### Synthèse par Formule Tarifaire"),
            synthese_fta
        ])
    else:
        _syntheses = mo.md("⏭️ Synthèses non disponibles")

    _syntheses
    return


@app.cell(hide_code=True)
def debug_zone_header(mo):
    mo.md(
        """
    ## 🔧 **Zone de Debug Interactive**

    Utilisez les cellules ci-dessous pour explorer les données plus en détail.
    """
    )
    return


@app.cell(hide_code=True)
def pdl_input_widget(mo):
    # Interface pour sélectionner un PDL spécifique à inspecter
    pdl_input = mo.ui.text(
        placeholder="Entrez un PDL à inspecter", 
        label="PDL à analyser en détail"
    )
    pdl_input
    return (pdl_input,)


@app.cell(hide_code=True)
def analyze_specific_pdl(meta_periodes_primary, mo, pdl_input):
    # Analyse détaillée d'un PDL spécifique
    if pdl_input.value and meta_periodes_primary is not None:
        pdl_data = meta_periodes_primary[meta_periodes_primary['pdl'] == pdl_input.value]
        if len(pdl_data) > 0:
            # Calcul de quelques métriques pour ce PDL
            nb_mois = len(pdl_data)
            changements = pdl_data['has_changement'].sum()
            puissance_moyenne = pdl_data['puissance_moyenne'].mean()
            turpe_total = (pdl_data['turpe_fixe'] + pdl_data['turpe_variable']).sum()

            _pdl_analysis = mo.vstack([
                mo.md(f"""
                ### Analyse détaillée du PDL: {pdl_input.value}

                - **Nombre de mois**: {nb_mois}
                - **Mois avec changements**: {changements}
                - **Puissance moyenne**: {puissance_moyenne:.1f} kVA
                - **TURPE total**: {turpe_total:.2f} €
                """),
                pdl_data
            ])
        else:
            _pdl_analysis = mo.md(f"❌ Aucune donnée trouvée pour le PDL: {pdl_input.value}")
    else:
        _pdl_analysis = mo.md("💡 Entrez un PDL ci-dessus pour voir l'analyse détaillée")

    _pdl_analysis
    return


@app.cell(hide_code=True)
def validation_and_anomalies(meta_periodes_primary, mo):
    # Validation et détection d'anomalies
    if meta_periodes_primary is not None:
        # Vérifications de cohérence
        anomalies = []

        # Valeurs négatives
        if 'turpe_fixe' in meta_periodes_primary.columns:
            negatifs_fixe = (meta_periodes_primary['turpe_fixe'] < 0).sum()
            if negatifs_fixe > 0:
                anomalies.append(f"❌ {negatifs_fixe} valeurs TURPE fixe négatives")

        if 'turpe_variable' in meta_periodes_primary.columns:
            negatifs_var = (meta_periodes_primary['turpe_variable'] < 0).sum()
            if negatifs_var > 0:
                anomalies.append(f"❌ {negatifs_var} valeurs TURPE variable négatives")

        # Puissances aberrantes
        puissances_extremes = (meta_periodes_primary['puissance_moyenne'] > 1000).sum()
        if puissances_extremes > 0:
            anomalies.append(f"⚠️ {puissances_extremes} puissances > 1000 kVA")

        # Périodes sans jours
        jours_zero = (meta_periodes_primary['nb_jours'] <= 0).sum()
        if jours_zero > 0:
            anomalies.append(f"❌ {jours_zero} périodes avec nb_jours <= 0")

        # Colonnes avec trop de NaN
        for _col in ['turpe_fixe', 'turpe_variable']:
            if _col in meta_periodes_primary.columns:
                nan_count = meta_periodes_primary[_col].isna().sum()
                nan_pct = nan_count / len(meta_periodes_primary) * 100
                if nan_pct > 10:
                    anomalies.append(f"⚠️ {_col}: {nan_pct:.1f}% de valeurs manquantes")

        if anomalies:
            _validation = mo.md(f"""
            ## ⚠️ **Validation et Anomalies Détectées**

            {chr(10).join(anomalies)}
            """)
        else:
            _validation = mo.md("""
            ## ✅ **Validation - Aucune Anomalie Détectée**

            Les données semblent cohérentes.
            """)
    else:
        _validation = mo.md("⏭️ Validation non disponible")

    _validation
    return


@app.cell(hide_code=True)
def columns_info_display(meta_periodes_primary, mo):
    # Colonnes disponibles pour inspection
    if meta_periodes_primary is not None:
        colonnes = list(meta_periodes_primary.columns)
        _columns_info = mo.md(f"""
        ### Structure des Méta-périodes ({len(colonnes)} colonnes)
        `{', '.join(colonnes)}`
        """)
    else:
        _columns_info = mo.md("❌ Pas de colonnes à afficher")

    _columns_info
    return


@app.cell(hide_code=True)
def development_notes(mo):
    mo.md(
        """
    ## 📝 **Notes de Développement**

    ### Pipeline testé
    **`facturation()`** - Pipeline complet orchestré avec méta-périodes mensuelles :
    1. Enrichissement historique (pipeline_perimetre) - une seule fois
    2. Génère périodes d'abonnement détaillées (pipeline_abonnement pur)
    3. Génère périodes d'énergie détaillées (pipeline_energie pur)  
    4. Agrégation mensuelle avec puissance moyenne pondérée (pipeline_facturation pur)

    ✅ **Architecture refactorisée** : pipelines purs + orchestration avec ResultatFacturation

    ### Avantages de l'approche méta-périodes
    - **Simplification**: Une ligne par mois et par référence contractuelle
    - **Équivalence mathématique**: Puissance moyenne pondérée = calcul exact
    - **Performance**: Réduction significative du volume de données
    - **Traçabilité**: Flag `has_changement` pour identifier les cas complexes

    ### Corrections apportées
    - **Problème résolu**: L'agrégation des énergies ne gérait pas les colonnes de dates
    - **Solution**: Ajout de `debut: 'min'` et `fin: 'max'` dans `agreger_energies_mensuel()`
    - **Résultat**: Plus de NaT dans les jointures, pipeline stable

    ### Validation
    - Les calculs respectent la linéarité de la tarification TURPE
    - L'agrégation préserve l'exactitude des montants
    - Les métadonnées permettent la traçabilité des changements intra-mois
    - Validation Pandera réussie sur toutes les colonnes

    **Prochaines étapes**: Export des résultats, intégration avec les systèmes de facturation
    """
    )
    return


if __name__ == "__main__":
    app.run()
