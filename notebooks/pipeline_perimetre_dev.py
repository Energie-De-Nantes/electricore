import marimo

__generated_with = "0.14.17"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def introduction(mo):
    mo.md(
        r"""
    # 🔧 Pipeline Périmètre - Tests et Validation

    Ce notebook permet de tester et valider la fonction **`pipeline_perimetre`** qui constitue l'étape 
    fondamentale d'enrichissement de l'historique du périmètre.

    ## 🎯 Objectifs du pipeline_perimetre

    1. **Détection des points de rupture** : Identifier les changements de périodes contractuelles
    2. **Insertion d'événements FACTURATION** : Ajouter des événements synthétiques (1er du mois)
    3. **Enrichissement structurel** : Préparer l'historique pour les calculs aval

    ## 📋 Workflow de validation

    1. **Chargement** : Import des données C15 avec ElectriFlux
    2. **Transformation** : Conversion vers HistoriquePérimètre 
    3. **Enrichissement** : Application du pipeline_perimetre
    4. **Comparaison** : Analyse avant/après enrichissement
    5. **Validation** : Vérification de la cohérence des résultats
    6. **Métriques** : Calcul des indicateurs de qualité

    Suivez les étapes ci-dessous pour analyser l'enrichissement de votre historique.
    """
    )
    return


@app.cell
def imports():
    import marimo as mo

    # Standard library
    from pathlib import Path
    import pandas as pd
    import numpy as np

    # ElectriFlux - Extract
    from electriflux.simple_reader import process_flux

    # ElectriCore - Transform
    from electricore.inputs.flux import lire_flux_c15

    # ElectriCore - Process (notre fonction à tester)
    from electricore.core.pipeline_perimetre import pipeline_perimetre

    # Debugging & utilities
    from icecream import ic

    return Path, lire_flux_c15, mo, pipeline_perimetre, process_flux


@app.cell(hide_code=True)
def configuration_paths(Path, mo):
    # Configuration des chemins de données
    data_path = Path('~/data/flux_enedis').expanduser()
    c15_path = data_path / 'C15'

    _status_message = mo.md(f"""
    ## 📁 Configuration des chemins

    - **Répertoire principal**: `{data_path}`
    - **Flux C15**: `{c15_path}` {'✅' if c15_path.exists() else '❌ (non trouvé)'}

    Le pipeline_perimetre ne nécessite que les données C15 (historique du périmètre).
    """)

    _status_message
    return (c15_path,)


@app.cell(hide_code=True)
def extract_data(c15_path, mo, process_flux):
    # Étape 1: Extract - Chargement des données brutes C15
    raw_c15, extract_success = None, False

    try:
        raw_c15 = process_flux('C15', c15_path)
        _extract_status = mo.md(f"""
        ## 📥 **Extract - Données C15 chargées**

        - **C15 (Contrats)**: {len(raw_c15)} lignes, {len(raw_c15.columns)} colonnes ✅

        Données brutes prêtes pour transformation.
        """)
        extract_success = True
    except Exception as e:
        _extract_status = mo.md(f"❌ **Erreur lors du chargement C15**: {str(e)}")

    _extract_status
    return extract_success, raw_c15


@app.cell(hide_code=True)
def transform_data(extract_success, lire_flux_c15, mo, raw_c15):
    # Étape 2: Transform - Conversion vers HistoriquePérimètre
    historique_original, transform_success = None, False

    if extract_success and raw_c15 is not None:
        try:
            # Transformation C15 → HistoriquePérimètre
            historique_original = lire_flux_c15(raw_c15)

            _transform_status = mo.md(f"""
            ## 🔄 **Transform - HistoriquePérimètre créé**

            - **HistoriquePérimètre**: {len(historique_original)} événements validés ✅
            - **PDL uniques**: {historique_original['pdl'].nunique()}
            - **Période couverte**: {historique_original['Date_Evenement'].min().strftime('%Y-%m-%d')} → {historique_original['Date_Evenement'].max().strftime('%Y-%m-%d')}

            Les données respectent le schéma Pandera HistoriquePérimètre.
            """)
            transform_success = True
        except Exception as e:
            _transform_status = mo.md(f"❌ **Erreur de transformation**: {str(e)}")
    else:
        _transform_status = mo.md("⏭️ Étape Transform ignorée (données brutes manquantes)")

    _transform_status
    return historique_original, transform_success


@app.cell(hide_code=True)
def inspect_original_historique(historique_original, mo):
    # Inspection de l'historique original
    if historique_original is not None:
        # Compter les événements par type
        evenements_par_type = historique_original['Evenement_Declencheur'].value_counts()

        _original_display = mo.vstack([
            mo.md("### 📊 Historique Original - Analyse"),
            mo.md(f"""
            **Types d'événements présents:**

            {evenements_par_type.to_string()}

            **Événements FACTURATION existants:** {evenements_par_type.get('FACTURATION', 0)}
            """),
            mo.md("### 📋 Échantillon des données"),
            historique_original
        ])
    else:
        _original_display = mo.md("❌ Historique original non disponible")

    _original_display
    return


@app.cell(hide_code=True)
def execute_pipeline_perimetre(
    historique_original,
    mo,
    pipeline_perimetre,
    transform_success,
):
    # Étape 3: Process - Application du pipeline_perimetre
    historique_enrichi, pipeline_success = None, False

    if transform_success and historique_original is not None:
        try:
            # Application du pipeline_perimetre
            historique_enrichi = pipeline_perimetre(historique_original)

            _pipeline_status = mo.md(f"""
            ## ⚙️ **Process - Pipeline Commun appliqué**

            - **Historique enrichi**: {len(historique_enrichi)} événements ✅
            - **Nouveaux événements**: +{len(historique_enrichi) - len(historique_original)}
            - **Colonnes**: {len(historique_enrichi.columns)}

            Pipeline commun exécuté avec succès !
            """)
            pipeline_success = True
        except Exception as e:
            _pipeline_status = mo.md(f"❌ **Erreur pipeline_perimetre**: {str(e)}")
    else:
        _pipeline_status = mo.md("⏭️ Pipeline ignoré (historique original manquant)")

    _pipeline_status
    return historique_enrichi, pipeline_success


@app.cell
def _(historique_enrichi):
    historique_enrichi
    return


@app.cell
def compare_before_after(
    historique_enrichi,
    historique_original,
    mo,
    pipeline_success,
):
    def _():
        # Comparaison avant/après enrichissement
        if pipeline_success and historique_original is not None and historique_enrichi is not None:
            # Événements par type - original
            original_events = historique_original['Evenement_Declencheur'].value_counts()
            enriched_events = historique_enrichi['Evenement_Declencheur'].value_counts()

            # Nouveaux événements FACTURATION
            nouveaux_facturation = enriched_events.get('FACTURATION', 0) - original_events.get('FACTURATION', 0)

            # Analyse des nouvelles colonnes
            nouvelles_colonnes = set(historique_enrichi.columns) - set(historique_original.columns)

            _comparison = mo.vstack([
                mo.md("## 🔍 **Comparaison Avant/Après Enrichissement**"),
                mo.md(f"""
                ### 📈 Impact de l'enrichissement

                - **Événements ajoutés**: +{len(historique_enrichi) - len(historique_original)}
                - **Nouveaux événements FACTURATION**: +{nouveaux_facturation}
                - **Nouvelles colonnes**: {len(nouvelles_colonnes)}

                ### 📊 Répartition des événements

                **Avant enrichissement:**
                {original_events.to_string()}

                **Après enrichissement:**
                {enriched_events.to_string()}
                """),
                mo.md(f"""
                ### 🆕 Nouvelles colonnes ajoutées

                {', '.join(sorted(nouvelles_colonnes)) if nouvelles_colonnes else 'Aucune nouvelle colonne'}
                """)
            ])
        else:
            _comparison = mo.md("⏭️ Comparaison non disponible")
        return _comparison


    _()
    return


@app.cell(hide_code=True)
def inspect_enriched_historique(historique_enrichi, mo, pipeline_success):
    # Inspection détaillée de l'historique enrichi
    if pipeline_success and historique_enrichi is not None:
        # Focus sur les nouveaux événements FACTURATION
        nouveaux_facturation = historique_enrichi[
            historique_enrichi['Evenement_Declencheur'] == 'FACTURATION'
        ].copy()

        _enriched_display = mo.vstack([
            mo.md("### 🔋 Historique Enrichi - Nouveaux événements FACTURATION"),
            mo.md(f"""
            **Nombre d'événements FACTURATION générés:** {len(nouveaux_facturation)}
            """),
            nouveaux_facturation.head(10) if len(nouveaux_facturation) > 0 else mo.md("❌ Aucun événement FACTURATION généré"),
            mo.md("### 📋 Échantillon complet de l'historique enrichi"),
            historique_enrichi.head(10)
        ])
    else:
        _enriched_display = mo.md("❌ Historique enrichi non disponible")

    _enriched_display
    return


@app.cell
def quality_metrics(
    historique_enrichi,
    historique_original,
    mo,
    pipeline_success,
):
    def _():
        # Métriques de qualité et validation
        if pipeline_success and historique_original is not None and historique_enrichi is not None:
            # Statistiques de base
            pdls_originaux = historique_original['pdl'].nunique()
            pdls_enrichis = historique_enrichi['pdl'].nunique()

            # Vérification de la préservation des PDL
            pdls_preserves = pdls_originaux == pdls_enrichis

            # Chronologie
            dates_originales = (historique_original['Date_Evenement'].min(), historique_original['Date_Evenement'].max())
            dates_enrichies = (historique_enrichi['Date_Evenement'].min(), historique_enrichi['Date_Evenement'].max())

            # Événements FACTURATION par mois
            facturation_events = historique_enrichi[
                historique_enrichi['Evenement_Declencheur'] == 'FACTURATION'
            ]

            if len(facturation_events) > 0:
                facturation_par_mois = facturation_events.groupby(
                    facturation_events['Date_Evenement'].dt.to_period('M')
                ).size()
                facturation_stats = f"Moyenne: {facturation_par_mois.mean():.1f}, Min: {facturation_par_mois.min()}, Max: {facturation_par_mois.max()}"
            else:
                facturation_stats = "Aucun événement FACTURATION"

            _quality_metrics = mo.md(f"""
            ## 📊 **Métriques de Qualité**

            ### 🎯 Préservation des données
            - **PDL préservés**: {pdls_preserves} ({'✅' if pdls_preserves else '❌'})
            - **PDL originaux**: {pdls_originaux}
            - **PDL enrichis**: {pdls_enrichis}

            ### 📅 Couverture temporelle
            - **Période originale**: {dates_originales[0].strftime('%Y-%m-%d')} → {dates_originales[1].strftime('%Y-%m-%d')}
            - **Période enrichie**: {dates_enrichies[0].strftime('%Y-%m-%d')} → {dates_enrichies[1].strftime('%Y-%m-%d')}

            ### ⚡ Événements FACTURATION générés
            - **Total**: {len(facturation_events)}
            - **Par mois**: {facturation_stats}
            - **Répartition par PDL**: {facturation_events['pdl'].nunique()} PDL couverts
            """)
        else:
            _quality_metrics = mo.md("⏭️ Métriques non disponibles")
        return _quality_metrics


    _()
    return


@app.cell
def validation_checks(historique_enrichi, mo, pipeline_success):
    # Tests de validation automatisés
    if pipeline_success and historique_enrichi is not None:
        validation_results = []

        # Test 1: Présence d'événements FACTURATION
        facturation_count = (historique_enrichi['Evenement_Declencheur'] == 'FACTURATION').sum()
        validation_results.append(f"✅ Événements FACTURATION générés: {facturation_count}" if facturation_count > 0 else "❌ Aucun événement FACTURATION généré")

        # Test 2: Dates d'événements FACTURATION (1er du mois)
        facturation_dates = historique_enrichi[
            historique_enrichi['Evenement_Declencheur'] == 'FACTURATION'
        ]['Date_Evenement']

        if len(facturation_dates) > 0:
            premiers_du_mois = facturation_dates.dt.day == 1
            pct_premiers_mois = (premiers_du_mois.sum() / len(facturation_dates)) * 100
            validation_results.append(f"✅ Événements FACTURATION au 1er du mois: {pct_premiers_mois:.1f}%" if pct_premiers_mois > 95 else f"⚠️ Événements FACTURATION au 1er du mois: {pct_premiers_mois:.1f}% (attendu >95%)")

        # Test 3: Ordre chronologique préservé
        dates_ordonnees = historique_enrichi['Date_Evenement'].is_monotonic_increasing
        validation_results.append("✅ Ordre chronologique préservé" if dates_ordonnees else "⚠️ Ordre chronologique perturbé")

        # Test 4: Nouveaux champs ajoutés
        nouvelles_colonnes = set(historique_enrichi.columns) - {'PDL', 'Date_Evenement', 'Evenement_Declencheur', 'Ref_Situation_Contractuelle', 'Statut_Technique', 'Puissance_Souscrite', 'Type_PDL', 'Categorie_Clientele', 'Statut_PDL', 'Secteur_Activite_PDL', 'Code_Postal'}
        validation_results.append(f"✅ Nouvelles colonnes ajoutées: {len(nouvelles_colonnes)}" if len(nouvelles_colonnes) > 0 else "⚠️ Aucune nouvelle colonne ajoutée")

        _validation = mo.vstack([
            mo.md("## ✅ **Tests de Validation Automatisés**"),
            mo.md("\n".join([f"- {result}" for result in validation_results]))
        ])
    else:
        _validation = mo.md("⏭️ Validation non disponible")

    _validation
    return


@app.cell(hide_code=True)
def debug_interface(mo):
    # Interface interactive pour le debug
    mo.md("""
    ## 🔧 **Zone de Debug Interactive**

    Utilisez les widgets ci-dessous pour explorer les données enrichies en détail.
    """)
    return


@app.cell(hide_code=True)
def pdl_selector_widget(mo):
    # Interface pour sélectionner un PDL spécifique à inspecter
    pdl_input = mo.ui.text(placeholder="Entrez un PDL à inspecter", label="PDL à analyser")
    pdl_input
    return (pdl_input,)


@app.cell(hide_code=True)
def pdl_detailed_analysis(historique_enrichi, mo, pdl_input, pipeline_success):
    # Analyse détaillée d'un PDL spécifique
    if pipeline_success and historique_enrichi is not None and pdl_input.value:
        pdl_data = historique_enrichi[historique_enrichi['pdl'] == pdl_input.value].copy()

        if len(pdl_data) > 0:
            # Trier par date
            pdl_data = pdl_data.sort_values('Date_Evenement')

            # Statistiques pour ce PDL
            total_events = len(pdl_data)
            facturation_events = (pdl_data['Evenement_Declencheur'] == 'FACTURATION').sum()
            periode_couverte = (
                pdl_data['Date_Evenement'].min().strftime('%Y-%m-%d'),
                pdl_data['Date_Evenement'].max().strftime('%Y-%m-%d')
            )

            _pdl_analysis = mo.vstack([
                mo.md(f"### 🔍 Analyse détaillée du PDL: **{pdl_input.value}**"),
                mo.md(f"""
                **Statistiques:**
                - Total événements: {total_events}
                - Événements FACTURATION: {facturation_events}
                - Période: {periode_couverte[0]} → {periode_couverte[1]}
                """),
                mo.md("**Chronologie complète:**"),
                pdl_data[['Date_Evenement', 'Evenement_Declencheur', 'Ref_Situation_Contractuelle']].reset_index(drop=True)
            ])
        else:
            _pdl_analysis = mo.md(f"❌ Aucune donnée trouvée pour le PDL: {pdl_input.value}")
    else:
        _pdl_analysis = mo.md("💡 Sélectionnez un PDL ci-dessus pour voir l'analyse détaillée")

    _pdl_analysis
    return


@app.cell(hide_code=True)
def event_filter_widget(historique_enrichi, mo, pipeline_success):
    # Widget pour filtrer par type d'événement
    if pipeline_success and historique_enrichi is not None:
        types_evenements = sorted(historique_enrichi['Evenement_Declencheur'].unique())
        event_filter = mo.ui.multiselect(
            options=types_evenements,
            value=['FACTURATION'] if 'FACTURATION' in types_evenements else types_evenements[:2],
            label="Types d'événements à afficher"
        )
    else:
        event_filter = mo.ui.multiselect(options=[], label="Types d'événements (données non disponibles)")

    event_filter
    return (event_filter,)


@app.cell(hide_code=True)
def filtered_events_analysis(
    event_filter,
    historique_enrichi,
    mo,
    pipeline_success,
):
    # Analyse des événements filtrés
    if pipeline_success and historique_enrichi is not None and event_filter.value:
        filtered_data = historique_enrichi[
            historique_enrichi['Evenement_Declencheur'].isin(event_filter.value)
        ].copy()

        if len(filtered_data) > 0:
            # Statistiques par type
            stats_par_type = filtered_data['Evenement_Declencheur'].value_counts()

            _filtered_analysis = mo.vstack([
                mo.md(f"### 📈 Analyse des événements: **{', '.join(event_filter.value)}**"),
                mo.md(f"""
                **Répartition:**

                {stats_par_type.to_string()}

                **Total événements sélectionnés:** {len(filtered_data)}
                """),
                mo.md("**Échantillon des données:**"),
                filtered_data.head(15).reset_index(drop=True)
            ])
        else:
            _filtered_analysis = mo.md("❌ Aucun événement trouvé avec les filtres sélectionnés")
    else:
        _filtered_analysis = mo.md("💡 Sélectionnez des types d'événements pour voir l'analyse")

    _filtered_analysis
    return


@app.cell(hide_code=True)
def development_notes(mo):
    mo.md(
        """
    ## 📝 **Notes de Développement**

    ### 🎯 Ce que teste ce notebook

    - **Pipeline commun** : Fonction centrale d'enrichissement de l'historique
    - **Points de rupture** : Détection automatique des changements de périodes
    - **Événements FACTURATION** : Insertion d'événements synthétiques mensuels
    - **Préservation des données** : Validation de l'intégrité des données originales

    ### 🔍 Points de vigilance

    - Les événements FACTURATION doivent être générés au 1er de chaque mois
    - L'ordre chronologique doit être préservé
    - Aucun PDL ne doit être perdu dans le processus
    - Les références contractuelles doivent rester cohérentes

    ### 🚀 Utilisation pour le debugging

    1. Vérifiez d'abord les métriques de qualité globales
    2. Utilisez l'analyse par PDL pour les cas spécifiques
    3. Filtrez par type d'événement pour comprendre l'impact
    4. Validez la chronologie des événements FACTURATION

    ### 📚 Context technique

    - **Source** : Flux C15 Enedis (événements contractuels)
    - **Transformation** : HistoriquePérimètre (schéma Pandera)
    - **Enrichissement** : pipeline_perimetre() avec détection de points de rupture
    - **Usage aval** : Base pour pipeline_abonnement et pipeline_energie
    """
    )
    return


if __name__ == "__main__":
    app.run()
